// Package podnetwork gives native pods their own IPs in the cluster's pod
// network. darwin-net does the per-pod work (a /32 lo0 alias per pod from
// this node's podCIDR, a userspace Service proxy that owns each ClusterIP on
// lo0, and the getaddrinfo shim's cluster DNS env), and package flannel joins
// the node to the cluster's flannel VXLAN network so those IPs are routed.
package podnetwork

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/netip"
	"slices"
	"sort"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	netv1 "k3sm.io/apis/net/v1"
	"k3sm.io/darwin-net/pkg/dns"
	"k3sm.io/darwin-net/pkg/podnet"
	"k3sm.io/darwin-net/pkg/proxy"
	"k3sm.io/runtimed/pkg/sandbox"

	"github.com/changtimwu/darwin-runner-vkube/internal/flannel"
)

// Config configures the node's pod network.
type Config struct {
	Client   kubernetes.Interface
	NodeName string
	NodeIP   netip.Addr
	// ClusterCIDR and ServiceCIDR are the cluster's pod and Service ranges
	// (k3s: 10.42.0.0/16 and 10.43.0.0/16).
	ClusterCIDR, ServiceCIDR netip.Prefix
	// ClusterDomain is the DNS domain Services live under.
	ClusterDomain string
	// APIServer, when valid, pins the kubernetes Service to this address: the
	// node reaches the apiserver through a loopback-published port, so the
	// endpoint the apiserver advertises is not reachable from here.
	APIServer netip.AddrPort
	// VMSubnet is the vmnet segment micro-VM guests lease addresses from
	// (macOS default 192.168.64.0/24). The relay to a guest refuses any other
	// address.
	VMSubnet netip.Prefix
	// VXLANEndpoints overrides where frames for a peer's public IP go (test
	// topologies only; see flannel.Config).
	VXLANEndpoints map[netip.Addr]netip.AddrPort
	Logger         *slog.Logger
}

// Network is the node's pod network. Until Start has the node's podCIDR, pod
// setup waits; a node that never gets one fails pod creation, not startup.
type Network struct {
	cfg Config
	log *slog.Logger

	ready chan struct{}
	err   error // set before ready closes

	podCIDR netip.Prefix
	ipam    *podnet.Network
	dp      *flannel.Datapath
	table   *proxy.RoutingTable
	dnsVIP  string

	mu      sync.Mutex // guards hostNet and guests
	hostNet map[string]bool
	guests  map[string]*guest
}

// guest is a vm pod's two addresses: the published pod IP on lo0, and the
// guest's own vmnet lease the proxy relays that IP's ports to.
type guest struct {
	cfg   sandbox.GuestNetworkConfig
	live  netip.Addr
	ports []uint16
}

// New returns a Network that is not yet started.
func New(cfg Config) *Network {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.ClusterDomain == "" {
		cfg.ClusterDomain = dns.DefaultClusterDomain
	}
	return &Network{cfg: cfg, log: cfg.Logger.With("component", "podnet"), ready: make(chan struct{}), hostNet: map[string]bool{}, guests: map[string]*guest{}}
}

// Start brings the network up in the background. It waits up to wait for the
// node's podCIDR first, so a re-started node is ready before its pods are
// re-attached; a node joining for the first time gets its podCIDR only after
// it registers, and finishes in the background.
func (n *Network) Start(ctx context.Context, wait time.Duration) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		n.err = n.start(ctx)
		if n.err != nil {
			n.log.Error("pod network failed; pods will not start", "err", n.err)
		}
		close(n.ready)
	}()
	select {
	case <-done:
	case <-time.After(wait):
		n.log.Info("waiting for the node's podCIDR; pod network starts once the cluster assigns it")
	}
}

func (n *Network) start(ctx context.Context) error {
	cidr, err := n.waitPodCIDR(ctx)
	if err != nil {
		return err
	}
	n.podCIDR = cidr
	if !n.cfg.ClusterCIDR.Contains(cidr.Addr()) {
		return fmt.Errorf("node podCIDR %s is outside the cluster CIDR %s (set --cluster-cidr)", cidr, n.cfg.ClusterCIDR)
	}

	// The VNI is the cluster's: take it from a peer, as flannel does.
	vni := uint32(1)
	if nodes, err := n.cfg.Client.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		for i := range nodes.Items {
			if p, ok := flannel.PeerFromNode(&nodes.Items[i]); ok && nodes.Items[i].Name != n.cfg.NodeName {
				vni = p.VNI
				break
			}
		}
	}
	mac := flannel.VtepMAC(n.cfg.NodeName)
	n.dp, err = flannel.Open(flannel.Config{PodCIDR: cidr, VNI: vni, VtepMAC: mac, Endpoints: n.cfg.VXLANEndpoints, Logger: n.log})
	if err != nil {
		return err
	}
	go n.dp.Run(ctx)
	if err := n.annotate(ctx, flannel.Annotations(n.cfg.NodeIP, mac, vni)); err != nil {
		return fmt.Errorf("publish flannel annotations: %w", err)
	}
	n.watchPeers(ctx)

	if n.ipam, err = podnet.New(cidr, podnet.WithLogger(n.log)); err != nil {
		return err
	}
	n.dnsVIP = n.lookupDNSVIP(ctx)
	if err := n.startProxy(ctx); err != nil {
		return err
	}
	n.log.Info("pod network up", "podCIDR", cidr, "utun", n.dp.Interface(), "vtep", mac.String(), "vni", vni, "dns", n.dnsVIP)
	return nil
}

func (n *Network) waitPodCIDR(ctx context.Context) (netip.Prefix, error) {
	for {
		node, err := n.cfg.Client.CoreV1().Nodes().Get(ctx, n.cfg.NodeName, metav1.GetOptions{})
		if err == nil && node.Spec.PodCIDR != "" {
			p, err := netip.ParsePrefix(node.Spec.PodCIDR)
			if err != nil || !p.Addr().Is4() || p.Bits() != 24 {
				return netip.Prefix{}, fmt.Errorf("node podCIDR %q: want an IPv4 /24 (darwin pods are lo0 aliases from a /24)", node.Spec.PodCIDR)
			}
			return p.Masked(), nil
		}
		select {
		case <-ctx.Done():
			return netip.Prefix{}, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// annotate merges the flannel annotations into the node. The virtual-kubelet
// node controller updates status only, so they stay.
func (n *Network) annotate(ctx context.Context, a map[string]string) error {
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": a}})
	_, err := n.cfg.Client.CoreV1().Nodes().Patch(ctx, n.cfg.NodeName, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// watchPeers keeps the datapath's peer set equal to the other flannel VXLAN
// nodes, and re-publishes this node's annotations if something drops them.
func (n *Network) watchPeers(ctx context.Context) {
	f := informers.NewSharedInformerFactory(n.cfg.Client, 10*time.Minute)
	inf := f.Core().V1().Nodes().Informer()
	sync := func() {
		var peers []flannel.Peer
		for _, o := range inf.GetStore().List() {
			node := o.(*corev1.Node)
			if node.Name == n.cfg.NodeName {
				continue
			}
			if p, ok := flannel.PeerFromNode(node); ok {
				peers = append(peers, p)
			}
		}
		sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
		n.dp.SetPeers(peers)
	}
	_, _ = inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { sync() },
		UpdateFunc: func(_, o any) { sync() },
		DeleteFunc: func(any) { sync() },
	})
	f.Start(ctx.Done())
	f.WaitForCacheSync(ctx.Done())
	sync()
}

func (n *Network) lookupDNSVIP(ctx context.Context) string {
	svc, err := n.cfg.Client.CoreV1().Services("kube-system").Get(ctx, "kube-dns", metav1.GetOptions{})
	if err == nil && svc.Spec.ClusterIP != "" && svc.Spec.ClusterIP != corev1.ClusterIPNone {
		return svc.Spec.ClusterIP
	}
	n.log.Warn("no kube-system/kube-dns Service; pods resolve with the host's DNS only", "err", err)
	return ""
}

func (n *Network) startProxy(ctx context.Context) error {
	n.table = proxy.NewRoutingTable(n.podCIDR)
	p := proxy.New(n.table,
		proxy.WithLogger(n.log),
		proxy.WithVMNetPrefix(n.cfg.VMSubnet),
		proxy.WithClusterPodCIDR(n.cfg.ClusterCIDR),
		proxy.WithMeshEgressSource(n.dp.LinkIP),
		proxy.WithNodeAddress(n.cfg.NodeIP),
	)
	var opts []proxy.WatcherOption
	if a := n.cfg.APIServer; a.IsValid() {
		opts = append(opts, proxy.WithStaticBackends(map[string][]netv1.Endpoint{
			"default/kubernetes": {{IP: a.Addr().String(), Port: int32(a.Port()), Ready: true}},
		}))
	}
	w := proxy.NewWatcher(n.cfg.Client, p, n.log, opts...)
	go func() {
		if err := p.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			n.log.Error("service proxy stopped", "err", err)
		}
	}()
	go func() {
		if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			n.log.Error("service watcher stopped", "err", err)
		}
	}()
	return nil
}

func (n *Network) wait(ctx context.Context) error {
	select {
	case <-n.ready:
		return n.err
	case <-ctx.Done():
		return fmt.Errorf("pod network not ready: %w", ctx.Err())
	}
}

// SetHostNetwork marks a pod as sharing the node's addresses. It must be
// called before Setup.
func (n *Network) SetHostNetwork(podID string) {
	n.mu.Lock()
	n.hostNet[podID] = true
	n.mu.Unlock()
}

func (n *Network) isHostNetwork(podID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.hostNet[podID]
}

// Setup implements runtimed's PodNetwork: it allocates the pod's IP, or
// returns the one it already has (the provider calls it before runtimed does,
// to put the IP in the pod's environment).
func (n *Network) Setup(ctx context.Context, podID string) (string, error) {
	if n.isHostNetwork(podID) {
		return n.cfg.NodeIP.String(), nil
	}
	if err := n.wait(ctx); err != nil {
		return "", err
	}
	ip, err := n.ipam.Setup(ctx, podID)
	if err != nil {
		return "", err
	}
	return ip.String(), nil
}

// Teardown implements runtimed's PodNetwork: it releases the pod's IP. For a
// vm pod the provider calls it, since runtimed never allocated the IP.
func (n *Network) Teardown(podID string) error {
	n.mu.Lock()
	host := n.hostNet[podID]
	delete(n.hostNet, podID)
	_, wasGuest := n.guests[podID]
	delete(n.guests, podID)
	n.mu.Unlock()
	if wasGuest {
		// Close the relay before the alias it listens on goes away.
		n.syncTransport()
	}
	if host {
		return nil
	}
	select {
	case <-n.ready:
	default:
		return nil // nothing was allocated before the network came up
	}
	if n.err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return n.ipam.Teardown(ctx, podID)
}

// Reattach re-claims the IP a pod had before the daemon restarted. It reports
// false when ip is not one of this node's pod IPs (a pod from before pod
// networking, which keeps the node IP).
func (n *Network) Reattach(ctx context.Context, podID string, ip netip.Addr, vm bool, namespace string) (bool, error) {
	if n.isHostNetwork(podID) {
		return false, nil
	}
	if err := n.wait(ctx); err != nil {
		return false, err
	}
	if !podnet.IsPodAddress(n.podCIDR, ip) {
		return false, nil
	}
	if !vm {
		return true, n.ipam.ReattachPod(ctx, podID, ip)
	}
	if err := n.ipam.ReattachGuest(ctx, podID, ip); err != nil {
		return false, err
	}
	n.mu.Lock()
	n.guests[podID] = &guest{cfg: n.guestConfig(ip, namespace)}
	n.mu.Unlock()
	return true, nil
}

// Sweep removes the lo0 aliases of pod IPs nothing re-attached: pods that
// ended while the daemon was down.
func (n *Network) Sweep(ctx context.Context) error {
	select {
	case <-n.ready:
	default:
		return nil // not up yet: a first join, with no earlier pods
	}
	if n.err != nil {
		return nil
	}
	return n.ipam.SweepStale(ctx, nil)
}

// SetupGuest allocates a vm pod's published IP. The guest itself stays on its
// vmnet lease; the proxy relays the published IP's ports to it once
// SetGuestTransport names the lease.
func (n *Network) SetupGuest(ctx context.Context, podID, namespace string) (string, error) {
	if err := n.wait(ctx); err != nil {
		return "", err
	}
	g, err := n.ipam.SetupGuest(ctx, podID)
	if err != nil {
		return "", err
	}
	n.mu.Lock()
	if _, ok := n.guests[podID]; !ok {
		n.guests[podID] = &guest{cfg: n.guestConfig(g.PodIP, namespace)}
	}
	n.mu.Unlock()
	return g.PodIP.String(), nil
}

// guestConfig points the guest's resolver at cluster DNS: its queries reach
// the DNS VIP on this host's lo0 through the vmnet gateway.
func (n *Network) guestConfig(ip netip.Addr, namespace string) sandbox.GuestNetworkConfig {
	cfg := sandbox.GuestNetworkConfig{PodIP: ip, NATSubnet: n.cfg.VMSubnet}
	if vip, err := netip.ParseAddr(n.dnsVIP); err == nil {
		d := dns.PodDNSConfig(n.dnsVIP, n.cfg.ClusterDomain, namespace)
		cfg.DNSVIP = vip
		cfg.Nameservers = []string{n.dnsVIP}
		cfg.Searches = d.SearchDomains
		cfg.Options = []string{fmt.Sprintf("ndots:%d", d.NDots)}
	}
	return cfg
}

// GuestNetwork implements runtimed's GuestNetworker for the vm pods given an
// IP by SetupGuest or Reattach.
func (n *Network) GuestNetwork(podID string) (sandbox.GuestNetworkConfig, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	g, ok := n.guests[podID]
	if !ok {
		return sandbox.GuestNetworkConfig{}, false
	}
	return g.cfg, true
}

// SetGuestTransport records a vm pod's live lease and declared TCP ports, so
// the proxy relays its published IP to the guest.
func (n *Network) SetGuestTransport(podID string, live netip.Addr, ports []uint16) {
	n.mu.Lock()
	g, ok := n.guests[podID]
	changed := ok && (g.live != live || !slices.Equal(g.ports, ports))
	if changed {
		g.live, g.ports = live, ports
	}
	n.mu.Unlock()
	if changed {
		n.syncTransport()
	}
}

func (n *Network) syncTransport() {
	if n.table == nil {
		return
	}
	m := map[netip.Addr]proxy.VMPodTransport{}
	n.mu.Lock()
	for _, g := range n.guests {
		if g.live.IsValid() {
			m[g.cfg.PodIP] = proxy.VMPodTransport{Live: g.live, Ports: g.ports}
		}
	}
	n.mu.Unlock()
	n.table.SetTransportOverrides(m)
}

// Env is what every container of a pod with this IP and namespace gets: the
// bind shim's pod IP and cluster scope, and the getaddrinfo shim's DNS config.
func (n *Network) Env(podIP netip.Addr, namespace string) map[string]string {
	env := map[string]string{}
	maps.Copy(env, podnet.BindDisciplineEnvWithCIDRs(podIP, []netip.Prefix{n.cfg.ClusterCIDR, n.cfg.ServiceCIDR}))
	if n.dnsVIP != "" {
		maps.Copy(env, dns.ConfigToEnv(dns.PodDNSConfig(n.dnsVIP, n.cfg.ClusterDomain, namespace)))
	}
	return env
}

// Peers lists the flannel peers this node routes to, for logs and tests.
func (n *Network) Peers() []string {
	if n.dp == nil {
		return nil
	}
	var out []string
	for _, p := range n.dp.Peers() {
		out = append(out, fmt.Sprintf("%s=%s", p.Name, p.PodCIDR))
	}
	return slices.Sorted(slices.Values(out))
}
