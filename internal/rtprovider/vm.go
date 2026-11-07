package rtprovider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/sandbox"
	"k3sm.io/runtimed/pkg/supervisor"
)

// backendFor resolves the pod's runtimeClassName to a runtimed backend through
// the RuntimeClass object's handler, as the kubelet hands the handler to its
// CRI. Handlers are immutable, so the answer is cached and podBox stays
// deterministic for re-attach.
func (p *Provider) backendFor(pod *corev1.Pod) (runtimev1.SandboxBackend, error) {
	name := pod.Spec.RuntimeClassName
	if name == nil || *name == "" {
		return runtimev1.SandboxBackend_SANDBOX_BACKEND_UNSPECIFIED, nil
	}
	p.mu.Lock()
	h, ok := p.handlers[*name]
	p.mu.Unlock()
	if !ok {
		if p.cfg.Client == nil {
			return 0, fmt.Errorf("runtimeClassName %q: no API client to resolve it", *name)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rc, err := p.cfg.Client.NodeV1().RuntimeClasses().Get(ctx, *name, metav1.GetOptions{})
		if err != nil {
			return 0, fmt.Errorf("runtimeClassName %q: %w", *name, err)
		}
		h = rc.Handler
		p.mu.Lock()
		p.handlers[*name] = h
		p.mu.Unlock()
	}
	b, err := runtimev1.DefaultHandlerConfig().Backend(runtimev1.HandlerName(h))
	if err != nil || b != runtimev1.SandboxBackend_SANDBOX_BACKEND_VM {
		return 0, fmt.Errorf("runtimeClassName %q: handler %q is not supported on darwin nodes (supported: %q)", *name, h, runtimev1.HandlerVM)
	}
	if !p.vmAvailable {
		return 0, fmt.Errorf("runtimeClassName %q: this node cannot run micro-VMs (see the darwin-vk log for why)", *name)
	}
	return b, nil
}

// vmSizing derives the guest's vCPUs and RAM from the pod's limits; zero lets
// runtimed choose. A VM needs a concrete size, so limits, not requests, set it.
func vmSizing(pod *corev1.Pod) (vcpus uint32, mem int64) {
	var milli int64
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			milli += q.MilliValue()
		}
	}
	if milli > 0 {
		vcpus = uint32((milli + 999) / 1000)
	}
	return vcpus, podMemoryLimit(pod)
}

// guestNetwork is runtimed's pod network: native pods share the host's
// network, and vm pods get the node IP as their published identity plus the
// host's resolvers. Until pods get cluster-routable IPs, a vm pod is reached
// through its ports forwarded on the node IP (see forwarder).
type guestNetwork struct {
	supervisor.NodeNetwork
	nodeIP      netip.Addr
	nameservers []string
}

func newGuestNetwork(nodeIP string) guestNetwork {
	ip, _ := netip.ParseAddr(nodeIP)
	// Native pods get the node IP too; NodeNetwork's zero value is loopback.
	return guestNetwork{NodeNetwork: supervisor.NodeNetwork{IP: nodeIP}, nodeIP: ip, nameservers: hostNameservers("/etc/resolv.conf")}
}

func (g guestNetwork) GuestNetwork(string) (sandbox.GuestNetworkConfig, bool) {
	return sandbox.GuestNetworkConfig{PodIP: g.nodeIP, Nameservers: g.nameservers}, g.nodeIP.IsValid()
}

// hostNameservers reads the IPv4 resolvers macOS mirrors into resolv.conf.
// Loopback resolvers are useless inside the guest and are skipped.
func hostNameservers(path string) []string {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[0] != "nameserver" {
			continue
		}
		if a, err := netip.ParseAddr(f[1]); err == nil && a.Is4() && !a.IsLoopback() {
			out = append(out, a.String())
		}
	}
	return out
}

// forwarder proxies a vm pod's container ports from the node to its guest,
// whose address is a NAT lease only this host can reach.
type forwarder struct {
	mu        sync.Mutex
	target    string // guest transport address
	listeners []net.Listener
}

// syncForward starts or retargets the pod's forwarder once the guest has an
// address. Called with p.mu held.
func (p *Provider) syncForward(t *tracked, st *runtimev1.PodStatus) {
	addr := st.GetGuestTransportAddress()
	if !t.vm || addr == "" {
		return
	}
	if p.cfg.Network != nil && st.GetPodIp() != p.cfg.NodeIP {
		// The pod has its own IP: the network relays it to the guest.
		if live, err := netip.ParseAddr(addr); err == nil {
			p.cfg.Network.SetGuestTransport(string(t.pod.UID), live, tcpPorts(t.pod))
		}
		return
	}
	if t.fwd != nil {
		t.fwd.mu.Lock()
		t.fwd.target = addr
		t.fwd.mu.Unlock()
		return
	}
	f := &forwarder{target: addr}
	for _, c := range t.pod.Spec.Containers {
		for _, port := range c.Ports {
			if port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
				continue
			}
			ln, err := net.Listen("tcp", net.JoinHostPort("", strconv.Itoa(int(port.ContainerPort))))
			if err != nil {
				p.log.Warn("forward vm pod port", "pod", key(t.pod), "port", port.ContainerPort, "err", err)
				p.event(t.pod, corev1.EventTypeWarning, "FailedPortForward", "port %d is not reachable on the node: %s", port.ContainerPort, err)
				continue
			}
			f.listeners = append(f.listeners, ln)
			go f.serve(ln, int(port.ContainerPort))
		}
	}
	t.fwd = f
}

func (f *forwarder) serve(ln net.Listener, port int) {
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		f.mu.Lock()
		target := f.target
		f.mu.Unlock()
		go proxy(c, net.JoinHostPort(target, strconv.Itoa(port)))
	}
}

func proxy(c net.Conn, target string) {
	defer c.Close()
	g, err := net.DialTimeout("tcp", target, 5*time.Second)
	if err != nil {
		return
	}
	defer g.Close()
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		if tc, ok := dst.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
		done <- struct{}{}
	}
	go cp(g, c)
	go cp(c, g)
	<-done
	<-done
}

func (f *forwarder) close() {
	if f == nil {
		return
	}
	for _, ln := range f.listeners {
		_ = ln.Close()
	}
}

// probeHost is where http, tcp and grpc probes dial: the guest itself for a
// vm pod, the pod's own IP for a native pod that has one, the node otherwise.
func (p *Provider) probeHost(pod *corev1.Pod) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.pods[string(pod.UID)]; ok {
		if t.vm {
			if a := t.status.GetGuestTransportAddress(); a != "" {
				return a
			}
		} else if ip := t.status.GetPodIp(); ip != "" {
			return ip
		}
	}
	return p.cfg.NodeIP
}

// tcpPorts are the pod's declared TCP container ports.
func tcpPorts(pod *corev1.Pod) []uint16 {
	var out []uint16
	for _, c := range pod.Spec.Containers {
		for _, port := range c.Ports {
			if port.Protocol == "" || port.Protocol == corev1.ProtocolTCP {
				out = append(out, uint16(port.ContainerPort))
			}
		}
	}
	return out
}

func isVM(box *runtimev1.PodBox) bool {
	return box.GetSandboxProfile().GetBackend() == runtimev1.SandboxBackend_SANDBOX_BACKEND_VM
}
