package flannel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"golang.zx2c4.com/wireguard/tun"
)

// Config describes this node's end of the VXLAN network.
type Config struct {
	PodCIDR netip.Prefix
	VNI     uint32
	VtepMAC net.HardwareAddr
	// Endpoints overrides where frames for a peer's public IP are sent. Only a
	// test topology needs it: Docker Desktop cannot route from the host to a
	// container's address, so the container publishes 8472 on another port.
	Endpoints map[netip.Addr]netip.AddrPort
	Logger    *slog.Logger
}

// Datapath is the utun and UDP socket that carry pod traffic to and from the
// flannel peers. Root-only: it creates a utun and installs routes.
type Datapath struct {
	cfg   Config
	log   *slog.Logger
	dev   tun.Device
	iface string
	conn  *net.UDPConn
	// LinkIP is the utun's address, the first host of PodCIDR, so traffic this
	// host originates toward pods leaves with a source the peers route back.
	LinkIP netip.Addr

	peers atomic.Pointer[[]Peer]

	mu     sync.Mutex // guards routes and serializes SetPeers
	routes map[netip.Prefix]bool
}

// Open creates the utun, gives it LinkIP and binds the VXLAN port.
func Open(cfg Config) (*Datapath, error) {
	if !cfg.PodCIDR.Addr().Is4() || cfg.PodCIDR.Bits() > 30 {
		return nil, fmt.Errorf("pod CIDR %s: want an IPv4 prefix of /30 or wider", cfg.PodCIDR)
	}
	d := &Datapath{cfg: cfg, log: cfg.Logger, routes: map[netip.Prefix]bool{}, LinkIP: cfg.PodCIDR.Masked().Addr().Next()}
	if d.log == nil {
		d.log = slog.Default()
	}
	d.peers.Store(&[]Peer{})
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: Port})
	if err != nil {
		return nil, fmt.Errorf("listen on VXLAN port %d: %w", Port, err)
	}
	d.conn = conn
	if d.dev, err = tun.CreateTUN("utun", MTU); err != nil {
		conn.Close()
		return nil, fmt.Errorf("create utun: %w", err)
	}
	if d.iface, err = d.dev.Name(); err == nil {
		ip := d.LinkIP.String()
		err = run("ifconfig", d.iface, "inet", ip, ip, "netmask", "255.255.255.255", "up")
	}
	if err != nil {
		d.Close()
		return nil, err
	}
	return d, nil
}

// Interface is the utun's name, e.g. utun5.
func (d *Datapath) Interface() string { return d.iface }

// SetPeers replaces the peer set and routes each peer's podCIDR to the utun.
func (d *Datapath) SetPeers(peers []Peer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	want := map[netip.Prefix]bool{}
	var keep []Peer
	for _, p := range peers {
		if p.VNI != d.cfg.VNI || p.PodCIDR.Overlaps(d.cfg.PodCIDR) {
			d.log.Warn("ignoring flannel peer", "node", p.Name, "podCIDR", p.PodCIDR, "vni", p.VNI)
			continue
		}
		keep = append(keep, p)
		want[p.PodCIDR] = true
	}
	d.peers.Store(&keep)
	for cidr := range d.routes {
		if !want[cidr] {
			if err := run("route", "-n", "delete", "-net", cidr.String(), "-interface", d.iface); err != nil {
				d.log.Warn("remove flannel route", "cidr", cidr, "err", err)
			}
			delete(d.routes, cidr)
		}
	}
	for cidr := range want {
		if d.routes[cidr] {
			continue
		}
		if err := run("route", "-n", "add", "-net", cidr.String(), "-interface", d.iface); err != nil {
			d.log.Warn("add flannel route", "cidr", cidr, "err", err)
			continue
		}
		d.routes[cidr] = true
	}
}

// Run moves packets until ctx ends, then closes the datapath. Closing the utun
// also drops its routes.
func (d *Datapath) Run(ctx context.Context) {
	go func() {
		<-ctx.Done()
		d.Close()
	}()
	go d.egress()
	d.ingress()
}

// egress frames what the kernel routes to the utun and sends it to the peer
// that owns the destination.
func (d *Datapath) egress() {
	buf := make([]byte, HeaderLen+65535)
	bufs, sizes := [][]byte{buf}, []int{0}
	for {
		if _, err := d.dev.Read(bufs, sizes, HeaderLen); err != nil {
			if !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "closed") {
				d.log.Error("read utun", "err", err)
			}
			return
		}
		pkt := buf[HeaderLen : HeaderLen+sizes[0]]
		dst, ok := destination(pkt)
		if !ok {
			continue
		}
		p, ok := d.peerFor(dst)
		if !ok {
			continue
		}
		frame(buf, d.cfg.VNI, d.cfg.VtepMAC, p.VtepMAC)
		to := netip.AddrPortFrom(p.PublicIP, Port)
		if ep, ok := d.cfg.Endpoints[p.PublicIP]; ok {
			to = ep
		}
		if _, err := d.conn.WriteToUDPAddrPort(buf[:HeaderLen+len(pkt)], to); err != nil {
			d.log.Debug("send VXLAN frame", "peer", p.Name, "err", err)
		}
	}
}

// ingress delivers the inner packets of frames from peers to the kernel.
func (d *Datapath) ingress() {
	buf := make([]byte, 65535)
	for {
		n, from, err := d.conn.ReadFromUDPAddrPort(buf)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				d.log.Error("read VXLAN port", "err", err)
			}
			return
		}
		pkt, err := unframe(buf[:n], d.cfg.VNI, d.cfg.VtepMAC)
		if err != nil {
			d.log.Debug("drop VXLAN frame", "from", from, "err", err)
			continue
		}
		// Only a peer's pods (or the peer itself, from its podCIDR's network
		// address) may send into this node's pods.
		if src := netip.AddrFrom4([4]byte(pkt[12:16])); !d.fromPeer(src) {
			d.log.Debug("drop VXLAN frame from outside the peers' podCIDRs", "from", from, "src", src)
			continue
		}
		if _, err := d.dev.Write([][]byte{buf[:n]}, HeaderLen); err != nil {
			d.log.Debug("write utun", "err", err)
		}
	}
}

func (d *Datapath) peerFor(ip netip.Addr) (Peer, bool) {
	for _, p := range *d.peers.Load() {
		if p.PodCIDR.Contains(ip) {
			return p, true
		}
	}
	return Peer{}, false
}

func (d *Datapath) fromPeer(ip netip.Addr) bool {
	_, ok := d.peerFor(ip)
	return ok
}

// Peers is the current peer set.
func (d *Datapath) Peers() []Peer { return slices.Clone(*d.peers.Load()) }

// Close releases the socket and the utun.
func (d *Datapath) Close() {
	if d.conn != nil {
		_ = d.conn.Close()
	}
	if d.dev != nil {
		_ = d.dev.Close()
	}
}

func run(name string, args ...string) error {
	if out, err := exec.Command(name, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
