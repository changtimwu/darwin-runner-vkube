// Package flannel makes a darwin node a peer of the cluster's flannel VXLAN
// network. flannel's kube subnet manager learns a node's tunnel from its
// annotations and installs a static route, neighbor and FDB entry for the
// node's podCIDR, so publishing those annotations is all the Linux side needs.
// This side answers in userspace: VXLAN frames arrive on UDP 8472 and their
// inner IPv4 packets are written to a utun, and packets the kernel routes to
// the utun are framed and sent to the peer whose podCIDR holds the destination.
package flannel

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/netip"

	corev1 "k8s.io/api/core/v1"
)

// The annotations flannel's kube subnet manager reads and writes on each node
// (k3s keeps flannel's default prefix).
const (
	AnnotationBackendType = "flannel.alpha.coreos.com/backend-type"
	AnnotationBackendData = "flannel.alpha.coreos.com/backend-data"
	AnnotationPublicIP    = "flannel.alpha.coreos.com/public-ip"
	AnnotationSubnetMgr   = "flannel.alpha.coreos.com/kube-subnet-manager"
)

// Port is flannel's VXLAN UDP port (the Linux kernel default, 4789, is not
// what flannel uses).
const Port = 8472

const (
	ethHeaderLen   = 14
	vxlanHeaderLen = 8
	// HeaderLen is the framing in front of every inner IPv4 packet.
	HeaderLen = vxlanHeaderLen + ethHeaderLen
	// MTU leaves room for the outer IPv4, UDP and HeaderLen in a 1500-byte
	// link, as flannel sizes flannel.1.
	MTU = 1500 - 20 - 8 - HeaderLen
)

type backendData struct {
	VNI     int    `json:"VNI"`
	VtepMAC string `json:"VtepMAC"`
}

// Peer is another node's end of the VXLAN network.
type Peer struct {
	Name     string
	PodCIDR  netip.Prefix
	PublicIP netip.Addr
	VtepMAC  net.HardwareAddr
	VNI      uint32
}

// VtepMAC derives this node's VTEP MAC from its name: stable across restarts,
// so peers' static FDB entries stay valid, and locally administered unicast.
func VtepMAC(nodeName string) net.HardwareAddr {
	sum := sha256.Sum256([]byte("darwin-vk/vtep/" + nodeName))
	mac := net.HardwareAddr(sum[:6])
	mac[0] = mac[0]&^0x01 | 0x02
	return mac
}

// Annotations are what this node publishes for flannel to peer with it.
func Annotations(publicIP netip.Addr, mac net.HardwareAddr, vni uint32) map[string]string {
	data, _ := json.Marshal(backendData{VNI: int(vni), VtepMAC: mac.String()})
	return map[string]string{
		AnnotationBackendType: "vxlan",
		AnnotationBackendData: string(data),
		AnnotationPublicIP:    publicIP.String(),
		AnnotationSubnetMgr:   "true",
	}
}

// PeerFromNode reads a node's flannel VXLAN annotations. It reports false for
// a node flannel has not set up (yet) or that uses another backend.
func PeerFromNode(n *corev1.Node) (Peer, bool) {
	a := n.Annotations
	if a[AnnotationBackendType] != "vxlan" {
		return Peer{}, false
	}
	var bd backendData
	if err := json.Unmarshal([]byte(a[AnnotationBackendData]), &bd); err != nil || bd.VNI <= 0 {
		return Peer{}, false
	}
	mac, err := net.ParseMAC(bd.VtepMAC)
	if err != nil || len(mac) != 6 {
		return Peer{}, false
	}
	ip, err := netip.ParseAddr(a[AnnotationPublicIP])
	if err != nil || !ip.Is4() {
		return Peer{}, false
	}
	cidr, err := netip.ParsePrefix(n.Spec.PodCIDR)
	if err != nil || !cidr.Addr().Is4() {
		return Peer{}, false
	}
	return Peer{Name: n.Name, PodCIDR: cidr.Masked(), PublicIP: ip, VtepMAC: mac, VNI: uint32(bd.VNI)}, true
}

// frame writes the VXLAN and Ethernet headers into b[:HeaderLen], in front of
// the inner packet at b[HeaderLen:].
func frame(b []byte, vni uint32, src, dst net.HardwareAddr) {
	b[0] = 0x08 // I flag: the VNI is valid
	b[1], b[2], b[3] = 0, 0, 0
	binary.BigEndian.PutUint32(b[4:8], vni<<8)
	eth := b[vxlanHeaderLen:HeaderLen]
	copy(eth[0:6], dst)
	copy(eth[6:12], src)
	binary.BigEndian.PutUint16(eth[12:14], 0x0800)
}

// unframe returns the inner IPv4 packet of a VXLAN frame addressed to mac on
// vni. ARP is never needed: flannel installs static neighbor entries.
func unframe(b []byte, vni uint32, mac net.HardwareAddr) ([]byte, error) {
	if len(b) < HeaderLen+20 {
		return nil, fmt.Errorf("short frame (%d bytes)", len(b))
	}
	if b[0]&0x08 == 0 || binary.BigEndian.Uint32(b[4:8])>>8 != vni {
		return nil, fmt.Errorf("not VNI %d", vni)
	}
	eth := b[vxlanHeaderLen:HeaderLen]
	if !bytes.Equal(eth[0:6], mac) {
		return nil, fmt.Errorf("for another VTEP %s", net.HardwareAddr(eth[0:6]))
	}
	if binary.BigEndian.Uint16(eth[12:14]) != 0x0800 || b[HeaderLen]>>4 != 4 {
		return nil, fmt.Errorf("not IPv4")
	}
	return b[HeaderLen:], nil
}

// destination is an IPv4 packet's destination address.
func destination(pkt []byte) (netip.Addr, bool) {
	if len(pkt) < 20 || pkt[0]>>4 != 4 {
		return netip.Addr{}, false
	}
	return netip.AddrFrom4([4]byte(pkt[16:20])), true
}
