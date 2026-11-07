package flannel

import (
	"bytes"
	"net/netip"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestVtepMAC(t *testing.T) {
	a, b := VtepMAC("mac-a"), VtepMAC("mac-a")
	if !bytes.Equal(a, b) {
		t.Errorf("not stable: %s vs %s", a, b)
	}
	if bytes.Equal(a, VtepMAC("mac-b")) {
		t.Error("two nodes share a MAC")
	}
	if a[0]&0x01 != 0 || a[0]&0x02 == 0 {
		t.Errorf("%s is not a locally administered unicast MAC", a)
	}
}

func TestPeerFromNode(t *testing.T) {
	mac := VtepMAC("self")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "self", Annotations: Annotations(netip.MustParseAddr("192.168.1.101"), mac, 1)},
		Spec:       corev1.NodeSpec{PodCIDR: "10.42.1.0/24"},
	}
	p, ok := PeerFromNode(node)
	if !ok || p.PodCIDR != netip.MustParsePrefix("10.42.1.0/24") || p.PublicIP.String() != "192.168.1.101" || !bytes.Equal(p.VtepMAC, mac) || p.VNI != 1 {
		t.Fatalf("round trip = %+v, %v", p, ok)
	}
	// What k3s's flannel writes on a Linux node.
	linux := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "linux", Annotations: map[string]string{
			AnnotationBackendType: "vxlan",
			AnnotationBackendData: `{"VNI":1,"VtepMAC":"86:08:eb:cb:83:f7"}`,
			AnnotationPublicIP:    "172.17.0.3",
		}},
		Spec: corev1.NodeSpec{PodCIDR: "10.42.0.0/24"},
	}
	if p, ok := PeerFromNode(linux); !ok || p.VtepMAC.String() != "86:08:eb:cb:83:f7" {
		t.Errorf("linux node = %+v, %v", p, ok)
	}
	for name, mutate := range map[string]func(n *corev1.Node){
		"wireguard backend": func(n *corev1.Node) { n.Annotations[AnnotationBackendType] = "wireguard-native" },
		"no podCIDR":        func(n *corev1.Node) { n.Spec.PodCIDR = "" },
		"bad MAC":           func(n *corev1.Node) { n.Annotations[AnnotationBackendData] = `{"VNI":1,"VtepMAC":"zz"}` },
		"no public IP":      func(n *corev1.Node) { delete(n.Annotations, AnnotationPublicIP) },
	} {
		n := linux.DeepCopy()
		mutate(n)
		if _, ok := PeerFromNode(n); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	src, dst := VtepMAC("a"), VtepMAC("b")
	inner := make([]byte, 40)
	inner[0] = 0x45
	copy(inner[16:20], []byte{10, 42, 0, 7})
	buf := append(make([]byte, HeaderLen), inner...)
	frame(buf, 1, src, dst)

	got, err := unframe(buf, 1, dst)
	if err != nil || !bytes.Equal(got, inner) {
		t.Fatalf("unframe = %x, %v", got, err)
	}
	if ip, ok := destination(got); !ok || ip.String() != "10.42.0.7" {
		t.Errorf("destination = %v, %v", ip, ok)
	}
	if _, err := unframe(buf, 2, dst); err == nil {
		t.Error("accepted another VNI")
	}
	if _, err := unframe(buf, 1, src); err == nil {
		t.Error("accepted a frame for another VTEP")
	}
	if _, err := unframe(buf[:HeaderLen+10], 1, dst); err == nil {
		t.Error("accepted a short frame")
	}
}
