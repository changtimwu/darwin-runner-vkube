package rtprovider

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

func TestBackendFor(t *testing.T) {
	client := fake.NewClientset(
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "vm"}, Handler: "vm"},
		&nodev1.RuntimeClass{ObjectMeta: metav1.ObjectMeta{Name: "gvisor"}, Handler: "runsc"},
	)
	rc := func(name string) *corev1.Pod {
		pod := &corev1.Pod{}
		if name != "" {
			pod.Spec.RuntimeClassName = &name
		}
		return pod
	}
	p := &Provider{cfg: Config{Client: client}, handlers: map[string]string{}, vmAvailable: true}
	if b, err := p.backendFor(rc("")); err != nil || b != runtimev1.SandboxBackend_SANDBOX_BACKEND_UNSPECIFIED {
		t.Errorf("no runtimeClassName = %v, %v; want native", b, err)
	}
	if b, err := p.backendFor(rc("vm")); err != nil || b != runtimev1.SandboxBackend_SANDBOX_BACKEND_VM {
		t.Errorf("vm = %v, %v; want VM", b, err)
	}
	if p.handlers["vm"] != "vm" {
		t.Errorf("handler not cached: %v", p.handlers)
	}
	for _, name := range []string{"gvisor", "missing"} {
		if _, err := p.backendFor(rc(name)); err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	p.vmAvailable = false
	if _, err := p.backendFor(rc("vm")); err == nil || !strings.Contains(err.Error(), "cannot run micro-VMs") {
		t.Errorf("vm on an incapable node = %v; want refusal", err)
	}
}

func TestVMSizing(t *testing.T) {
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{
		{Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("1500m"), corev1.ResourceMemory: resource.MustParse("1Gi")}}},
		{Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}}},
	}}}
	if v, m := vmSizing(pod); v != 2 || m != 0 {
		// One container has no memory limit, so the pod has none.
		t.Errorf("vmSizing = %d, %d; want 2 vCPUs, backend-default memory", v, m)
	}
	if v, m := vmSizing(&corev1.Pod{}); v != 0 || m != 0 {
		t.Errorf("no limits = %d, %d; want backend defaults", v, m)
	}
}

func TestHostNameservers(t *testing.T) {
	f := filepath.Join(t.TempDir(), "resolv.conf")
	_ = os.WriteFile(f, []byte("# macOS\nnameserver 192.168.1.1\nnameserver 127.0.0.1\nnameserver fe80::1%en0\nnameserver 8.8.8.8\n"), 0o644)
	if got := hostNameservers(f); !slices.Equal(got, []string{"192.168.1.1", "8.8.8.8"}) {
		t.Errorf("hostNameservers = %v", got)
	}
}

func TestForwarder(t *testing.T) {
	guest, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	go func() {
		for {
			c, err := guest.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); c.Close() }()
		}
	}()
	port := guest.Addr().(*net.TCPAddr).Port
	f := &forwarder{target: "127.0.0.1"}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.listeners = []net.Listener{ln}
	go f.serve(ln, port)
	defer f.close()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("ping"))
	_ = c.(*net.TCPConn).CloseWrite()
	got, _ := io.ReadAll(c)
	if string(got) != "ping" {
		t.Errorf("echo through forwarder = %q", got)
	}
}
