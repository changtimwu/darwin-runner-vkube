package rtprovider

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mlxv1alpha1 "k3sm.io/apis/mlx/v1alpha1"
	runtimev1 "k3sm.io/apis/runtime/v1"

	"github.com/changtimwu/darwin-runner-vkube/internal/gpu"
)

func testPod(policy corev1.RestartPolicy, ctrs ...corev1.Container) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p", UID: "0b1e6c4e-5c9e-4d8b-9a6c-1f2e3d4c5b6a"},
		Spec:       corev1.PodSpec{RestartPolicy: policy, Containers: ctrs},
	}
}

func TestPodBox(t *testing.T) {
	p := &Provider{cfg: Config{NodeIP: "10.0.0.5", Root: "/r", PodLogsDir: "/logs", PodUID: 300, PodGID: 300}}
	pod := testPod(corev1.RestartPolicyAlways,
		corev1.Container{Name: "a", Image: "host", Command: []string{"sh", "-c", "true"},
			Env: []corev1.EnvVar{{Name: "K", Value: "V"}}},
		corev1.Container{Name: "b", Image: "localhost:5050/x:1",
			Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{mlxv1alpha1.ResourceGPU: resource.MustParse("1")}}},
	)
	box, err := p.podBox(pod)
	if err != nil {
		t.Fatal(err)
	}
	a, b := box.GetContainers()[0], box.GetContainers()[1]
	if a.GetImage() != nativeImage || !filepath.IsAbs(a.GetCommand()[0]) {
		t.Errorf("host image not mapped to native with absolute command: %q %v", a.GetImage(), a.GetCommand())
	}
	if b.GetImage() != "localhost:5050/x:1" {
		t.Errorf("OCI image changed: %q", b.GetImage())
	}
	if !box.GetSandboxProfile().GetAllowGpu() {
		t.Error("GPU limit did not set allow_gpu")
	}
	if got, want := box.GetSandboxProfile().GetDataVolumePath(), "/r/pods/"+string(pod.UID)+"/rootfs"; got != want {
		t.Errorf("data volume %q, want %q", got, want)
	}
	if box.GetUid() != 300 || box.GetPodIp() != "10.0.0.5" || box.GetLogDirectory() != "/logs/ns_p_"+string(pod.UID) {
		t.Errorf("identity/ip/log dir wrong: %v", box)
	}

	if _, err := p.podBox(testPod("", corev1.Container{Name: "x", Image: "native"})); err == nil {
		t.Error("native image without command accepted")
	}

	p.developerDir = "/Applications/Xcode.app/Contents/Developer"
	if box.GetSandboxProfile().GetXcodeToolchainDir() != "" {
		t.Error("toolchain granted without the annotation")
	}
	pod.Annotations = map[string]string{runtimev1.AnnotationXcodeToolchain: ""}
	if box, _ = p.podBox(pod); box.GetSandboxProfile().GetXcodeToolchainDir() != p.developerDir {
		t.Error("xcode-toolchain annotation not granted")
	}
}

func exited(code int32) *runtimev1.ContainerStatus {
	return &runtimev1.ContainerStatus{Name: "c", State: &runtimev1.ContainerState{Terminated: &runtimev1.ContainerStateTerminated{ExitCode: code}}}
}

func running() *runtimev1.ContainerStatus {
	return &runtimev1.ContainerStatus{Name: "c", Ready: true, State: &runtimev1.ContainerState{Running: &runtimev1.ContainerStateRunning{}}}
}

func TestPhase(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  corev1.RestartPolicy
		cs      *runtimev1.ContainerStatus
		backoff bool
		want    corev1.PodPhase
	}{
		{"running", corev1.RestartPolicyAlways, running(), false, corev1.PodRunning},
		{"never success", corev1.RestartPolicyNever, exited(0), false, corev1.PodSucceeded},
		{"never failure", corev1.RestartPolicyNever, exited(1), false, corev1.PodFailed},
		{"onfailure success", corev1.RestartPolicyOnFailure, exited(0), false, corev1.PodSucceeded},
		{"onfailure failure restarts", corev1.RestartPolicyOnFailure, exited(1), true, corev1.PodRunning},
		{"always exit stays running", corev1.RestartPolicyAlways, exited(0), true, corev1.PodRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := testPod(tc.policy, corev1.Container{Name: "c"})
			st := &runtimev1.PodStatus{ContainerStatuses: []*runtimev1.ContainerStatus{tc.cs}}
			var bo map[string]bool
			if tc.backoff {
				bo = map[string]bool{"c": true}
			}
			got := toPodStatus(pod, st, "10.0.0.5", bo)
			if got.Phase != tc.want {
				t.Errorf("phase %s, want %s", got.Phase, tc.want)
			}
			if tc.backoff && got.ContainerStatuses[0].State.Waiting == nil {
				t.Error("back-off not reported as Waiting")
			}
		})
	}
	if got := toPodStatus(testPod("", corev1.Container{Name: "c"}), nil, "ip", nil); got.Phase != corev1.PodPending ||
		got.ContainerStatuses[0].State.Waiting.Reason != "ContainerCreating" {
		t.Errorf("no runtime status: %+v", got)
	}
}

func TestShouldRestartAndBackoff(t *testing.T) {
	if shouldRestart(corev1.RestartPolicyNever, 1) || shouldRestart(corev1.RestartPolicyOnFailure, 0) ||
		!shouldRestart(corev1.RestartPolicyOnFailure, 2) || !shouldRestart("", 0) {
		t.Error("restart policy decisions wrong")
	}
	if backoff(0) != 10*time.Second || backoff(2) != 40*time.Second || backoff(10) != 5*time.Minute {
		t.Errorf("backoff: %s %s %s", backoff(0), backoff(2), backoff(10))
	}
}

func TestCRILog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.log")
	data := "2026-11-06T00:00:01.000000000Z stdout F one\n" +
		"2026-11-06T00:00:02.000000000Z stderr P two-\n" +
		"2026-11-06T00:00:02.500000000Z stderr F part\n" +
		"2026-11-06T00:00:03.000000000Z stdout F three\n"
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	read := func(opts api.ContainerLogOpts) string {
		f, _ := os.Open(path)
		defer f.Close()
		var buf bytes.Buffer
		if err := copyCRILog(context.Background(), f, &buf, opts, false, func() bool { return false }); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	for _, tc := range []struct {
		name string
		opts api.ContainerLogOpts
		want string
	}{
		{"all", api.ContainerLogOpts{}, "one\ntwo-part\nthree\n"},
		{"tail 2 keeps distinct lines", api.ContainerLogOpts{Tail: 2}, "part\nthree\n"},
		{"timestamps", api.ContainerLogOpts{Tail: 1, Timestamps: true}, "2026-11-06T00:00:03Z three\n"},
		{"since", api.ContainerLogOpts{SinceTime: time.Date(2026, 11, 6, 0, 0, 2, 600e6, time.UTC)}, "three\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := read(tc.opts); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
	if p, err := previousLog("/d/c/3.log"); err != nil || p != "/d/c/2.log" {
		t.Errorf("previousLog: %q %v", p, err)
	}
	if _, err := previousLog("/d/c/0.log"); err == nil {
		t.Error("previousLog of first instance should fail")
	}
}

func gpuPod(uid, mem string) *corev1.Pod {
	c := corev1.Container{Name: "c", Image: "localhost:5050/x:1",
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{mlxv1alpha1.ResourceGPU: resource.MustParse("1")}}}
	if mem != "" {
		c.Resources.Limits[corev1.ResourceMemory] = resource.MustParse(mem)
	}
	pod := testPod(corev1.RestartPolicyNever, c)
	pod.UID = types.UID(uid)
	return pod
}

func TestAdmitGPU(t *testing.T) {
	p := &Provider{cfg: Config{Root: "/r"}, pods: map[string]*tracked{}}
	admit := func(pod *corev1.Pod) error {
		box, err := p.podBox(pod)
		if err != nil {
			t.Fatal(err)
		}
		if err := p.admitGPU(pod, box); err != nil {
			return err
		}
		p.pods[string(pod.UID)] = &tracked{pod: pod}
		return nil
	}

	if err := admit(gpuPod("a", "1Gi")); !errdefs.IsInvalidInput(err) {
		t.Fatalf("GPU pod on a node without a GPU: %v", err)
	}
	if err := admit(testPod("", corev1.Container{Name: "c", Image: "x"})); err != nil {
		t.Fatalf("non-GPU pod refused: %v", err)
	}

	p.gpu = &runtimev1.GPUFacts{MetalAvailable: true, SandboxGpuSupported: true, RecommendedMaxWorkingSetBytes: 3 << 30}
	if err := admit(gpuPod("a", "")); !errdefs.IsInvalidInput(err) || !strings.Contains(err.Error(), "memory limit") {
		t.Fatalf("pod without a memory limit: %v", err)
	}
	if err := admit(gpuPod("a", "2Gi")); err != nil {
		t.Fatalf("first pod: %v", err)
	}
	if err := admit(gpuPod("b", "2Gi")); !errors.Is(err, gpu.ErrOverCeiling) || errdefs.IsInvalidInput(err) {
		t.Fatalf("second pod should be a retryable over-ceiling refusal: %v", err)
	}
	p.pods["a"].status = &runtimev1.PodStatus{ContainerStatuses: []*runtimev1.ContainerStatus{exited(0)}}
	if err := admit(gpuPod("b", "2Gi")); err != nil {
		t.Fatalf("finished pod still holds GPU memory: %v", err)
	}
}

func TestPodMemoryLimit(t *testing.T) {
	lim := func(q string) corev1.ResourceRequirements {
		return corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(q)}}
	}
	pod := testPod("", corev1.Container{Name: "a", Resources: lim("1Gi")}, corev1.Container{Name: "b", Resources: lim("512Mi")})
	if got := podMemoryLimit(pod); got != 1536<<20 {
		t.Errorf("sum: %d", got)
	}
	pod.Spec.InitContainers = []corev1.Container{{Name: "i", Resources: lim("2Gi")}}
	if got := podMemoryLimit(pod); got != 2<<30 {
		t.Errorf("init max: %d", got)
	}
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "u"})
	if got := podMemoryLimit(pod); got != 0 {
		t.Errorf("unbounded container: %d", got)
	}
}
