package gpu

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	mlxv1alpha1 "k3sm.io/apis/mlx/v1alpha1"
	runtimev1 "k3sm.io/apis/runtime/v1"
)

const gib = 1 << 30

func TestSlots(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ws, wired uint64
		ceiling   int64
		slots     int64
	}{
		{"unknown", 0, 0, 0, 1},
		{"tiny", gib, 0, gib, 1},
		{"working set only", 3 * gib, 0, 3 * gib, 2},
		{"wired is tighter", 100 * gib, 2 * gib, 2 * gib, 1},
		{"capped", 400 * gib, 0, 400 * gib, MaxSlots},
	} {
		f := &runtimev1.GPUFacts{RecommendedMaxWorkingSetBytes: tc.ws, IogpuWiredLimitBytes: tc.wired}
		if got := CeilingBytes(f); got != tc.ceiling {
			t.Errorf("%s: ceiling %d, want %d", tc.name, got, tc.ceiling)
		}
		if got := Slots(f); got != tc.slots {
			t.Errorf("%s: slots %d, want %d", tc.name, got, tc.slots)
		}
	}
}

func TestFits(t *testing.T) {
	if Fits(0, 0, 0) != nil {
		t.Error("unknown ceiling must admit")
	}
	if err := Fits(10*gib, 0, 0); !errors.Is(err, ErrUnbounded) {
		t.Errorf("unbounded pod: %v", err)
	}
	if Fits(10*gib, 4*gib, 6*gib) != nil {
		t.Error("exact fit refused")
	}
	if err := Fits(10*gib, 5*gib, 6*gib); !errors.Is(err, ErrOverCeiling) {
		t.Errorf("over ceiling: %v", err)
	}
}

func TestChipSlug(t *testing.T) {
	for raw, want := range map[string]string{
		"Apple M4 Max": "apple-m4-max",
		"  M2 (Pro) ":  "m2-pro",
		"M4":           "m4",
		"":             "",
	} {
		if got := ChipSlug(raw); got != want {
			t.Errorf("ChipSlug(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestApplyAndWithdraw(t *testing.T) {
	n := &corev1.Node{}
	n.Labels = map[string]string{"keep": "me"}
	n.Status.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}
	n.Status.Allocatable = n.Status.Capacity.DeepCopy()

	Apply(n, &runtimev1.GPUFacts{
		MetalAvailable: true, SandboxGpuSupported: true,
		ChipBrand: "Apple M4 Max", ChipFamily: "M4", MemBytes: 128 * gib,
		RecommendedMaxWorkingSetBytes: 100 * gib,
	})
	want := map[string]string{
		mlxv1alpha1.LabelGPUPresent: "true", mlxv1alpha1.LabelChip: "apple-m4-max",
		mlxv1alpha1.LabelChipFamily: "m4", mlxv1alpha1.LabelMemoryGB: "128",
	}
	for k, v := range want {
		if n.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, n.Labels[k], v)
		}
	}
	for _, list := range []corev1.ResourceList{n.Status.Capacity, n.Status.Allocatable} {
		if q := list[ResourceName]; q.Value() != 2 {
			t.Errorf("gpu resource %s, want 2", q.String())
		}
	}

	// Metal without a sandbox that can grant it is not a GPU node.
	Apply(n, &runtimev1.GPUFacts{MetalAvailable: true, ChipBrand: "Apple M4 Max", MemBytes: 128 * gib})
	for k := range want {
		if _, ok := n.Labels[k]; ok {
			t.Errorf("label %s survived withdrawal", k)
		}
	}
	if _, ok := n.Status.Allocatable[ResourceName]; ok {
		t.Error("gpu resource survived withdrawal")
	}
	if n.Labels["keep"] != "me" || n.Status.Capacity.Cpu().Value() != 8 {
		t.Error("withdrawal touched unrelated fields")
	}
	Apply(n, nil) // unknown facts fail closed without panicking
}
