// Package gpu decides how this node advertises its Apple Silicon GPU and
// whether one more GPU pod fits. It is pure: runtimed's GPUFacts in, node
// fields and verdicts out.
//
// The arithmetic is ported from k3sm (pkg/mlx/capacity.go and
// cmd/k3sm/gpu.go, Apache-2.0, the k3sm Authors) rather than imported, because
// k3sm is not fetchable as a Go module. It must stay identical to k3sm's: the
// mlx.k3sm.io keys mean the same thing on a k3sm node and on a darwin-vk node,
// so workloads (and k3sm's MLX operator) can target either.
package gpu

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	mlxv1alpha1 "k3sm.io/apis/mlx/v1alpha1"
	runtimev1 "k3sm.io/apis/runtime/v1"
)

// ResourceName is the extended resource a node advertises and a pod requests.
const ResourceName = corev1.ResourceName(mlxv1alpha1.ResourceGPU)

// SlotFloorBytes is the GPU memory one slot is sized at (1.5 GiB).
const SlotFloorBytes int64 = 1536 << 20

// MaxSlots caps the slot count. Slots are co-tenants on one integrated GPU,
// not isolated devices, so past two the contention outweighs the concurrency.
const MaxSlots = 2

var (
	// ErrOverCeiling: the pod does not fit beside the GPU pods already admitted.
	// Deleting another GPU pod fixes it.
	ErrOverCeiling = errors.New("pod does not fit in the node's GPU memory ceiling beside the pods already admitted")
	// ErrUnbounded: the pod declares no memory limit, so it cannot be fit
	// against a known ceiling. Adding a limit fixes it.
	ErrUnbounded = errors.New("pod requests the GPU without a memory limit on every container, so it cannot be fit against the node's GPU memory ceiling")
)

// Advertisable reports whether the node may advertise a GPU: a usable Metal
// device AND a sandbox backend that can grant one. Either alone would attract
// GPU pods the sandbox then denies. Nil facts (unknown) fail closed.
func Advertisable(f *runtimev1.GPUFacts) bool {
	return f.GetMetalAvailable() && f.GetSandboxGpuSupported()
}

// CeilingBytes is the usable GPU memory ceiling: the tighter of Metal's
// recommended working set and the iogpu wired limit, ignoring zeros. 0 means
// unknown, never unlimited. It is never mem_bytes, which the rest of the Mac
// shares.
func CeilingBytes(f *runtimev1.GPUFacts) int64 {
	ws := clamp(f.GetRecommendedMaxWorkingSetBytes())
	wired := clamp(f.GetIogpuWiredLimitBytes())
	switch {
	case ws > 0 && wired > 0:
		return min(ws, wired)
	case ws > 0:
		return ws
	default:
		return wired
	}
}

// Slots is the advertised count: one per whole SlotFloorBytes of ceiling,
// between 1 and MaxSlots. An unknown ceiling advertises 1, since the fit check
// is then disabled and a second slot would be admitted against nothing.
func Slots(f *runtimev1.GPUFacts) int64 {
	ceiling := CeilingBytes(f)
	if ceiling == 0 {
		return 1
	}
	return min(MaxSlots, max(1, ceiling/SlotFloorBytes))
}

// Fits returns nil when want more bytes fit under ceiling beside admitted. An
// unknown ceiling admits everything; an unbounded pod (want 0) on a known
// ceiling is refused, or it would count as 0 for its whole life.
func Fits(ceiling, admitted, want int64) error {
	if ceiling <= 0 {
		return nil
	}
	if want <= 0 {
		return fmt.Errorf("%w: ceiling %s, %s already admitted", ErrUnbounded, human(ceiling), human(admitted))
	}
	if want <= ceiling-admitted {
		return nil
	}
	return fmt.Errorf("%w: want %s beside %s already admitted, ceiling %s",
		ErrOverCeiling, human(want), human(admitted), human(ceiling))
}

// Apply advertises the GPU on n (resource in Capacity and Allocatable, plus
// the presence, chip, chip-family and memory labels), or removes all of them
// when the GPU is not advertisable. Removal matters: a node that lost its GPU
// across a restart must stop attracting GPU pods.
func Apply(n *corev1.Node, f *runtimev1.GPUFacts) {
	present := Advertisable(f)
	if n.Labels == nil {
		n.Labels = map[string]string{}
	}
	label := func(key, value string) {
		if present && value != "" {
			n.Labels[key] = value
		} else {
			delete(n.Labels, key)
		}
	}
	label(mlxv1alpha1.LabelGPUPresent, "true")
	label(mlxv1alpha1.LabelChip, ChipSlug(f.GetChipBrand()))
	label(mlxv1alpha1.LabelChipFamily, ChipSlug(f.GetChipFamily()))
	label(mlxv1alpha1.LabelMemoryGB, memoryGiB(f.GetMemBytes()))

	if !present {
		delete(n.Status.Capacity, ResourceName)
		delete(n.Status.Allocatable, ResourceName)
		return
	}
	for _, list := range []*corev1.ResourceList{&n.Status.Capacity, &n.Status.Allocatable} {
		if *list == nil {
			*list = corev1.ResourceList{}
		}
		(*list)[ResourceName] = *resource.NewQuantity(Slots(f), resource.DecimalSI)
	}
}

// ChipSlug turns a raw chip fact ("Apple M4 Max") into the label value
// ("apple-m4-max") per the rule documented on the mlx.k3sm.io label keys.
func ChipSlug(raw string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(raw) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			if dash {
				b.WriteByte('-')
				dash = false
			}
			b.WriteRune(r)
		} else if b.Len() > 0 {
			dash = true
		}
	}
	s := b.String()
	if len(s) > 63 {
		s = strings.TrimRight(s[:63], "-")
	}
	return s
}

// memoryGiB is the whole-GiB label value, or "" (label removed) below 1 GiB.
func memoryGiB(b uint64) string {
	if gib := b >> 30; gib > 0 {
		return strconv.FormatUint(gib, 10)
	}
	return ""
}

func clamp(v uint64) int64 {
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

func human(b int64) string { return resource.NewQuantity(b, resource.BinarySI).String() }
