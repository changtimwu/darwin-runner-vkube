package rtprovider

import (
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"

	"github.com/changtimwu/darwin-runner-vkube/internal/gpu"
)

// admitGPU refuses a GPU pod the node cannot grant, or one that does not fit
// in GPU memory beside the GPU pods already admitted. The scheduler only
// counts slots; memory is checked here. Callers hold p.mu.
func (p *Provider) admitGPU(pod *corev1.Pod, box *runtimev1.PodBox) error {
	if !box.GetSandboxProfile().GetAllowGpu() {
		return nil
	}
	if !gpu.Advertisable(p.gpu) {
		return errdefs.AsInvalidInput(fmt.Errorf("pod %s requests %s, but this node has no GPU it can grant", key(pod), gpu.ResourceName))
	}
	var admitted int64
	for id, t := range p.pods {
		if id == string(pod.UID) || t.final || !requestsGPU(t.pod) {
			continue
		}
		// Finished pods hold no GPU memory, as the scheduler also assumes.
		if ph := toPodStatus(t.pod, t.status, "", nil).Phase; ph == corev1.PodSucceeded || ph == corev1.PodFailed {
			continue
		}
		admitted += podMemoryLimit(t.pod)
	}
	err := gpu.Fits(gpu.CeilingBytes(p.gpu), admitted, box.GetMemoryLimitBytes())
	switch {
	case errors.Is(err, gpu.ErrUnbounded):
		return errdefs.AsInvalidInput(fmt.Errorf("pod %s: %w", key(pod), err))
	case err != nil:
		// Retried by virtual-kubelet; it succeeds once another GPU pod goes.
		return fmt.Errorf("pod %s: %w", key(pod), err)
	}
	return nil
}

// podMemoryLimit is the pod's memory limit in bytes, as the kubelet computes
// it: the sum over containers, or the largest init container if that is
// bigger. 0 (unlimited) when any container has no limit.
func podMemoryLimit(pod *corev1.Pod) int64 {
	var sum int64
	for _, c := range pod.Spec.Containers {
		q, ok := c.Resources.Limits[corev1.ResourceMemory]
		if !ok || q.IsZero() {
			return 0
		}
		sum += q.Value()
	}
	for _, c := range pod.Spec.InitContainers {
		if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			sum = max(sum, q.Value())
		}
	}
	return sum
}

// developerDir is the Xcode developer directory the k3sm.io/xcode-toolchain
// annotation grants read access to. runtimed only accepts a full Xcode
// ".../Developer" directory; a Command Line Tools root is readable anyway.
func developerDir(log *slog.Logger) string {
	out, err := exec.Command("/usr/bin/xcode-select", "-p").Output()
	dir := strings.TrimSpace(string(out))
	if err != nil || filepath.Base(dir) != "Developer" {
		log.Info("no Xcode developer directory; the xcode-toolchain annotation grants nothing", "dir", dir, "err", err)
		return ""
	}
	return dir
}
