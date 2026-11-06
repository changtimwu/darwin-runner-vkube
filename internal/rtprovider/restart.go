package rtprovider

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// Kubelet CrashLoopBackOff schedule: 10s doubling to a 5m cap.
const (
	backoffBase = 10 * time.Second
	backoffMax  = 5 * time.Minute
)

// restartState tracks, per container, which termination was already acted on
// and which containers are waiting out a back-off. Guarded by Provider.mu.
type restartState struct {
	handled map[string]string
	pending map[string]bool
}

// maybeRestart applies the pod's restartPolicy to every terminated container.
// runtimed never restarts on exit; the provider is the single authority.
func (p *Provider) maybeRestart(t *tracked, st *runtimev1.PodStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	policy := t.pod.Spec.RestartPolicy
	for _, cs := range st.GetContainerStatuses() {
		term := cs.GetState().GetTerminated()
		if term == nil || !shouldRestart(policy, term.GetExitCode()) {
			continue
		}
		k := fmt.Sprintf("%d/%d", cs.GetRestartCount(), term.GetFinishedAt().AsTime().UnixNano())
		if t.restarts.handled == nil {
			t.restarts.handled, t.restarts.pending = map[string]string{}, map[string]bool{}
		}
		if t.restarts.handled[cs.GetName()] == k {
			continue
		}
		t.restarts.handled[cs.GetName()] = k
		t.restarts.pending[cs.GetName()] = true
		delay := backoff(cs.GetRestartCount())
		p.log.Info("container exited; restarting after back-off", "pod", key(t.pod), "container", cs.GetName(),
			"exitCode", term.GetExitCode(), "backoff", delay)
		go p.restartAfter(st.GetPodId(), cs.GetName(), delay)
	}
}

func (p *Provider) restartAfter(podID, container string, delay time.Duration) {
	time.Sleep(delay)
	p.mu.Lock()
	t, ok := p.pods[podID]
	if ok {
		delete(t.restarts.pending, container)
	}
	p.mu.Unlock()
	if !ok {
		return // deleted during the back-off
	}
	ctx := context.Background()
	_, err := p.rt.RestartContainer(ctx, &runtimev1.RestartContainerRequest{PodId: podID, Container: container, Reason: "container exited"})
	if err != nil {
		// A container that died while the daemon was down has no process to
		// restart; runtimed re-spawns it through StartContainer instead.
		if _, serr := p.rt.StartContainer(ctx, &runtimev1.StartContainerRequest{PodId: podID, Container: container}); serr != nil {
			p.log.Warn("restart container", "pod", podID, "container", container, "err", err, "startErr", serr)
		}
	}
	if r, err := p.rt.GetPodStatus(ctx, &runtimev1.GetPodStatusRequest{PodId: podID}); err == nil {
		p.observe(r.GetStatus())
	}
}

func shouldRestart(policy corev1.RestartPolicy, exitCode int32) bool {
	switch policy {
	case corev1.RestartPolicyNever:
		return false
	case corev1.RestartPolicyOnFailure:
		return exitCode != 0
	}
	return true // Always, or unset
}

func backoff(restarts int32) time.Duration {
	d := backoffBase
	for i := int32(0); i < restarts && d < backoffMax; i++ {
		d *= 2
	}
	return min(d, backoffMax)
}
