package rtprovider

import (
	"context"
	"fmt"
	"slices"
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
	// Image pulls: attempts so far, containers with a retry scheduled, and
	// the container instance a "Pulled" event was last recorded for.
	pulls       map[string]int32
	pullPending map[string]bool
	pulledFor   map[string]string
}

// maybeRestart applies the pod's restartPolicy to every terminated container
// and retries every container that could not start. runtimed does neither on
// its own; the provider is the single authority.
func (p *Provider) maybeRestart(t *tracked, st *runtimev1.PodStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retryStarts(t, st)
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

// retryStarts schedules a StartContainer, on the kubelet's image back-off
// schedule, for each container waiting on a retryable failure, and records
// the kubelet's pull events. Callers hold p.mu.
func (p *Provider) retryStarts(t *tracked, st *runtimev1.PodStatus) {
	r := &t.restarts
	if r.pulls == nil {
		r.pulls, r.pullPending, r.pulledFor = map[string]int32{}, map[string]bool{}, map[string]string{}
	}
	for _, cs := range slices.Concat(st.GetInitContainerStatuses(), st.GetContainerStatuses()) {
		c := cs.GetName()
		w := cs.GetState().GetWaiting()
		if w == nil || !retryable(w.GetFailureReason()) {
			if w == nil {
				delete(r.pulls, c)
			}
			if pull := cs.GetImagePull(); pull != nil && cs.GetContainerId() != "" && r.pulledFor[c] != cs.GetContainerId() {
				r.pulledFor[c] = cs.GetContainerId()
				if pull.GetPulled() {
					p.event(t.pod, corev1.EventTypeNormal, "Pulled", "Successfully pulled image %q in %s", cs.GetImage(), pull.GetDuration().AsDuration().Round(time.Millisecond))
				} else {
					p.event(t.pod, corev1.EventTypeNormal, "Pulled", "Container image %q already present on machine", cs.GetImage())
				}
			}
			continue
		}
		if r.pullPending[c] {
			continue
		}
		delay := backoff(r.pulls[c])
		r.pulls[c]++
		r.pullPending[c] = true
		if w.GetFailureReason() == runtimev1.FailureReason_FAILURE_REASON_CONTAINER_CONFIG {
			p.event(t.pod, corev1.EventTypeWarning, "Failed", "Error: %s", w.GetMessage())
		} else {
			p.event(t.pod, corev1.EventTypeWarning, "Failed", "Failed to pull image %q: %s", cs.GetImage(), w.GetMessage())
			p.event(t.pod, corev1.EventTypeNormal, "BackOff", "Back-off pulling image %q", cs.GetImage())
		}
		p.log.Info("container could not start; retrying after back-off", "pod", key(t.pod), "container", c,
			"reason", w.GetFailureReason().String(), "message", w.GetMessage(), "backoff", delay)
		go p.startAfter(st.GetPodId(), c, delay)
	}
}

// startAfter retries a container that could not start. It stays pending
// until runtimed answers, so a status seen meanwhile schedules nothing.
func (p *Provider) startAfter(podID, container string, delay time.Duration) {
	time.Sleep(delay)
	p.mu.Lock()
	_, ok := p.pods[podID]
	p.mu.Unlock()
	if !ok {
		return // deleted during the back-off
	}
	ctx := context.Background()
	if _, err := p.rt.StartContainer(ctx, &runtimev1.StartContainerRequest{PodId: podID, Container: container}); err != nil {
		p.log.Warn("start container", "pod", podID, "container", container, "err", err)
	}
	p.mu.Lock()
	if t, ok := p.pods[podID]; ok {
		delete(t.restarts.pullPending, container)
	}
	p.mu.Unlock()
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
