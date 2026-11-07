package rtprovider

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mlxv1alpha1 "k3sm.io/apis/mlx/v1alpha1"
	runtimev1 "k3sm.io/apis/runtime/v1"
)

// nativeImage is runtimed's sentinel for "command[0] is a host binary".
const nativeImage = "native"

// podBox translates a Pod into runtimed's PodBox. It must be deterministic:
// re-attach verifies the compiled sandbox profile against the original spawn.
func (p *Provider) podBox(pod *corev1.Pod) (*runtimev1.PodBox, error) {
	id := string(pod.UID)
	if id == "" {
		return nil, fmt.Errorf("pod %s has no UID", key(pod))
	}
	box := &runtimev1.PodBox{
		PodId:           id,
		Namespace:       pod.Namespace,
		Name:            pod.Name,
		Uid:             p.cfg.PodUID,
		Gid:             p.cfg.PodGID,
		PodIp:           p.cfg.NodeIP,
		PodIps:          []string{p.cfg.NodeIP},
		Labels:          pod.Labels,
		Annotations:     pod.Annotations,
		SignaturePolicy: p.cfg.SignaturePolicy,
		LogDirectory:    podLogDir(p.cfg.PodLogsDir, pod),
		Hostname:        pod.Name,
		SandboxProfile: &runtimev1.SandboxProfile{
			DataVolumePath: filepath.Join(p.cfg.Root, "pods", id, "rootfs"),
			// /bin/sh reads its selector symlink here to pick bash/dash/zsh.
			ExtraReadPaths: []string{"/private/var/select"},
			AllowNetwork:   true,
			AllowGpu:       requestsGPU(pod),
		},
		TerminationGracePeriodSeconds: 30,
		MemoryLimitBytes:              podMemoryLimit(pod),
		QosClass:                      qosClass(pod.Status.QOSClass),
	}
	backend, err := p.backendFor(pod)
	if err != nil {
		return nil, err
	}
	if backend == runtimev1.SandboxBackend_SANDBOX_BACKEND_VM {
		if requestsGPU(pod) {
			return nil, fmt.Errorf("vm pods cannot use the GPU: Virtualization.framework has no GPU passthrough for Linux guests")
		}
		// Root inside the guest is confined by the hypervisor, not the host
		// identity, so containers default to the image's own user.
		box.Uid, box.Gid = 0, 0
		box.SandboxProfile.Backend = backend
		box.SandboxProfile.VmVcpus, box.SandboxProfile.VmMemoryBytes = vmSizing(pod)
	}
	if p.cfg.PathShim != "" {
		// dyld loads the shim inside the sandbox, so the pod must read it.
		box.SandboxProfile.ExtraReadPaths = append(box.SandboxProfile.ExtraReadPaths, p.cfg.PathShim)
	}
	if _, ok := pod.Annotations[runtimev1.AnnotationXcodeToolchain]; ok {
		box.SandboxProfile.XcodeToolchainDir = p.developerDir
	}
	for _, s := range pod.Spec.ImagePullSecrets {
		box.ImagePullSecrets = append(box.ImagePullSecrets, &runtimev1.LocalObjectReference{Name: s.Name})
	}
	if g := pod.Spec.TerminationGracePeriodSeconds; g != nil {
		box.TerminationGracePeriodSeconds = *g
	}
	if sc := pod.Spec.SecurityContext; sc != nil {
		box.PodSecurityContext = &runtimev1.PodSecurityContext{
			RunAsUser:  deref(sc.RunAsUser),
			RunAsGroup: deref(sc.RunAsGroup),
			FsGroup:    deref(sc.FSGroup),
		}
	}
	if box.Gid != 0 && box.GetPodSecurityContext().GetFsGroup() == 0 {
		// runtimed leaves the pod's tree root-owned, so a dropped pod user
		// could not write its volumes. The kubelet makes emptyDir world
		// writable; granting the pod user's group via fsGroup is the narrower
		// equivalent. vm pods have Gid 0, as they refuse fsGroup.
		if box.PodSecurityContext == nil {
			box.PodSecurityContext = &runtimev1.PodSecurityContext{}
		}
		box.PodSecurityContext.FsGroup = int64(box.Gid)
	}
	if box.Volumes, err = toVolumes(pod.Spec.Volumes); err != nil {
		return nil, err
	}
	if box.InitContainers, err = toContainers(pod.Spec.InitContainers); err != nil {
		return nil, err
	}
	if box.Containers, err = toContainers(pod.Spec.Containers); err != nil {
		return nil, err
	}
	return box, nil
}

func toContainers(cs []corev1.Container) ([]*runtimev1.Container, error) {
	out := make([]*runtimev1.Container, 0, len(cs))
	for _, c := range cs {
		rc := &runtimev1.Container{
			Name:            c.Name,
			Image:           c.Image,
			Command:         c.Command,
			Args:            c.Args,
			WorkingDir:      c.WorkingDir,
			Tty:             c.TTY,
			Stdin:           c.Stdin,
			ImagePullPolicy: pullPolicy(c.ImagePullPolicy),
			VolumeMounts:    toVolumeMounts(c.VolumeMounts),
		}
		// "host" was darwin-vk v0's placeholder; both mean "run a host binary".
		if rc.Image == "" || rc.Image == "host" {
			rc.Image = nativeImage
		}
		if rc.Image == nativeImage {
			if len(rc.Command) == 0 {
				return nil, fmt.Errorf("container %s: image %q needs a command", c.Name, nativeImage)
			}
			if !filepath.IsAbs(rc.Command[0]) {
				path, err := exec.LookPath(rc.Command[0])
				if err != nil {
					return nil, fmt.Errorf("container %s: %w", c.Name, err)
				}
				rc.Command = append([]string{path}, rc.Command[1:]...)
			}
		}
		for _, e := range c.Env {
			// virtual-kubelet has already resolved valueFrom into value.
			rc.Env = append(rc.Env, &runtimev1.EnvVar{Name: e.Name, Value: e.Value})
		}
		for _, port := range c.Ports {
			rc.Ports = append(rc.Ports, &runtimev1.ContainerPort{Name: port.Name, ContainerPort: port.ContainerPort, Protocol: string(port.Protocol)})
		}
		if sc := c.SecurityContext; sc != nil {
			rc.SecurityContext = &runtimev1.SecurityContext{RunAsUser: deref(sc.RunAsUser), RunAsGroup: deref(sc.RunAsGroup)}
		}
		out = append(out, rc)
	}
	return out, nil
}

func qosClass(q corev1.PodQOSClass) runtimev1.QOSClass {
	switch q {
	case corev1.PodQOSGuaranteed:
		return runtimev1.QOSClass_QOS_CLASS_GUARANTEED
	case corev1.PodQOSBurstable:
		return runtimev1.QOSClass_QOS_CLASS_BURSTABLE
	case corev1.PodQOSBestEffort:
		return runtimev1.QOSClass_QOS_CLASS_BEST_EFFORT
	}
	return runtimev1.QOSClass_QOS_CLASS_UNSPECIFIED
}

func pullPolicy(p corev1.PullPolicy) runtimev1.ImagePullPolicy {
	switch p {
	case corev1.PullAlways:
		return runtimev1.ImagePullPolicy_IMAGE_PULL_POLICY_ALWAYS
	case corev1.PullNever:
		return runtimev1.ImagePullPolicy_IMAGE_PULL_POLICY_NEVER
	case corev1.PullIfNotPresent:
		return runtimev1.ImagePullPolicy_IMAGE_PULL_POLICY_IF_NOT_PRESENT
	}
	return runtimev1.ImagePullPolicy_IMAGE_PULL_POLICY_UNSPECIFIED
}

// requestsGPU reports whether any container asks for the node's Metal GPU.
func requestsGPU(pod *corev1.Pod) bool {
	for _, c := range pod.Spec.Containers {
		if q, ok := c.Resources.Limits[mlxv1alpha1.ResourceGPU]; ok && !q.IsZero() {
			return true
		}
	}
	return false
}

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// toPodStatus builds the corev1 status from runtimed's. The phase is derived
// here rather than taken from runtimed, because the provider (not runtimed)
// owns restartPolicy: a container that exited under Always is still Running.
func toPodStatus(pod *corev1.Pod, st *runtimev1.PodStatus, nodeIP string, ov overrides) corev1.PodStatus {
	out := corev1.PodStatus{
		Phase:    corev1.PodPending,
		HostIP:   nodeIP,
		HostIPs:  []corev1.HostIP{{IP: nodeIP}},
		PodIP:    nodeIP,
		PodIPs:   []corev1.PodIP{{IP: nodeIP}},
		QOSClass: pod.Status.QOSClass, // computed by the API server at creation
		Reason:   st.GetReason(),
		Message:  st.GetMessage(),
	}
	if t := st.GetStartTime(); t != nil {
		out.StartTime = &metav1.Time{Time: t.AsTime()}
	}
	out.InitContainerStatuses = toContainerStatuses(pod.Spec.InitContainers, st.GetInitContainerStatuses(), overrides{pulling: ov.pulling})
	out.ContainerStatuses = toContainerStatuses(pod.Spec.Containers, st.GetContainerStatuses(), ov)

	initDone := true
	for _, cs := range out.InitContainerStatuses {
		t := cs.State.Terminated
		if t == nil || t.ExitCode != 0 {
			initDone = false
			if t != nil && pod.Spec.RestartPolicy == corev1.RestartPolicyNever {
				out.Phase = corev1.PodFailed
			}
		}
	}

	var running, succeeded, failed, ready int
	for _, cs := range out.ContainerStatuses {
		switch {
		case cs.State.Running != nil:
			running++
		case cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0:
			succeeded++
		case cs.State.Terminated != nil:
			failed++
		}
		if cs.Ready {
			ready++
		}
	}
	n := len(pod.Spec.Containers)
	policy := pod.Spec.RestartPolicy
	switch {
	case st == nil || out.Phase == corev1.PodFailed:
	case st.GetPhase() == runtimev1.PodPhase_POD_PHASE_FAILED && len(st.GetContainerStatuses()) == 0:
		out.Phase = corev1.PodFailed // runtimed could not create the pod at all
	case !initDone:
		out.Phase = corev1.PodPending
	case succeeded == n && policy != corev1.RestartPolicyAlways && policy != "":
		out.Phase = corev1.PodSucceeded
	case succeeded+failed == n && failed > 0 && policy == corev1.RestartPolicyNever:
		out.Phase = corev1.PodFailed
	case running > 0 || succeeded+failed > 0 || len(ov.backoff) > 0:
		out.Phase = corev1.PodRunning
	}

	allReady := n > 0 && ready == n
	out.Conditions = []corev1.PodCondition{
		condition(corev1.PodScheduled, true, out.StartTime),
		condition(corev1.PodInitialized, initDone, out.StartTime),
		condition(corev1.ContainersReady, allReady, out.StartTime),
		condition(corev1.PodReady, allReady, out.StartTime),
	}
	return out
}

func condition(t corev1.PodConditionType, ok bool, since *metav1.Time) corev1.PodCondition {
	c := corev1.PodCondition{Type: t, Status: corev1.ConditionFalse}
	if ok {
		c.Status = corev1.ConditionTrue
	}
	if since != nil {
		c.LastTransitionTime = *since
	}
	return c
}

// overrides is what the provider knows about containers that runtimed does
// not: which are in CrashLoopBackOff, which wait to retry a pull, and what
// startup and readiness probes decided.
type overrides struct {
	backoff, pulling map[string]bool
	// probed holds containers whose probes decide Started and Ready.
	probed map[string]probeView
}

type probeView struct{ started, ready bool }

func toContainerStatuses(specs []corev1.Container, sts []*runtimev1.ContainerStatus, ov overrides) []corev1.ContainerStatus {
	byName := map[string]*runtimev1.ContainerStatus{}
	for _, s := range sts {
		byName[s.GetName()] = s
	}
	out := make([]corev1.ContainerStatus, 0, len(specs))
	for _, c := range specs {
		s, ok := byName[c.Name]
		if !ok {
			out = append(out, corev1.ContainerStatus{
				Name: c.Name, Image: c.Image,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
			})
			continue
		}
		started := s.GetStarted()
		cs := corev1.ContainerStatus{
			Name:                 c.Name,
			Image:                c.Image,
			ImageID:              s.GetImageId(),
			ContainerID:          containerID(s.GetContainerId()),
			State:                toState(s.GetState()),
			LastTerminationState: toState(s.GetLastTerminationState()),
			Ready:                s.GetReady(),
			RestartCount:         s.GetRestartCount(),
			Started:              &started,
		}
		if w := s.GetState().GetWaiting(); w != nil && w.GetFailureReason() != runtimev1.FailureReason_FAILURE_REASON_UNSPECIFIED {
			cs.State.Waiting.Reason = waitingReason(w.GetFailureReason(), ov.pulling[c.Name])
		}
		if pv, ok := ov.probed[c.Name]; ok && cs.State.Running != nil {
			cs.Started = &pv.started
			cs.Ready = pv.started && pv.ready
		}
		if ov.backoff[c.Name] && cs.State.Terminated != nil {
			cs.LastTerminationState = cs.State
			cs.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
				Reason:  "CrashLoopBackOff",
				Message: fmt.Sprintf("back-off restarting container %s", c.Name),
			}}
			cs.Ready = false
		}
		out = append(out, cs)
	}
	return out
}

// waitingReason names a container that could not start in the kubelet's
// words. runtimed leaves the reason empty for failures and types the cause,
// because only the provider knows whether a retry is pending.
func waitingReason(fr runtimev1.FailureReason, retrying bool) string {
	switch fr {
	case runtimev1.FailureReason_FAILURE_REASON_INVALID_IMAGE_NAME:
		return "InvalidImageName"
	case runtimev1.FailureReason_FAILURE_REASON_IMAGE_NEVER_PULL:
		return "ErrImageNeverPull"
	case runtimev1.FailureReason_FAILURE_REASON_CONTAINER_CONFIG:
		return "CreateContainerConfigError"
	}
	if retrying {
		return "ImagePullBackOff"
	}
	return "ErrImagePull"
}

// retryable reports whether starting the container again can succeed: a bad
// image name cannot, anything else (registry down, Secret fixed later, image
// pushed or loaded later) can.
func retryable(fr runtimev1.FailureReason) bool {
	return fr != runtimev1.FailureReason_FAILURE_REASON_UNSPECIFIED &&
		fr != runtimev1.FailureReason_FAILURE_REASON_INVALID_IMAGE_NAME
}

func toState(s *runtimev1.ContainerState) corev1.ContainerState {
	switch {
	case s.GetTerminated() != nil:
		t := s.GetTerminated()
		return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: t.GetExitCode(), Signal: t.GetSignal(), Reason: t.GetReason(), Message: t.GetMessage(),
			StartedAt: ts(t.GetStartedAt()), FinishedAt: ts(t.GetFinishedAt()), ContainerID: containerID(t.GetContainerId()),
		}}
	case s.GetRunning() != nil:
		return corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: ts(s.GetRunning().GetStartedAt())}}
	case s.GetWaiting() != nil:
		return corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: s.GetWaiting().GetReason(), Message: s.GetWaiting().GetMessage()}}
	}
	return corev1.ContainerState{}
}

func ts(t *timestamppb.Timestamp) metav1.Time {
	if t == nil {
		return metav1.Time{}
	}
	return metav1.Time{Time: t.AsTime()}
}

func containerID(id string) string {
	if id == "" || strings.Contains(id, "://") {
		return id
	}
	return "runtimed://" + id
}
