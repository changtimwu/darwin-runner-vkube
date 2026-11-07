package rtprovider

import (
	"context"
	"maps"
	"net/netip"
	"slices"

	corev1 "k8s.io/api/core/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/sandbox"
)

// dyldInsertAnnotation tells runtimed which DYLD library to load into the
// pod's processes, after the path-rebase shim.
const dyldInsertAnnotation = "k3sm.io/dyld-insert-libraries"

// podNetwork is what runtimed sees: the pod network, with the node-IP guest
// network for vm pods it gave no IP.
type podNetwork struct {
	PodNetwork
	guest guestNetwork
}

func (n podNetwork) GuestNetwork(podID string) (sandbox.GuestNetworkConfig, bool) {
	if cfg, ok := n.PodNetwork.GuestNetwork(podID); ok {
		return cfg, true
	}
	return n.guest.GuestNetwork(podID)
}

// allocateIP returns the IP a new pod gets: its own from the pod network, or
// the node IP for hostNetwork pods and when there is no pod network.
func (p *Provider) allocateIP(ctx context.Context, pod *corev1.Pod) (string, error) {
	if p.cfg.Network == nil {
		return p.cfg.NodeIP, nil
	}
	if p.isVMPod(pod) {
		return p.cfg.Network.SetupGuest(ctx, string(pod.UID), pod.Namespace)
	}
	if pod.Spec.HostNetwork {
		p.cfg.Network.SetHostNetwork(string(pod.UID))
	}
	return p.cfg.Network.Setup(ctx, string(pod.UID))
}

// reattachIP re-claims the IP a running pod was published with.
func (p *Provider) reattachIP(ctx context.Context, pod *corev1.Pod) (string, error) {
	if p.cfg.Network == nil {
		return p.cfg.NodeIP, nil
	}
	vm := p.isVMPod(pod)
	if pod.Spec.HostNetwork && !vm {
		p.cfg.Network.SetHostNetwork(string(pod.UID))
		return p.cfg.NodeIP, nil
	}
	ip, err := netip.ParseAddr(pod.Status.PodIP)
	if err != nil {
		return p.cfg.NodeIP, nil
	}
	ok, err := p.cfg.Network.Reattach(ctx, string(pod.UID), ip, vm, pod.Namespace)
	if err != nil || !ok {
		return p.cfg.NodeIP, err
	}
	return ip.String(), nil
}

// isVMPod reports whether the pod runs in a micro-VM, from the RuntimeClass
// handler alone (an unresolvable class fails later, in podBox).
func (p *Provider) isVMPod(pod *corev1.Pod) bool {
	b, err := p.backendFor(pod)
	return err == nil && b == runtimev1.SandboxBackend_SANDBOX_BACKEND_VM
}

// addNetworkEnv gives a native pod with its own IP the bind shim's and the DNS
// shim's environment, and loads the DNS shim. Appended after the pod's own
// env, so a container cannot override them.
func (p *Provider) addNetworkEnv(box *runtimev1.PodBox, pod *corev1.Pod, podIP string) {
	ip, err := netip.ParseAddr(podIP)
	if p.cfg.Network == nil || err != nil || podIP == p.cfg.NodeIP {
		return
	}
	env := p.cfg.Network.Env(ip, pod.Namespace)
	keys := slices.Sorted(maps.Keys(env))
	for _, c := range append(slices.Clone(box.InitContainers), box.Containers...) {
		for _, k := range keys {
			c.Env = append(c.Env, &runtimev1.EnvVar{Name: k, Value: env[k]})
		}
	}
	if p.cfg.DNSShim != "" {
		box.Annotations = maps.Clone(box.Annotations)
		if box.Annotations == nil {
			box.Annotations = map[string]string{}
		}
		box.Annotations[dyldInsertAnnotation] = p.cfg.DNSShim
		box.SandboxProfile.ExtraReadPaths = append(box.SandboxProfile.ExtraReadPaths, p.cfg.DNSShim)
	}
}
