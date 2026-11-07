package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"

	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/guestartifacts"
	"k3sm.io/runtimed/pkg/runtime"
	"k3sm.io/runtimed/pkg/sandbox"

	"github.com/changtimwu/darwin-runner-vkube/internal/rtprovider"
)

// newRuntimedProvider starts the embedded runtimed and re-attaches the pods
// already bound to this node from a previous run.
func newRuntimedProvider(ctx context.Context, client kubernetes.Interface, o options, ip string) (*rtprovider.Provider, error) {
	uid, gid, err := podIdentity(o.podUser, o.rootPods)
	if err != nil {
		return nil, err
	}
	policy, ok := map[string]runtimev1.SignaturePolicy{
		"adhoc-ok":          runtimev1.SignaturePolicy_SIGNATURE_POLICY_ADHOC_OK,
		"require-signed":    runtimev1.SignaturePolicy_SIGNATURE_POLICY_REQUIRE_SIGNED,
		"require-notarized": runtimev1.SignaturePolicy_SIGNATURE_POLICY_REQUIRE_NOTARIZED,
	}[o.sigPolicy]
	if !ok {
		return nil, fmt.Errorf("unknown --signature-policy %q", o.sigPolicy)
	}
	bound, err := client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", o.nodeName).String(),
	})
	if err != nil {
		return nil, fmt.Errorf("list pods bound to %s: %w", o.nodeName, err)
	}
	existing := make([]*corev1.Pod, 0, len(bound.Items))
	for i := range bound.Items {
		existing = append(existing, &bound.Items[i])
	}
	dataDir, err := filepath.Abs(o.dataDir)
	if err != nil {
		return nil, err
	}
	root := filepath.Join(dataDir, "runtime")
	if err := traversablePodsDir(filepath.Join(root, "pods")); err != nil {
		return nil, err
	}
	return rtprovider.New(ctx, rtprovider.Config{
		NodeName:           o.nodeName,
		NodeIP:             ip,
		Root:               root,
		PodLogsDir:         filepath.Join(dataDir, "pod-logs"),
		PodUID:             uid,
		PodGID:             gid,
		SignaturePolicy:    policy,
		DisableGPU:         o.noGPU,
		Client:             client,
		InsecureRegistries: o.insecureRegs,
		PathShim:           pathShim(o.logger),
		ShadowBinDir:       o.shadowBin,
		Events:             eventRecorder(client, o.nodeName),
		VMBackend:          vmBackend(ctx, o, root),
		Logger:             o.logger,
	}, existing)
}

// pathShim is the path-rebase library installed beside the binary, which lets
// pods see their volumes at the mount paths they asked for. "" disables it.
// The sandbox never grants pods anything under /Users, so a build run from a
// home directory cannot use it.
func pathShim(log *slog.Logger) string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	p := filepath.Join(filepath.Dir(exe), "libk3sm_pathrebase_shim.dylib")
	if _, err := os.Stat(p); err != nil {
		log.Warn("path-rebase shim not found; volumes are written but not visible at their mount paths", "want", p)
		return ""
	}
	if strings.HasPrefix(p, "/Users/") {
		log.Warn("path-rebase shim is under /Users, which pods cannot read; volumes are written but not visible at their mount paths (install with deploy/install.sh)", "path", p)
		return ""
	}
	return p
}

// vmBackend fetches the pinned Linux guest kernel and initramfs and returns
// the micro-VM backend, or nil when vm pods cannot run here. Like k3sm, a
// failure only turns the vm RuntimeClass off; native pods are unaffected.
func vmBackend(ctx context.Context, o options, root string) runtime.VMBackend {
	if o.noVM {
		return nil
	}
	pin, err := guestartifacts.Lookup(guestartifacts.ActiveGuestKernel)
	if err != nil {
		o.logger.Warn("vm RuntimeClass off: no guest kernel pin", "err", err)
		return nil
	}
	ectx, cancel := context.WithTimeout(ctx, guestartifacts.DefaultFetchTimeout)
	defer cancel()
	art, err := guestartifacts.EnsureGuestArtifacts(ectx, filepath.Join(root, guestartifacts.GuestArtifactsSubdir), pin, &guestartifacts.HTTPFetcher{})
	if err != nil {
		o.logger.Warn("vm RuntimeClass off: guest kernel unavailable", "err", err)
		return nil
	}
	b := sandbox.NewVMBackend(
		sandbox.WithStateRoot(root),
		sandbox.WithLogger(o.logger.With("component", "vm")),
		sandbox.WithGuestArtifacts(guestartifacts.Locator(pin, art)))
	if !b.Available() {
		o.logger.Warn("vm RuntimeClass off: this host cannot run micro-VMs (needs macOS 26+ and k3sm-vmhost, signed with the virtualization entitlement, beside darwin-vk)")
		return nil
	}
	o.logger.Info("vm RuntimeClass available", "guestKernel", guestartifacts.ActiveGuestKernel)
	return b
}

// eventRecorder records pod events as this node, like the kubelet does.
func eventRecorder(client kubernetes.Interface, nodeName string) record.EventRecorder {
	b := record.NewBroadcaster()
	b.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: client.CoreV1().Events(metav1.NamespaceAll)})
	return b.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "kubelet", Host: nodeName})
}

// podIdentity picks the uid/gid pods drop to. Unprivileged, pods share the
// daemon's identity (dropping is impossible). As root, pods must not run as
// root by default, so a dedicated service user is required.
func podIdentity(name string, allowRoot bool) (uint32, uint32, error) {
	if os.Geteuid() != 0 {
		return 0, 0, nil
	}
	u, err := user.Lookup(name)
	if err != nil {
		if allowRoot {
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("running as root but pod user %q does not exist (create it with deploy/install.sh, or pass --allow-root-pods): %w", name, err)
	}
	uid, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid, _ := strconv.ParseUint(u.Gid, 10, 32)
	if uid == 0 && !allowRoot {
		return 0, 0, fmt.Errorf("pod user %q is root; pass --allow-root-pods to accept", name)
	}
	return uint32(uid), uint32(gid), nil
}

// traversablePodsDir makes runtimed's pods tree reachable by the dropped pod
// user. runtimed creates it implicitly with whichever pod comes first, and its
// vm path uses 0750, so a root daemon whose first pod was a vm pod locked every
// native pod out of its own rootfs. Each pod dir keeps its own mode, and the
// Seatbelt profile still denies a pod the other pods' dirs.
func traversablePodsDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if fi.Mode().Perm()&0o011 != 0o011 {
		return os.Chmod(dir, fi.Mode().Perm()|0o055)
	}
	return nil
}
