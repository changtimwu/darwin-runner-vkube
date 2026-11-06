package main

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"

	runtimev1 "k3sm.io/apis/runtime/v1"

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
	return rtprovider.New(ctx, rtprovider.Config{
		NodeName:        o.nodeName,
		NodeIP:          ip,
		Root:            filepath.Join(dataDir, "runtime"),
		PodLogsDir:      filepath.Join(dataDir, "pod-logs"),
		PodUID:          uid,
		PodGID:          gid,
		SignaturePolicy: policy,
		Logger:          o.logger,
	}, existing)
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
