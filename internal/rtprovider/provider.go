// Package rtprovider is a virtual-kubelet provider backed by k3sm's runtimed,
// embedded in-process. Pods run as native macOS processes confined by Seatbelt,
// from OCI images (darwin/arm64) or host binaries (image "native").
//
// runtimed is driven two ways: unary calls go straight to the *runtime.Runtime,
// and streaming RPCs (status watch, exec, port-forward) go through a gRPC client
// over an in-memory listener, so this package never fakes a gRPC stream.
package rtprovider

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"

	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/runtime"
)

// Config configures a Provider.
type Config struct {
	NodeName string
	NodeIP   string
	// Root is runtimed's work dir (image cache + pod dirs). Absolute and clean.
	Root string
	// PodLogsDir is the container log tree root (the kubelet's /var/log/pods).
	PodLogsDir string
	// PodUID/PodGID is the identity pods without a securityContext drop to. Zero
	// keeps the daemon's identity, which is only acceptable when not root.
	PodUID, PodGID uint32
	// SignaturePolicy is enforced on image binaries before exec.
	SignaturePolicy runtimev1.SignaturePolicy
	// DisableGPU withholds the GPU: the node advertises none and GPU pods are
	// refused, even when the host has one.
	DisableGPU bool
	// Client reads imagePullSecrets. Nil pulls anonymously.
	Client kubernetes.Interface
	// InsecureRegistries are registry host[:port]s pulled over plain HTTP.
	// Loopback and RFC 1918 addresses already are.
	InsecureRegistries []string
	// PathShim is the path-rebase DYLD library that makes volume mounts appear
	// at their mount paths, and ShadowBinDir the root-owned re-signed host
	// shells it can load into. Empty disables each.
	PathShim, ShadowBinDir string
	// Events records pull events on pods. Nil records none.
	Events record.EventRecorder
	Logger *slog.Logger
}

// Provider implements nodeutil.Provider and node.PodNotifier on top of runtimed.
//
// Locking: mu guards pods. The notify callback runs outside mu.
type Provider struct {
	cfg    Config
	log    *slog.Logger
	rt     *runtime.Runtime
	client runtimev1.RuntimeClient
	conn   *grpc.ClientConn
	stop   context.CancelFunc
	served chan struct{}

	// Fixed at startup, like runtimed's own probe: podBox must be
	// deterministic for re-attach, and the node advertises what admission
	// enforces.
	gpu          *runtimev1.GPUFacts // nil when unknown or disabled
	developerDir string              // granted by the xcode-toolchain annotation; "" grants nothing

	mu     sync.Mutex
	pods   map[string]*tracked // keyed by pod UID (the runtimed pod_id)
	notify func(*corev1.Pod)
}

// tracked is what the provider keeps per pod that runtimed does not.
type tracked struct {
	pod      *corev1.Pod
	status   *runtimev1.PodStatus
	restarts restartState
	// final marks a pod that finished before a daemon restart: runtimed no
	// longer knows it, so its last API status stands and only logs are served.
	final bool
	// probes holds the probe and hook state of each running container.
	probes map[string]*probeSet
}

// New starts the embedded runtime, re-attaches pods that survived a previous
// run (existing lists the Pods bound to this node), then starts serving. The
// order matters: runtimed's startup reap kills every process group that no
// attached pod owns.
func New(ctx context.Context, cfg Config, existing []*corev1.Pod) (*Provider, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	for _, d := range []string{cfg.Root, cfg.PodLogsDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	p := &Provider{cfg: cfg, log: cfg.Logger, pods: map[string]*tracked{}, served: make(chan struct{})}
	rt, err := runtime.New(runtime.Config{
		Root:           cfg.Root,
		PodLogsDir:     cfg.PodLogsDir,
		RuntimeVersion: "darwin-vk",
		PathShimPath:   cfg.PathShim,
		ShadowBinDir:   cfg.ShadowBinDir,
		Logger:         cfg.Logger.With("component", "runtimed"),
	}, p.deps())
	if err != nil {
		return nil, fmt.Errorf("start runtimed: %w", err)
	}
	p.rt = rt
	if info, err := rt.GetRuntimeInfo(ctx, &runtimev1.GetRuntimeInfoRequest{}); err == nil && !cfg.DisableGPU {
		p.gpu = info.GetGpu()
	}
	p.developerDir = developerDir(p.log)

	p.reattach(ctx, existing)
	p.pruneLogDirs(existing)

	lis := bufconn.Listen(1 << 20)
	srvCtx, stop := context.WithCancel(context.Background())
	p.stop = stop
	go func() {
		defer close(p.served)
		if err := runtime.NewServer(rt).Serve(srvCtx, lis); err != nil {
			p.log.Error("runtimed server stopped", "err", err)
		}
	}()
	p.conn, err = grpc.NewClient("passthrough:///runtimed",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.Close()
		return nil, err
	}
	p.client = runtimev1.NewRuntimeClient(p.conn)
	return p, nil
}

func (p *Provider) deps() runtime.Deps {
	d := runtime.Deps{ClusterRegistries: p.cfg.InsecureRegistries}
	if p.cfg.Client != nil {
		d.Credentials = secretCredentials{client: p.cfg.Client, log: p.log}
		d.Resolver = kubeResolver{client: p.cfg.Client}
	}
	return d
}

// event records a pod event when a recorder is configured.
func (p *Provider) event(pod *corev1.Pod, eventType, reason, format string, args ...any) {
	if p.cfg.Events != nil {
		p.cfg.Events.Eventf(pod, eventType, reason, format, args...)
	}
}

// Close stops serving and releases the runtime. Pod processes keep running so
// a restarted daemon can re-attach them.
func (p *Provider) Close() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	if p.stop != nil {
		p.stop()
		<-p.served
	}
	if err := p.rt.Close(); err != nil {
		p.log.Warn("runtime close", "err", err)
	}
}

// GPUFacts is what the node advertises and admission enforces: runtimed's
// probe at startup, or nil when the GPU is unknown or disabled.
func (p *Provider) GPUFacts() *runtimev1.GPUFacts { return p.gpu }

// reattach adopts the still-running processes of pods bound to this node.
// Anything runtimed refuses is left for virtual-kubelet to create again.
func (p *Provider) reattach(ctx context.Context, pods []*corev1.Pod) {
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			p.pods[string(pod.UID)] = &tracked{pod: pod.DeepCopy(), final: true}
			continue
		}
		if pod.DeletionTimestamp != nil {
			continue
		}
		box, err := p.podBox(pod)
		if err != nil {
			p.log.Warn("re-attach: translate", "pod", key(pod), "err", err)
			continue
		}
		st, err := p.rt.AttachPod(withPod(ctx, pod), box)
		if err != nil {
			p.log.Info("re-attach refused; pod will be recreated", "pod", key(pod), "err", err)
			continue
		}
		t := &tracked{pod: pod.DeepCopy(), status: st}
		p.pods[box.GetPodId()] = t
		p.log.Info("re-attached pod", "pod", key(pod))
	}
}

// pruneLogDirs removes log trees of pods no longer bound to this node, such
// as pods deleted while the daemon was down.
func (p *Provider) pruneLogDirs(existing []*corev1.Pod) {
	keep := map[string]bool{}
	for _, pod := range existing {
		keep[filepath.Base(podLogDir(p.cfg.PodLogsDir, pod))] = true
	}
	entries, _ := os.ReadDir(p.cfg.PodLogsDir)
	for _, e := range entries {
		if e.IsDir() && !keep[e.Name()] {
			_ = os.RemoveAll(filepath.Join(p.cfg.PodLogsDir, e.Name()))
		}
	}
}

// CreatePod materializes the pod's log tree and asks runtimed to start it.
func (p *Provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	t := &tracked{pod: pod.DeepCopy()}
	box, err := p.podBox(pod)
	if err != nil {
		p.reject(t, err)
		return nil
	}
	if err := ensureLogDirs(box.GetLogDirectory(), pod); err != nil {
		return err
	}
	p.mu.Lock()
	if err := p.admitGPU(pod, box); err != nil {
		p.mu.Unlock()
		if !errdefs.IsInvalidInput(err) {
			return err // retried until it fits
		}
		p.reject(t, err)
		return nil
	}
	p.pods[box.GetPodId()] = t
	p.mu.Unlock()

	resp, err := p.rt.CreatePod(withPod(ctx, pod), &runtimev1.CreatePodRequest{Pod: box})
	if err == nil && resp.GetError() != nil {
		err = errors.New(resp.GetError().GetMessage())
	}
	if err != nil {
		p.mu.Lock()
		delete(p.pods, box.GetPodId())
		p.mu.Unlock()
		if resp.GetFailureReason() == runtimev1.FailureReason_FAILURE_REASON_INVALID_POD_BOX {
			p.reject(t, err)
			return nil
		}
		// Like the kubelet, keep the pod Pending and let virtual-kubelet
		// retry: a missing ConfigMap or Secret may appear later.
		reason := "FailedCreatePodSandBox"
		if resp.GetFailureReason() == runtimev1.FailureReason_FAILURE_REASON_ROOTFS_SETUP {
			reason = "FailedMount"
		}
		p.event(pod, corev1.EventTypeWarning, reason, "%s", err)
		return fmt.Errorf("create pod %s: %w", key(pod), err)
	}
	p.observe(resp.GetStatus())
	return nil
}

// reject fails a pod this node can never run, as the kubelet does at
// admission: Failed releases the scheduler's resources, where a Pending retry
// would hold them forever.
func (p *Provider) reject(t *tracked, err error) {
	t.final = true
	t.pod.Status = corev1.PodStatus{Phase: corev1.PodFailed, Reason: "AdmissionRejected", Message: err.Error()}
	p.mu.Lock()
	p.pods[string(t.pod.UID)] = t
	p.mu.Unlock()
	p.log.Info("pod rejected", "pod", key(t.pod), "err", err)
	p.event(t.pod, corev1.EventTypeWarning, "AdmissionRejected", "%s", err)
	p.emit(t.pod.DeepCopy())
}

// UpdatePod records metadata changes; runtimed refuses spec changes as k8s does.
func (p *Provider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.pods[string(pod.UID)]; ok {
		t.pod = pod.DeepCopy()
	}
	return nil
}

// DeletePod stops the pod's processes and removes its log tree.
func (p *Provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	id := string(pod.UID)
	p.mu.Lock()
	t, ok := p.pods[id]
	delete(p.pods, id)
	var running []corev1.Container
	if ok {
		stopProbes(t)
		for _, c := range t.pod.Spec.Containers {
			for _, cs := range t.status.GetContainerStatuses() {
				if cs.GetName() == c.Name && cs.GetState().GetRunning() != nil {
					running = append(running, c)
				}
			}
		}
	}
	p.mu.Unlock()
	if !ok {
		// Not tracked, e.g. not re-attached after an upgrade. virtual-kubelet
		// only removes the pod once it sees a terminated status, so report one
		// rather than NotFound, which would leave it Terminating forever.
		t = &tracked{pod: pod.DeepCopy()}
	}
	var grace int64
	if g := pod.DeletionGracePeriodSeconds; g != nil {
		grace = *g
	} else if g := pod.Spec.TerminationGracePeriodSeconds; g != nil {
		grace = *g
	}
	// preStop hooks run in parallel and share the grace period with the
	// SIGTERM that follows, as on the kubelet.
	start := time.Now()
	var wg sync.WaitGroup
	for _, c := range running {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.preStop(ctx, t.pod, c, grace)
		}()
	}
	wg.Wait()
	if len(running) > 0 && grace > 0 {
		grace = max(grace-int64(time.Since(start).Seconds()), 1)
	}
	if _, err := p.rt.DeletePod(ctx, &runtimev1.DeletePodRequest{PodId: id, GracePeriodSeconds: grace}); err != nil {
		p.log.Warn("runtimed delete pod", "pod", key(pod), "err", err)
	}
	_ = os.RemoveAll(podLogDir(p.cfg.PodLogsDir, pod))

	// virtual-kubelet expects a final terminated status for a deleted pod.
	gone := p.toPod(t)
	gone.Status.Phase = corev1.PodSucceeded
	for i := range gone.Status.ContainerStatuses {
		cs := &gone.Status.ContainerStatuses[i]
		if cs.State.Terminated == nil {
			cs.State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				Reason: "Completed", Message: "pod deleted", FinishedAt: metav1.Now(),
			}}
		}
		cs.Ready = false
	}
	p.emit(gone)
	go p.finishDelete(pod)
	return nil
}

// finishDelete removes the pod object once its containers are gone, as the
// kubelet does. virtual-kubelet would otherwise wait out the whole grace
// period: it ignores status-only updates, so it never notices the pod
// stopped. The UID precondition spares a recreated pod with the same name.
func (p *Provider) finishDelete(pod *corev1.Pod) {
	if p.cfg.Client == nil {
		return
	}
	time.Sleep(time.Second) // let virtual-kubelet record the final status first
	zero := int64(0)
	err := p.cfg.Client.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		Preconditions:      &metav1.Preconditions{UID: &pod.UID},
	})
	if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		p.log.Warn("remove deleted pod", "pod", key(pod), "err", err)
	}
}

// GetPod returns the pod with its current runtime status.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	t, err := p.find(namespace, name)
	if err != nil {
		return nil, err
	}
	return p.toPod(t), nil
}

// GetPodStatus returns only the pod's status.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

// GetPods lists every pod the provider tracks.
func (p *Provider) GetPods(ctx context.Context) ([]*corev1.Pod, error) {
	p.mu.Lock()
	ts := make([]*tracked, 0, len(p.pods))
	for _, t := range p.pods {
		ts = append(ts, t)
	}
	p.mu.Unlock()
	out := make([]*corev1.Pod, 0, len(ts))
	for _, t := range ts {
		out = append(out, p.toPod(t))
	}
	return out, nil
}

// NotifyPods makes this an async provider: status changes stream from runtimed
// and a periodic resync backstops anything the stream drops.
func (p *Provider) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	p.mu.Lock()
	p.notify = cb
	p.mu.Unlock()
	go p.watch(ctx)
	go p.resync(ctx, 10*time.Second)
	go p.refreshVolumes(ctx, time.Minute)
}

func (p *Provider) watch(ctx context.Context) {
	for ctx.Err() == nil {
		stream, err := p.client.WatchPodStatus(ctx, &runtimev1.WatchPodStatusRequest{})
		if err == nil {
			for {
				ev, err := stream.Recv()
				if err != nil {
					break
				}
				if ev.GetType() != runtimev1.PodStatusEventType_POD_STATUS_EVENT_TYPE_DELETED {
					p.observe(ev.GetStatus())
				}
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

func (p *Provider) resync(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		p.mu.Lock()
		ids := make([]string, 0, len(p.pods))
		for id, t := range p.pods {
			if !t.final {
				ids = append(ids, id)
			}
		}
		p.mu.Unlock()
		for _, id := range ids {
			r, err := p.rt.GetPodStatus(ctx, &runtimev1.GetPodStatusRequest{PodId: id})
			if err == nil {
				p.observe(r.GetStatus())
			}
		}
	}
}

// refreshVolumes re-renders configMap, secret, downwardAPI and projected
// volumes from current data and renews service account tokens, like the
// kubelet's periodic volume sync.
func (p *Provider) refreshVolumes(ctx context.Context, every time.Duration) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		p.mu.Lock()
		pods := make([]*corev1.Pod, 0, len(p.pods))
		for _, t := range p.pods {
			if !t.final && len(t.pod.Spec.Volumes) > 0 {
				pods = append(pods, t.pod)
			}
		}
		p.mu.Unlock()
		for _, pod := range pods {
			if _, err := p.rt.RefreshProjectedVolumes(withPod(ctx, pod), string(pod.UID)); err != nil {
				p.log.Warn("refresh volumes", "pod", key(pod), "err", err)
			}
		}
	}
}

// observe records a runtime status, drives restarts, and notifies VK.
func (p *Provider) observe(st *runtimev1.PodStatus) {
	if st == nil {
		return
	}
	p.mu.Lock()
	t, ok := p.pods[st.GetPodId()]
	if ok {
		t.status = st
	}
	p.mu.Unlock()
	if !ok {
		return
	}
	p.maybeRestart(t, st)
	p.mu.Lock()
	p.syncProbes(t, st)
	p.mu.Unlock()
	p.emit(p.toPod(t))
}

func (p *Provider) emit(pod *corev1.Pod) {
	p.mu.Lock()
	cb := p.notify
	p.mu.Unlock()
	if cb != nil {
		cb(pod)
	}
}

func (p *Provider) find(namespace, name string) (*tracked, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.pods {
		if t.pod.Namespace == namespace && t.pod.Name == name {
			return t, nil
		}
	}
	return nil, errdefs.NotFoundf("pod %s/%s not found", namespace, name)
}

// toPod renders the tracked pod with a corev1 status built from runtimed's.
func (p *Provider) toPod(t *tracked) *corev1.Pod {
	p.mu.Lock()
	if t.final {
		defer p.mu.Unlock()
		return t.pod.DeepCopy()
	}
	pod, st := t.pod.DeepCopy(), t.status
	ov := overrides{
		backoff: make(map[string]bool, len(t.restarts.pending)),
		pulling: make(map[string]bool, len(t.restarts.pullPending)),
		probed:  make(map[string]probeView, len(t.probes)),
	}
	for c := range t.restarts.pending {
		ov.backoff[c] = true
	}
	for c := range t.restarts.pullPending {
		ov.pulling[c] = true
	}
	for c, ps := range t.probes {
		if ps.decides() {
			ov.probed[c] = probeView{started: ps.started, ready: ps.ready}
		}
	}
	p.mu.Unlock()
	pod.Status = toPodStatus(pod, st, p.cfg.NodeIP, ov)
	return pod
}

func key(pod *corev1.Pod) string { return pod.Namespace + "/" + pod.Name }

func podLogDir(root string, pod *corev1.Pod) string {
	return filepath.Join(root, pod.Namespace+"_"+pod.Name+"_"+string(pod.UID))
}

// ensureLogDirs creates <root>/<ns>_<name>_<uid>/<container>/ for every
// container. runtimed requires the tree and derives restart indexes from it.
func ensureLogDirs(dir string, pod *corev1.Pod) error {
	var names []string
	for _, c := range pod.Spec.InitContainers {
		names = append(names, c.Name)
	}
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	for _, n := range names {
		if err := os.MkdirAll(filepath.Join(dir, n), 0o755); err != nil {
			return fmt.Errorf("create log dir: %w", err)
		}
	}
	return nil
}

var errNoContainer = errors.New("container not found")
