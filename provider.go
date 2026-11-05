package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/creack/pty"
	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/log"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	statsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

// Provider runs each container of a pod as a host process. The image field is
// ignored; containers must set `command`. All pods share the host network, so
// the pod IP is the node IP.
type Provider struct {
	nodeName  string
	nodeIP    string
	dataDir   string
	disableIO bool // exec/port-forward disabled when the API is unauthenticated or off

	mu   sync.Mutex
	pods map[string]*podRun
}

type podRun struct {
	pod       *v1.Pod
	dir       string
	ctx       context.Context
	cancel    context.CancelFunc
	startTime metav1.Time

	mu         sync.Mutex
	initDone   bool
	initFailed bool
	containers map[string]*ctrRun
}

type ctrRun struct {
	spec     v1.Container
	logPath  string
	state    v1.ContainerState
	last     v1.ContainerState
	restarts int32
	proc     *os.Process
}

func NewProvider(nodeName, nodeIP, dataDir string, disableIO bool) (*Provider, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	return &Provider{nodeName: nodeName, nodeIP: nodeIP, dataDir: dataDir, disableIO: disableIO, pods: map[string]*podRun{}}, nil
}

func key(ns, name string) string { return ns + "/" + name }

func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	k := key(pod.Namespace, pod.Name)
	p.mu.Lock()
	if _, ok := p.pods[k]; ok {
		p.mu.Unlock()
		return errdefs.AsInvalidInput(fmt.Errorf("pod %s already exists", k))
	}
	dir := filepath.Join(p.dataDir, "pods", pod.Namespace, pod.Name+"_"+string(pod.UID))
	runCtx, cancel := context.WithCancel(context.Background())
	pr := &podRun{pod: pod.DeepCopy(), dir: dir, ctx: runCtx, cancel: cancel, startTime: metav1.Now(), containers: map[string]*ctrRun{}}
	for _, c := range append(append([]v1.Container{}, pod.Spec.InitContainers...), pod.Spec.Containers...) {
		pr.containers[c.Name] = &ctrRun{
			spec:    c,
			logPath: filepath.Join(dir, "logs", c.Name+".log"),
			state:   v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "PodInitializing"}},
		}
	}
	p.pods[k] = pr
	p.mu.Unlock()

	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		return err
	}
	log.G(ctx).Infof("starting pod %s", k)
	go p.runPod(pr)
	return nil
}

func (p *Provider) runPod(pr *podRun) {
	for _, c := range pr.pod.Spec.InitContainers {
		code := p.runOnce(pr, pr.containers[c.Name])
		if code != 0 {
			pr.mu.Lock()
			pr.initFailed = true
			pr.mu.Unlock()
			return
		}
	}
	pr.mu.Lock()
	pr.initDone = true
	pr.mu.Unlock()
	for _, c := range pr.pod.Spec.Containers {
		go p.supervise(pr, pr.containers[c.Name])
	}
}

// supervise runs a container and restarts it according to the pod's restartPolicy.
func (p *Provider) supervise(pr *podRun, cr *ctrRun) {
	backoff := 10 * time.Second
	for {
		code := p.runOnce(pr, cr)
		policy := pr.pod.Spec.RestartPolicy
		if pr.ctx.Err() != nil || policy == v1.RestartPolicyNever || (policy == v1.RestartPolicyOnFailure && code == 0) {
			return
		}
		select {
		case <-pr.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Minute)
		pr.mu.Lock()
		cr.restarts++
		pr.mu.Unlock()
	}
}

// runOnce starts the container process, waits for it to exit and returns its exit code.
func (p *Provider) runOnce(pr *podRun, cr *ctrRun) int32 {
	setState := func(s v1.ContainerState) {
		pr.mu.Lock()
		if cr.state.Terminated != nil {
			cr.last = cr.state
		}
		cr.state = s
		pr.mu.Unlock()
	}
	fail := func(reason, msg string) int32 {
		setState(v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: 128, Reason: reason, Message: msg, FinishedAt: metav1.Now()}})
		return 128
	}

	argv := append(append([]string{}, cr.spec.Command...), cr.spec.Args...)
	if len(cr.spec.Command) == 0 {
		return fail("NoCommand", "darwin-vk runs host processes; container.command is required")
	}
	logf, err := os.OpenFile(cr.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fail("LogOpenError", err.Error())
	}
	defer logf.Close()

	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Dir = p.workDir(pr, cr.spec)
	cmd.Env = containerEnv(pr.pod, cr.spec)
	cmd.Stdout, cmd.Stderr = logf, logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return fail("StartError", err.Error())
	}
	started := metav1.Now()
	pr.mu.Lock()
	cr.proc = cmd.Process
	pr.mu.Unlock()
	setState(v1.ContainerState{Running: &v1.ContainerStateRunning{StartedAt: started}})

	waitErr := cmd.Wait()
	code := int32(0)
	reason := "Completed"
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		code = int32(exitErr.ExitCode())
		reason = "Error"
		if ws, ok := exitErr.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			code = 128 + int32(ws.Signal())
		}
	}
	setState(v1.ContainerState{Terminated: &v1.ContainerStateTerminated{ExitCode: code, Reason: reason, StartedAt: started, FinishedAt: metav1.Now()}})
	return code
}

func (p *Provider) workDir(pr *podRun, c v1.Container) string {
	if c.WorkingDir != "" {
		return c.WorkingDir
	}
	return pr.dir
}

func containerEnv(pod *v1.Pod, c v1.Container) []string {
	env := os.Environ()
	env = append(env, "HOSTNAME="+pod.Name, "POD_NAME="+pod.Name, "POD_NAMESPACE="+pod.Namespace)
	// virtual-kubelet resolves configMap/secret/fieldRef env sources into Value before CreatePod.
	for _, e := range c.Env {
		env = append(env, e.Name+"="+e.Value)
	}
	return env
}

func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pr, ok := p.pods[key(pod.Namespace, pod.Name)]; ok {
		pr.mu.Lock()
		pr.pod.ObjectMeta = *pod.ObjectMeta.DeepCopy()
		pr.mu.Unlock()
	}
	return nil
}

func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	k := key(pod.Namespace, pod.Name)
	p.mu.Lock()
	pr, ok := p.pods[k]
	delete(p.pods, k)
	p.mu.Unlock()
	if !ok {
		return errdefs.NotFoundf("pod %s not found", k)
	}
	log.G(ctx).Infof("stopping pod %s", k)
	pr.cancel()

	grace := 30 * time.Second
	if pod.Spec.TerminationGracePeriodSeconds != nil {
		grace = time.Duration(*pod.Spec.TerminationGracePeriodSeconds) * time.Second
	}
	pr.mu.Lock()
	var pgids []int
	for _, cr := range pr.containers {
		if cr.proc != nil && cr.state.Running != nil {
			pgids = append(pgids, cr.proc.Pid)
		}
	}
	pr.mu.Unlock()
	for _, pg := range pgids {
		_ = syscall.Kill(-pg, syscall.SIGTERM)
	}
	go func() {
		deadline := time.Now().Add(grace)
		for _, pg := range pgids {
			for time.Now().Before(deadline) && syscall.Kill(-pg, 0) == nil {
				time.Sleep(200 * time.Millisecond)
			}
			_ = syscall.Kill(-pg, syscall.SIGKILL)
		}
		_ = os.RemoveAll(pr.dir)
	}()
	return nil
}

func (p *Provider) get(ns, name string) (*podRun, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pr, ok := p.pods[key(ns, name)]
	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s not found", ns, name)
	}
	return pr, nil
}

func (p *Provider) GetPod(ctx context.Context, ns, name string) (*v1.Pod, error) {
	pr, err := p.get(ns, name)
	if err != nil {
		return nil, err
	}
	return p.snapshot(pr), nil
}

func (p *Provider) GetPodStatus(ctx context.Context, ns, name string) (*v1.PodStatus, error) {
	pod, err := p.GetPod(ctx, ns, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}

func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	p.mu.Lock()
	runs := make([]*podRun, 0, len(p.pods))
	for _, pr := range p.pods {
		runs = append(runs, pr)
	}
	p.mu.Unlock()
	out := make([]*v1.Pod, 0, len(runs))
	for _, pr := range runs {
		out = append(out, p.snapshot(pr))
	}
	return out, nil
}

// snapshot builds the pod object with an up-to-date status.
func (p *Provider) snapshot(pr *podRun) *v1.Pod {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	pod := pr.pod.DeepCopy()

	statuses := func(cs []v1.Container) []v1.ContainerStatus {
		var out []v1.ContainerStatus
		for _, c := range cs {
			cr := pr.containers[c.Name]
			running := cr.state.Running != nil
			out = append(out, v1.ContainerStatus{
				Name: c.Name, Image: c.Image, ImageID: "process://" + c.Image,
				ContainerID: fmt.Sprintf("process://%s/%s", pod.UID, c.Name),
				State:       cr.state, LastTerminationState: cr.last,
				Ready: running, Started: &running, RestartCount: cr.restarts,
			})
		}
		return out
	}
	pod.Status.InitContainerStatuses = statuses(pod.Spec.InitContainers)
	pod.Status.ContainerStatuses = statuses(pod.Spec.Containers)

	var running, succeeded, failed int
	for _, cs := range pod.Status.ContainerStatuses {
		switch {
		case cs.State.Running != nil:
			running++
		case cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0:
			succeeded++
		case cs.State.Terminated != nil:
			failed++
		}
	}
	n := len(pod.Spec.Containers)
	restartAlways := pod.Spec.RestartPolicy == v1.RestartPolicyAlways || pod.Spec.RestartPolicy == ""
	phase := v1.PodPending
	switch {
	case pr.initFailed && pod.Spec.RestartPolicy != v1.RestartPolicyAlways:
		phase = v1.PodFailed
	case !pr.initDone:
		phase = v1.PodPending
	case succeeded == n && !restartAlways:
		phase = v1.PodSucceeded
	case succeeded+failed == n && failed > 0 && pod.Spec.RestartPolicy == v1.RestartPolicyNever:
		phase = v1.PodFailed
	case running > 0 || succeeded+failed > 0:
		phase = v1.PodRunning
	}

	ready := running == n
	cond := func(t v1.PodConditionType, ok bool) v1.PodCondition {
		s := v1.ConditionFalse
		if ok {
			s = v1.ConditionTrue
		}
		return v1.PodCondition{Type: t, Status: s, LastTransitionTime: pr.startTime}
	}
	pod.Status.Phase = phase
	pod.Status.HostIP = p.nodeIP
	pod.Status.PodIP = p.nodeIP
	pod.Status.HostIPs = []v1.HostIP{{IP: p.nodeIP}}
	pod.Status.PodIPs = []v1.PodIP{{IP: p.nodeIP}}
	pod.Status.StartTime = &pr.startTime
	pod.Status.Conditions = []v1.PodCondition{
		cond(v1.PodScheduled, true),
		cond(v1.PodInitialized, pr.initDone),
		cond(v1.ContainersReady, ready),
		cond(v1.PodReady, ready),
	}
	return pod
}

func (p *Provider) GetContainerLogs(ctx context.Context, ns, podName, ctrName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	pr, err := p.get(ns, podName)
	if err != nil {
		return nil, err
	}
	cr, ok := pr.containers[ctrName]
	if !ok {
		return nil, errdefs.NotFoundf("container %s not found", ctrName)
	}
	return openLog(ctx, pr.ctx, cr.logPath, opts)
}

func (p *Provider) RunInContainer(ctx context.Context, ns, podName, ctrName string, argv []string, attach api.AttachIO) error {
	if p.disableIO {
		return errdefs.InvalidInput("exec disabled")
	}
	pr, err := p.get(ns, podName)
	if err != nil {
		return err
	}
	cr, ok := pr.containers[ctrName]
	if !ok || len(argv) == 0 {
		return errdefs.NotFoundf("container %s not found", ctrName)
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = p.workDir(pr, cr.spec)
	cmd.Env = containerEnv(pr.pod, cr.spec)

	if attach.TTY() {
		f, err := pty.Start(cmd)
		if err != nil {
			return err
		}
		defer f.Close()
		go func() {
			for sz := range attach.Resize() {
				_ = pty.Setsize(f, &pty.Winsize{Rows: sz.Height, Cols: sz.Width})
			}
		}()
		if in := attach.Stdin(); in != nil {
			go io.Copy(f, in) //nolint:errcheck
		}
		if out := attach.Stdout(); out != nil {
			go io.Copy(out, f) //nolint:errcheck
		}
		return cmd.Wait()
	}
	if in := attach.Stdin(); in != nil {
		cmd.Stdin = in
	}
	if out := attach.Stdout(); out != nil {
		cmd.Stdout = out
	}
	if e := attach.Stderr(); e != nil {
		cmd.Stderr = e
	}
	return cmd.Run()
}

func (p *Provider) AttachToContainer(ctx context.Context, ns, podName, ctrName string, attach api.AttachIO) error {
	return errdefs.InvalidInput("attach is not supported by the darwin provider")
}

// PortForward dials the port on localhost, since pods share the host network.
func (p *Provider) PortForward(ctx context.Context, ns, podName string, port int32, stream io.ReadWriteCloser) error {
	if p.disableIO {
		return errdefs.InvalidInput("port-forward disabled")
	}
	if _, err := p.get(ns, podName); err != nil {
		return err
	}
	defer stream.Close()
	conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return err
	}
	defer conn.Close()
	done := make(chan struct{}, 2)
	go func() { io.Copy(conn, stream); done <- struct{}{} }() //nolint:errcheck
	go func() { io.Copy(stream, conn); done <- struct{}{} }() //nolint:errcheck
	select {
	case <-done:
	case <-ctx.Done():
	}
	return nil
}

func (p *Provider) GetStatsSummary(ctx context.Context) (*statsv1alpha1.Summary, error) {
	return &statsv1alpha1.Summary{Node: statsv1alpha1.NodeStats{NodeName: p.nodeName, StartTime: metav1.Now()}}, nil
}

func (p *Provider) GetMetricsResource(ctx context.Context) ([]*dto.MetricFamily, error) {
	return nil, nil
}
