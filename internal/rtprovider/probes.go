package rtprovider

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// runtimed carries probe definitions but never runs them, and has no
// lifecycle hooks at all: the provider plays the kubelet's prober and hook
// runner. Exec handlers re-enter the container through runtimed's Exec;
// network handlers dial the pod IP, which is the node's.

// probeSet is the probe and hook state of one running container instance.
// Guarded by Provider.mu.
type probeSet struct {
	cid       string // the container instance the workers watch
	cancel    context.CancelFunc
	started   bool // startup probe passed, or there is none
	startedCh chan struct{}
	ready     bool // last readiness verdict
	// hasStartup and hasReadiness mean probes, not runtimed, decide Started
	// and Ready.
	hasStartup, hasReadiness bool
}

func (ps *probeSet) decides() bool { return ps.hasStartup || ps.hasReadiness }

// syncProbes starts the hooks and probes of each newly running container
// instance and stops those of instances that are gone. Callers hold p.mu.
func (p *Provider) syncProbes(t *tracked, st *runtimev1.PodStatus) {
	if t.probes == nil {
		t.probes = map[string]*probeSet{}
	}
	running := map[string]string{}
	for _, cs := range st.GetContainerStatuses() {
		if cs.GetState().GetRunning() != nil && cs.GetContainerId() != "" {
			running[cs.GetName()] = cs.GetContainerId()
		}
	}
	for name, ps := range t.probes {
		if running[name] != ps.cid {
			ps.cancel()
			delete(t.probes, name)
		}
	}
	for _, c := range t.pod.Spec.Containers {
		cid, ok := running[c.Name]
		if !ok || t.probes[c.Name] != nil {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		ps := &probeSet{
			cid: cid, cancel: cancel, startedCh: make(chan struct{}),
			hasStartup: c.StartupProbe != nil, hasReadiness: c.ReadinessProbe != nil,
		}
		ps.started = !ps.hasStartup
		ps.ready = !ps.hasReadiness
		t.probes[c.Name] = ps
		go p.runContainerProbes(ctx, t.pod, c, ps)
	}
}

// stopProbes cancels every worker of a pod. Callers hold p.mu.
func stopProbes(t *tracked) {
	for name, ps := range t.probes {
		ps.cancel()
		delete(t.probes, name)
	}
}

// runContainerProbes runs postStart, then the probes, as the kubelet does:
// liveness and readiness wait for the startup probe to pass.
func (p *Provider) runContainerProbes(ctx context.Context, pod *corev1.Pod, c corev1.Container, ps *probeSet) {
	if h := c.Lifecycle; h != nil && h.PostStart != nil {
		if out, err := p.runHandler(ctx, pod, c, lifecycleAction(h.PostStart), 0); err != nil {
			if ctx.Err() != nil {
				return
			}
			p.event(pod, corev1.EventTypeWarning, "FailedPostStartHook", "PostStartHook failed: %s", describe(err, out))
			p.kill(pod, c, "FailedPostStartHook")
			return
		}
	}
	if c.StartupProbe == nil {
		close(ps.startedCh)
	} else {
		go p.probeLoop(ctx, pod, c, "Startup", c.StartupProbe, nil, func(ok bool) bool {
			if !ok {
				return false
			}
			p.mu.Lock()
			ps.started = true
			p.mu.Unlock()
			close(ps.startedCh)
			p.emitTracked(pod)
			return true // the startup probe stops once it passes
		})
	}
	if c.LivenessProbe != nil {
		go p.probeLoop(ctx, pod, c, "Liveness", c.LivenessProbe, ps.startedCh, func(bool) bool { return false })
	}
	if c.ReadinessProbe != nil {
		go p.probeLoop(ctx, pod, c, "Readiness", c.ReadinessProbe, ps.startedCh, func(ok bool) bool {
			p.mu.Lock()
			changed := ps.ready != ok
			ps.ready = ok
			p.mu.Unlock()
			if changed {
				p.emitTracked(pod)
			}
			return false
		})
	}
}

// probeLoop runs one probe on its period and reports each threshold verdict
// to onVerdict, which returns true to stop. A liveness or startup verdict of
// failure kills the container instead; restartPolicy then decides whether it
// comes back.
func (p *Provider) probeLoop(ctx context.Context, pod *corev1.Pod, c corev1.Container, kind string, pr *corev1.Probe, gate <-chan struct{}, onVerdict func(bool) bool) {
	if gate != nil {
		select {
		case <-ctx.Done():
			return
		case <-gate:
		}
	}
	if !sleepCtx(ctx, time.Duration(pr.InitialDelaySeconds)*time.Second) {
		return
	}
	period := time.Duration(max(pr.PeriodSeconds, 1)) * time.Second
	successNeeded, failureNeeded := max(pr.SuccessThreshold, 1), max(pr.FailureThreshold, 1)
	timeout := time.Duration(max(pr.TimeoutSeconds, 1)) * time.Second
	var successes, failures int32
	verdict := -1 // unknown, so the first threshold crossing always reports
	for {
		out, err := p.runHandler(ctx, pod, c, probeAction(pr.ProbeHandler), timeout)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			successes, failures = successes+1, 0
		} else {
			successes, failures = 0, failures+1
			p.event(pod, corev1.EventTypeWarning, "Unhealthy", "%s probe failed: %s", kind, describe(err, out))
		}
		switch {
		case successes >= successNeeded && verdict != 1:
			verdict = 1
			if onVerdict(true) {
				return
			}
		case failures >= failureNeeded && verdict != 0:
			verdict = 0
			if kind != "Readiness" {
				p.event(pod, corev1.EventTypeNormal, "Killing", "Container %s failed %s probe, will be restarted", c.Name, strings.ToLower(kind))
				p.kill(pod, c, kind+"ProbeFailed")
				return
			}
			if onVerdict(false) {
				return
			}
		}
		if !sleepCtx(ctx, period) {
			return
		}
	}
}

// kill stops a container after its preStop hook, as the kubelet does when a
// probe or postStart fails. The restart path then applies restartPolicy.
func (p *Provider) kill(pod *corev1.Pod, c corev1.Container, reason string) {
	grace := int64(30)
	if g := pod.Spec.TerminationGracePeriodSeconds; g != nil {
		grace = *g
	}
	ctx := context.Background()
	start := time.Now()
	p.preStop(ctx, pod, c, grace)
	left := max(grace-int64(time.Since(start).Seconds()), 1)
	if _, err := p.rt.StopContainer(ctx, &runtimev1.StopContainerRequest{PodId: string(pod.UID), Container: c.Name, GracePeriodSeconds: left, Reason: reason}); err != nil {
		p.log.Warn("stop container", "pod", key(pod), "container", c.Name, "err", err)
	}
	if r, err := p.rt.GetPodStatus(ctx, &runtimev1.GetPodStatusRequest{PodId: string(pod.UID)}); err == nil {
		p.observe(r.GetStatus())
	}
}

// preStop runs the container's preStop hook within grace seconds.
func (p *Provider) preStop(ctx context.Context, pod *corev1.Pod, c corev1.Container, grace int64) {
	h := c.Lifecycle
	if h == nil || h.PreStop == nil {
		return
	}
	if out, err := p.runHandler(ctx, pod, c, lifecycleAction(h.PreStop), time.Duration(max(grace, 1))*time.Second); err != nil {
		p.event(pod, corev1.EventTypeWarning, "FailedPreStopHook", "PreStopHook failed: %s", describe(err, out))
	}
}

// emitTracked re-renders a pod after a probe verdict changed its status.
func (p *Provider) emitTracked(pod *corev1.Pod) {
	p.mu.Lock()
	t, ok := p.pods[string(pod.UID)]
	p.mu.Unlock()
	if ok {
		p.emit(p.toPod(t))
	}
}

// action is a probe or lifecycle handler; exactly one field is set.
type action struct {
	exec  *corev1.ExecAction
	http  *corev1.HTTPGetAction
	tcp   *corev1.TCPSocketAction
	grpc  *corev1.GRPCAction
	sleep *corev1.SleepAction
}

func probeAction(h corev1.ProbeHandler) action {
	return action{exec: h.Exec, http: h.HTTPGet, tcp: h.TCPSocket, grpc: h.GRPC}
}

func lifecycleAction(h *corev1.LifecycleHandler) action {
	return action{exec: h.Exec, http: h.HTTPGet, tcp: h.TCPSocket, sleep: h.Sleep}
}

// errUnhealthy is a handler that ran and reported failure.
var errUnhealthy = errors.New("unhealthy")

// runHandler runs one handler, with a timeout when timeout > 0, and returns
// its output (exec, HTTP body) for the event message.
func (p *Provider) runHandler(ctx context.Context, pod *corev1.Pod, c corev1.Container, a action, timeout time.Duration) (string, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	host := p.probeHost(pod)
	switch {
	case a.exec != nil:
		return p.execProbe(ctx, pod, c.Name, a.exec.Command)
	case a.http != nil:
		port, err := resolvePort(a.http.Port, c)
		if err != nil {
			return "", err
		}
		if a.http.Host != "" {
			host = a.http.Host
		}
		return httpProbe(ctx, a.http, host, port)
	case a.tcp != nil:
		port, err := resolvePort(a.tcp.Port, c)
		if err != nil {
			return "", err
		}
		if a.tcp.Host != "" {
			host = a.tcp.Host
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
		if err != nil {
			return "", err
		}
		return "", conn.Close()
	case a.grpc != nil:
		return "", grpcProbe(ctx, net.JoinHostPort(host, strconv.Itoa(int(a.grpc.Port))), a.grpc.Service)
	case a.sleep != nil:
		sleepCtx(ctx, time.Duration(a.sleep.Seconds)*time.Second)
		return "", nil
	}
	return "", errors.New("handler has no action")
}

// execProbe runs cmd in the container and fails on a non-zero exit.
func (p *Provider) execProbe(ctx context.Context, pod *corev1.Pod, container string, cmd []string) (string, error) {
	stream, err := p.client.Exec(ctx)
	if err != nil {
		return "", err
	}
	if err := stream.Send(&runtimev1.ExecRequest{PodId: string(pod.UID), Container: container, Command: p.execCommand(pod, cmd), Stdout: true, Stderr: true}); err != nil {
		return "", err
	}
	_ = stream.CloseSend()
	var out strings.Builder
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = errors.New("exec ended without an exit status")
			}
			return out.String(), err
		}
		if out.Len() < 4096 {
			out.Write(resp.GetStdout())
			out.Write(resp.GetStderr())
		}
		if ex := resp.GetExit(); ex != nil {
			if e := ex.GetError(); e != nil && e.GetMessage() != "" {
				return out.String(), errors.New(e.GetMessage())
			}
			if ex.GetExitCode() != 0 {
				return out.String(), fmt.Errorf("%w: exit code %d", errUnhealthy, ex.GetExitCode())
			}
			return out.String(), nil
		}
	}
}

// probeHTTP is shared: probes never follow redirects off the probed host and
// skip TLS verification, as the kubelet does.
var probeHTTP = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, DisableKeepAlives: true},
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 || req.URL.Host != via[0].URL.Host {
			return http.ErrUseLastResponse
		}
		return nil
	},
}

func httpProbe(ctx context.Context, h *corev1.HTTPGetAction, host string, port int) (string, error) {
	scheme := strings.ToLower(string(h.Scheme))
	if scheme == "" {
		scheme = "http"
	}
	u := url.URL{Scheme: scheme, Host: net.JoinHostPort(host, strconv.Itoa(port)), Path: h.Path}
	if q := strings.IndexByte(h.Path, '?'); q >= 0 {
		u.Path, u.RawQuery = h.Path[:q], h.Path[q+1:]
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "kube-probe/darwin-vk")
	for _, hd := range h.HTTPHeaders {
		if strings.EqualFold(hd.Name, "Host") {
			req.Host = hd.Value
		} else {
			req.Header.Add(hd.Name, hd.Value)
		}
	}
	resp, err := probeHTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return string(body), fmt.Errorf("%w: HTTP probe failed with statuscode: %d", errUnhealthy, resp.StatusCode)
	}
	return "", nil
}

func grpcProbe(ctx context.Context, addr string, service *string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	req := &healthpb.HealthCheckRequest{}
	if service != nil {
		req.Service = *service
	}
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, req)
	if err != nil {
		return err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("%w: service unhealthy (responded with %q)", errUnhealthy, resp.GetStatus())
	}
	return nil
}

// resolvePort turns a number or a container port name into a port.
func resolvePort(port intstr.IntOrString, c corev1.Container) (int, error) {
	if port.Type == intstr.Int {
		return port.IntValue(), nil
	}
	for _, cp := range c.Ports {
		if cp.Name == port.StrVal {
			return int(cp.ContainerPort), nil
		}
	}
	if n, err := strconv.Atoi(port.StrVal); err == nil {
		return n, nil
	}
	return 0, fmt.Errorf("container %s has no port named %q", c.Name, port.StrVal)
}

func describe(err error, out string) string {
	if out = strings.TrimSpace(out); out != "" {
		return fmt.Sprintf("%v: %s", err, out)
	}
	return err.Error()
}

// sleepCtx sleeps for d, returning false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
