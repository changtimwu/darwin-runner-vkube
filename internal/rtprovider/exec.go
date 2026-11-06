package rtprovider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	utilexec "k8s.io/client-go/util/exec"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// execCommand resolves an absolute command against the pod's filesystem
// first, so `kubectl exec pod -- /bin/tool` runs the image's tool rather than
// looking for it on the host. runtimed resolves only bare names. A vm pod's
// command already runs inside its guest.
func (p *Provider) execCommand(pod *corev1.Pod, cmd []string) []string {
	if len(cmd) == 0 || !filepath.IsAbs(cmd[0]) {
		return cmd
	}
	p.mu.Lock()
	t, ok := p.pods[string(pod.UID)]
	vm := ok && t.vm
	p.mu.Unlock()
	if vm {
		return cmd
	}
	inPod := filepath.Join(p.cfg.Root, "pods", string(pod.UID), "rootfs", cmd[0])
	if fi, err := os.Stat(inPod); err == nil && fi.Mode().IsRegular() {
		return append([]string{inPod}, cmd[1:]...)
	}
	return cmd
}

// RunInContainer serves `kubectl exec` through runtimed's Exec stream; the
// command re-enters the pod's Seatbelt profile and identity.
func (p *Provider) RunInContainer(ctx context.Context, namespace, podName, container string, cmd []string, attach api.AttachIO) error {
	t, err := p.find(namespace, podName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := p.client.Exec(ctx)
	if err != nil {
		return err
	}
	first := &runtimev1.ExecRequest{
		PodId: string(t.pod.UID), Container: container, Command: p.execCommand(t.pod, cmd), Tty: attach.TTY(),
		Stdin: attach.Stdin() != nil, Stdout: attach.Stdout() != nil, Stderr: attach.Stderr() != nil,
	}
	if err := stream.Send(first); err != nil {
		return err
	}

	// Sends are serialized: stdin and resize run on separate goroutines.
	var sendMu sync.Mutex
	send := func(r *runtimev1.ExecRequest) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		return stream.Send(r)
	}
	if in := attach.Stdin(); in != nil {
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := in.Read(buf)
				if n > 0 && send(&runtimev1.ExecRequest{StdinData: append([]byte(nil), buf[:n]...)}) != nil {
					return
				}
				if err != nil {
					sendMu.Lock()
					_ = stream.CloseSend()
					sendMu.Unlock()
					return
				}
			}
		}()
	}
	if attach.TTY() {
		go func() {
			for sz := range attach.Resize() {
				if send(&runtimev1.ExecRequest{Resize: &runtimev1.TerminalSize{Width: uint32(sz.Width), Height: uint32(sz.Height)}}) != nil {
					return
				}
			}
		}()
	}

	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if b := resp.GetStdout(); len(b) > 0 && attach.Stdout() != nil {
			_, _ = attach.Stdout().Write(b)
		}
		if b := resp.GetStderr(); len(b) > 0 && attach.Stderr() != nil {
			_, _ = attach.Stderr().Write(b)
		}
		if ex := resp.GetExit(); ex != nil {
			if e := ex.GetError(); e != nil && e.GetMessage() != "" {
				return errors.New(e.GetMessage())
			}
			if code := ex.GetExitCode(); code != 0 {
				// The kubelet API turns this into the exec's exit status.
				return utilexec.CodeExitError{Err: fmt.Errorf("command terminated with exit code %d", code), Code: int(code)}
			}
			return nil
		}
	}
}

// AttachToContainer is not supported: runtimed has no attach for native pods.
func (p *Provider) AttachToContainer(ctx context.Context, namespace, podName, container string, attach api.AttachIO) error {
	return errdefs.InvalidInput("kubectl attach is not supported on darwin-vk nodes")
}

// PortForward proxies one forwarded connection through runtimed, which dials
// the pod port on this node.
func (p *Provider) PortForward(ctx context.Context, namespace, podName string, port int32, conn io.ReadWriteCloser) error {
	t, err := p.find(namespace, podName)
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := p.client.PortForward(ctx)
	if err != nil {
		return err
	}
	const connID = 1
	if err := stream.Send(&runtimev1.PortForwardRequest{PodId: string(t.pod.UID), Port: port, ConnectionId: connID}); err != nil {
		return err
	}
	go func() {
		defer cancel()
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 && stream.Send(&runtimev1.PortForwardRequest{PodId: string(t.pod.UID), Port: port, ConnectionId: connID, Data: append([]byte(nil), buf[:n]...)}) != nil {
				return
			}
			if err != nil {
				_ = stream.Send(&runtimev1.PortForwardRequest{PodId: string(t.pod.UID), Port: port, ConnectionId: connID, Close: true})
				return
			}
		}
	}()
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if e := resp.GetError(); e != nil && e.GetMessage() != "" {
			return fmt.Errorf("port-forward to %d: %s", port, e.GetMessage())
		}
		if len(resp.GetData()) > 0 {
			if _, err := conn.Write(resp.GetData()); err != nil {
				return nil
			}
		}
		if resp.GetClose() {
			return nil
		}
	}
}
