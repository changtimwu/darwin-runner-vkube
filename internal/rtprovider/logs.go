package rtprovider

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// GetContainerLogs serves `kubectl logs` from the CRI log file runtimed writes
// at ContainerStatus.log_path (runtimed's GetLogs RPC is unimplemented by design).
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, podName, container string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	t, err := p.find(namespace, podName)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	podID, st, pod := string(t.pod.UID), t.status, t.pod
	p.mu.Unlock()
	path := ""
	if cs := findContainer(st, container); cs != nil {
		path = cs.GetLogPath()
	}
	if path == "" {
		path = latestLog(filepath.Join(podLogDir(p.cfg.PodLogsDir, pod), container))
	}
	if path == "" {
		return nil, errdefs.NotFoundf("no logs for container %s in pod %s/%s", container, namespace, podName)
	}
	if opts.Previous {
		if path, err = previousLog(path); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errdefs.AsNotFound(err)
	}

	pr, pw := io.Pipe()
	go func() {
		defer f.Close()
		w := io.Writer(pw)
		if opts.LimitBytes > 0 {
			w = &limitWriter{w: pw, n: int64(opts.LimitBytes)}
		}
		follow := opts.Follow && !opts.Previous
		err := copyCRILog(ctx, f, w, opts, follow, func() bool { return p.containerRunning(podID, container) })
		pw.CloseWithError(err)
	}()
	return pr, nil
}

func (p *Provider) containerRunning(podID, container string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	t, ok := p.pods[podID]
	if !ok {
		return false
	}
	cs := findContainer(t.status, container)
	return cs != nil && cs.GetState().GetRunning() != nil
}

func findContainer(st *runtimev1.PodStatus, name string) *runtimev1.ContainerStatus {
	for _, cs := range append(st.GetContainerStatuses(), st.GetInitContainerStatuses()...) {
		if cs.GetName() == name {
			return cs
		}
	}
	return nil
}

// latestLog returns the highest-numbered <n>.log in dir, or "".
func latestLog(dir string) string {
	entries, _ := os.ReadDir(dir)
	best, path := -1, ""
	for _, e := range entries {
		n, err := strconv.Atoi(strings.TrimSuffix(e.Name(), ".log"))
		if err == nil && strings.HasSuffix(e.Name(), ".log") && n > best {
			best, path = n, filepath.Join(dir, e.Name())
		}
	}
	return path
}

// previousLog maps <dir>/<n>.log to <dir>/<n-1>.log.
func previousLog(path string) (string, error) {
	n, err := strconv.Atoi(strings.TrimSuffix(filepath.Base(path), ".log"))
	if err != nil || n == 0 {
		return "", errdefs.NotFoundf("no previous instance of this container")
	}
	return filepath.Join(filepath.Dir(path), fmt.Sprintf("%d.log", n-1)), nil
}

// criLine is one parsed CRI log record: "<RFC3339Nano> <stream> <P|F> <content>".
type criLine struct {
	ts      time.Time
	content []byte
	partial bool
}

func parseCRI(line []byte) (criLine, bool) {
	line = bytes.TrimSuffix(line, []byte("\n"))
	parts := bytes.SplitN(line, []byte(" "), 4)
	if len(parts) < 3 {
		return criLine{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, string(parts[0]))
	if err != nil {
		return criLine{}, false
	}
	l := criLine{ts: ts, partial: string(parts[2]) == "P"}
	if len(parts) == 4 {
		l.content = append([]byte(nil), parts[3]...) // the read buffer is reused
	}
	return l, true
}

// copyCRILog writes the decoded log to w honoring tail/since/timestamps, then
// keeps following while running() reports the container alive.
func copyCRILog(ctx context.Context, f *os.File, w io.Writer, opts api.ContainerLogOpts, follow bool, running func() bool) error {
	since := opts.SinceTime
	if opts.SinceSeconds > 0 {
		since = time.Now().Add(-time.Duration(opts.SinceSeconds) * time.Second)
	}
	write := func(l criLine) error {
		if !since.IsZero() && l.ts.Before(since) {
			return nil
		}
		var b []byte
		if opts.Timestamps {
			b = append(b, l.ts.Format(time.RFC3339Nano)...)
			b = append(b, ' ')
		}
		b = append(b, l.content...)
		if !l.partial {
			b = append(b, '\n')
		}
		_, err := w.Write(b)
		return err
	}

	r := bufio.NewReader(f)
	var lines []criLine
	var pending []byte
	for {
		chunk, err := r.ReadBytes('\n')
		pending = append(pending, chunk...)
		if err == nil {
			if l, ok := parseCRI(pending); ok {
				if opts.Tail > 0 {
					lines = append(lines, l)
					if len(lines) > opts.Tail {
						lines = lines[1:]
					}
				} else if werr := write(l); werr != nil {
					return werr
				}
			}
			pending = pending[:0]
			continue
		}
		if err != io.EOF {
			return err
		}
		// At EOF: flush a buffered tail once, then follow or stop.
		for _, l := range lines {
			if werr := write(l); werr != nil {
				return werr
			}
		}
		lines, opts.Tail = nil, 0
		if !follow {
			return nil
		}
		if !running() {
			follow = false // one more pass drains lines written before the exit
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(250 * time.Millisecond):
		}
	}
}

type limitWriter struct {
	w io.Writer
	n int64
}

func (l *limitWriter) Write(b []byte) (int, error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(b)) > l.n {
		b = b[:l.n]
	}
	n, err := l.w.Write(b)
	l.n -= int64(n)
	return n, err
}
