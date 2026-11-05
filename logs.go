package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
)

// openLog returns the container log, honoring tail, limitBytes and follow.
func openLog(reqCtx, podCtx context.Context, path string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if opts.Tail > 0 {
		if _, err := f.Seek(tailOffset(f, opts.Tail), io.SeekStart); err != nil {
			f.Close()
			return nil, err
		}
	}
	var r io.Reader = f
	if opts.Follow {
		r = &followReader{f: f, req: reqCtx, pod: podCtx}
	}
	if opts.LimitBytes > 0 {
		r = io.LimitReader(r, int64(opts.LimitBytes))
	}
	return struct {
		io.Reader
		io.Closer
	}{r, f}, nil
}

// tailOffset finds the byte offset where the last n lines start.
func tailOffset(f *os.File, n int) int64 {
	data, err := io.ReadAll(f)
	if err != nil {
		return 0
	}
	data = bytes.TrimSuffix(data, []byte("\n"))
	for i := len(data) - 1; i >= 0; i-- {
		if data[i] == '\n' {
			n--
			if n == 0 {
				return int64(i + 1)
			}
		}
	}
	return 0
}

// followReader keeps reading past EOF until the request or pod ends.
type followReader struct {
	f        *os.File
	req, pod context.Context
}

func (r *followReader) Read(p []byte) (int, error) {
	for {
		n, err := r.f.Read(p)
		if n > 0 || err != io.EOF {
			return n, err
		}
		select {
		case <-r.req.Done():
			return 0, io.EOF
		case <-r.pod.Done():
			return 0, io.EOF
		case <-time.After(300 * time.Millisecond):
		}
	}
}
