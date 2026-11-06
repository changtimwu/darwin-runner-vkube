// Command p0 is the P0 spike: drive k3sm's runtimed in-process, with no
// Kubernetes involved, to prove it builds and runs pods on this Mac.
//
//	go build -o bin/p0 . && go build -o bin/k3sm-execshim k3sm.io/runtimed/cmd/k3sm-execshim
//	./bin/p0 [-image ref] [-gpu]
//
// See FINDINGS.md for results.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/runtime"
)

func main() {
	home, _ := os.UserHomeDir()
	var (
		root    = flag.String("root", filepath.Join(home, ".darwin-vk-p0", "runtime"), "runtimed root (absolute, clean)")
		logs    = flag.String("pod-logs", filepath.Join(home, ".darwin-vk-p0", "pod-logs"), "pod log root")
		img     = flag.String("image", "", "optional darwin/arm64 OCI image to run")
		imgCmd  = flag.String("image-cmd", "", "optional command for -image (default: image entrypoint)")
		gpu     = flag.Bool("gpu", false, "set allow_gpu on the pods")
		verbose = flag.Bool("v", false, "debug logging")
	)
	flag.Parse()

	lvl := slog.LevelInfo
	if *verbose {
		lvl = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	if err := run(*root, *logs, *img, *imgCmd, *gpu, log); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
}

func run(root, logs, img, imgCmd string, gpu bool, log *slog.Logger) error {
	ctx := context.Background()
	if err := os.MkdirAll(logs, 0o755); err != nil {
		return err
	}
	rt, err := runtime.New(runtime.Config{Root: root, PodLogsDir: logs, RuntimeVersion: "p0-spike", Logger: log}, runtime.Deps{})
	if err != nil {
		return fmt.Errorf("runtime.New: %w", err)
	}
	defer rt.Close()

	info, err := rt.GetRuntimeInfo(ctx, &runtimev1.GetRuntimeInfoRequest{})
	if err != nil {
		return fmt.Errorf("GetRuntimeInfo: %w", err)
	}
	section("GetRuntimeInfo")
	fmt.Println(protojson.Format(info))

	home, _ := os.UserHomeDir()
	native := &runtimev1.Container{
		Name:    "main",
		Image:   "native",
		Command: []string{"/bin/sh", "-c"},
		Args: []string{`echo "hello from $(sw_vers -productVersion) $(uname -m) uid=$(id -u) pwd=$PWD"
echo "write pod dir:"; echo ok > ./probe && cat ./probe
echo "read host home (expect denied):"; ls ` + home + ` 2>&1 | head -2
echo "write /tmp (expect denied):"; touch /tmp/p0-escape 2>&1
exit 0`},
	}
	if err := runPod(ctx, rt, root, logs, "p0-native", native, gpu); err != nil {
		return err
	}

	if img != "" {
		c := &runtimev1.Container{Name: "main", Image: img}
		if imgCmd != "" {
			c.Command = []string{imgCmd}
		}
		if err := runPod(ctx, rt, root, logs, "p0-image", c, gpu); err != nil {
			return err
		}
	}
	return nil
}

func runPod(ctx context.Context, rt *runtime.Runtime, root, logs, name string, c *runtimev1.Container, gpu bool) error {
	section("pod " + name)
	id := fmt.Sprintf("%s-%d", name, time.Now().UnixNano())
	logDir := filepath.Join(logs, "default_"+name+"_"+id)
	if err := os.MkdirAll(filepath.Join(logDir, c.Name), 0o755); err != nil {
		return err
	}
	box := &runtimev1.PodBox{
		PodId:                         id,
		Namespace:                     "default",
		Name:                          name,
		PodIp:                         "127.0.0.1",
		Containers:                    []*runtimev1.Container{c},
		LogDirectory:                  logDir,
		TerminationGracePeriodSeconds: 5,
		SignaturePolicy:               runtimev1.SignaturePolicy_SIGNATURE_POLICY_ADHOC_OK,
		SandboxProfile: &runtimev1.SandboxProfile{
			DataVolumePath: filepath.Join(root, "pods", id, "rootfs"),
			AllowNetwork:   true,
			AllowGpu:       gpu,
		},
	}
	start := time.Now()
	resp, err := rt.CreatePod(ctx, &runtimev1.CreatePodRequest{Pod: box})
	if err != nil {
		return fmt.Errorf("CreatePod: %w", err)
	}
	fmt.Printf("CreatePod returned in %s: %s\n", time.Since(start).Round(time.Millisecond), compact(resp))

	var st *runtimev1.PodStatus
	for deadline := time.Now().Add(2 * time.Minute); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		r, err := rt.GetPodStatus(ctx, &runtimev1.GetPodStatusRequest{PodId: id})
		if err != nil {
			return fmt.Errorf("GetPodStatus: %w", err)
		}
		st = r.GetStatus()
		if done(st) {
			break
		}
	}
	fmt.Printf("final status after %s:\n%s\n", time.Since(start).Round(time.Millisecond), protojson.Format(st))

	for _, cs := range st.GetContainerStatuses() {
		if p := cs.GetLogPath(); p != "" {
			b, err := os.ReadFile(p)
			fmt.Printf("--- CRI log %s (err=%v)\n%s", p, err, b)
		}
	}

	_, err = rt.DeletePod(ctx, &runtimev1.DeletePodRequest{PodId: id})
	fmt.Printf("DeletePod err=%v\n", err)
	return nil
}

// done reports whether every container has terminated (or the pod failed outright).
func done(st *runtimev1.PodStatus) bool {
	if st == nil {
		return false
	}
	css := st.GetContainerStatuses()
	if len(css) == 0 {
		return st.GetPhase() == runtimev1.PodPhase_POD_PHASE_FAILED
	}
	for _, cs := range css {
		if cs.GetState().GetTerminated() == nil {
			return false
		}
	}
	return true
}

func compact(m proto.Message) string { return protojson.MarshalOptions{}.Format(m) }

func section(s string) { fmt.Printf("\n===== %s =====\n", s) }
