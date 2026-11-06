// Command darwin-vk registers this Mac as a Kubernetes node via virtual-kubelet.
// Pods scheduled onto the node run their containers as native macOS processes.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/log"
	vkslog "github.com/virtual-kubelet/virtual-kubelet/log/slog"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"

	"github.com/changtimwu/darwin-runner-vkube/internal/gpu"
)

func main() {
	hostname, _ := os.Hostname()
	hostname = strings.ToLower(strings.TrimSuffix(hostname, ".local"))
	home, _ := os.UserHomeDir()
	defaultDataDir := filepath.Join(home, ".darwin-vk")
	if os.Geteuid() == 0 {
		defaultDataDir = "/var/lib/darwin-vk"
	}

	var (
		nodeName     = flag.String("nodename", hostname, "Kubernetes node name")
		kubeconfig   = flag.String("kubeconfig", os.Getenv("KUBECONFIG"), "path to kubeconfig for the cluster")
		dataDir      = flag.String("data-dir", defaultDataDir, "directory for images, pod dirs and logs")
		runtimeKind  = flag.String("runtime", "runtimed", "pod runtime: runtimed (Seatbelt-confined, OCI images) or process (unconfined host processes)")
		podUser      = flag.String("pod-user", "_darwinvk", "when running as root, the user pods without a securityContext run as")
		rootPods     = flag.Bool("allow-root-pods", false, "when running as root without --pod-user, let pods run as root (inside Seatbelt)")
		sigPolicy    = flag.String("signature-policy", "adhoc-ok", "image binary signature policy: adhoc-ok, require-signed or require-notarized")
		nodeIP       = flag.String("node-ip", "", "IP advertised for the node and its pods (default: auto-detect)")
		listen       = flag.String("listen", ":10250", "kubelet API listen address (logs/exec/port-forward)")
		tlsCert      = flag.String("tls-cert", "", "kubelet API serving cert (default: self-signed)")
		tlsKey       = flag.String("tls-key", "", "kubelet API serving key")
		clientCA     = flag.String("client-ca", "", "cluster CA used to verify apiserver client certs; enables webhook authn/authz")
		insecureAPI  = flag.Bool("insecure-no-auth", false, "serve kubelet API without auth when --client-ca is unset (DANGEROUS: grants host exec)")
		maxPods      = flag.Int("max-pods", 20, "pod capacity advertised by the node")
		taint        = flag.Bool("taint", true, "taint node with virtual-kubelet.io/provider=darwin:NoSchedule")
		insecureRegs []string
		noGPU        = flag.Bool("disable-gpu", false, "do not advertise the GPU, and refuse pods that request "+string(gpu.ResourceName))
		debug        = flag.Bool("debug", false, "debug logging")
	)
	flag.Func("insecure-registry", "registry host[:port] to pull from over plain HTTP; repeatable (loopback and RFC 1918 addresses already are)", func(s string) error {
		insecureRegs = append(insecureRegs, s)
		return nil
	})
	flag.Parse()

	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	log.L = vkslog.FromSlog(logger)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx = log.WithLogger(ctx, log.L.WithField("node", *nodeName))

	if err := run(ctx, options{
		nodeName: *nodeName, kubeconfig: *kubeconfig, dataDir: *dataDir, nodeIP: *nodeIP,
		listen: *listen, tlsCert: *tlsCert, tlsKey: *tlsKey, clientCA: *clientCA,
		insecureAPI: *insecureAPI, maxPods: *maxPods, taint: *taint,
		runtime: *runtimeKind, podUser: *podUser, rootPods: *rootPods, sigPolicy: *sigPolicy, noGPU: *noGPU, insecureRegs: insecureRegs, logger: logger,
	}); err != nil {
		log.G(ctx).WithError(err).Error("exiting")
		os.Exit(1)
	}
}

type options struct {
	nodeName, kubeconfig, dataDir, nodeIP, listen, tlsCert, tlsKey, clientCA string
	runtime, podUser, sigPolicy                                              string
	insecureAPI, taint, rootPods, noGPU                                      bool
	insecureRegs                                                             []string
	maxPods                                                                  int
	logger                                                                   *slog.Logger
}

func run(ctx context.Context, o options) error {
	client, err := nodeutil.ClientsetFromEnv(o.kubeconfig)
	if err != nil {
		return fmt.Errorf("kube client: %w", err)
	}

	ip := o.nodeIP
	if ip == "" {
		if ip, err = detectNodeIP(o.kubeconfig); err != nil {
			return fmt.Errorf("detect node ip (set --node-ip): %w", err)
		}
	}
	_, portStr, _ := strings.Cut(o.listen, ":")
	var port int32 = 10250
	fmt.Sscanf(portStr, "%d", &port)

	serveAPI := o.clientCA != "" || o.insecureAPI
	mux := http.NewServeMux()

	var newProvider nodeutil.NewProviderFunc
	switch o.runtime {
	case "process":
		newProvider = func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			p, err := NewProvider(o.nodeName, ip, o.dataDir, !serveAPI)
			if err != nil {
				return nil, nil, err
			}
			configureNode(cfg.Node, ip, port, o.maxPods, o.taint, o.dataDir, "process://darwin", nil)
			return p, nil, nil
		}
	case "runtimed":
		p, err := newRuntimedProvider(ctx, client, o, ip)
		if err != nil {
			return err
		}
		defer p.Close()
		newProvider = func(cfg nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
			configureNode(cfg.Node, ip, port, o.maxPods, o.taint, o.dataDir, "runtimed://k3sm-runtimed", p.GPUFacts())
			return p, nil, nil
		}
	default:
		return fmt.Errorf("unknown --runtime %q", o.runtime)
	}

	opts := []nodeutil.NodeOpt{
		nodeutil.WithClient(client),
		func(c *nodeutil.NodeConfig) error {
			c.HTTPListenAddr = o.listen
			c.InformerResyncPeriod = time.Minute
			return nil
		},
	}

	if serveAPI {
		certOpt := nodeutil.WithKeyPairFromPath(o.tlsCert, o.tlsKey)
		if o.tlsCert == "" {
			certOpt = selfSignedCert(o.nodeName, ip)
		}
		tlsOpts := []func(*tls.Config) error{certOpt}
		if o.clientCA != "" {
			tlsOpts = append(tlsOpts, nodeutil.WithCAFromPath(o.clientCA))
		}
		opts = append(opts,
			nodeutil.WithTLSConfig(tlsOpts...),
			nodeutil.AttachProviderRoutes(mux),
			setAuth(o.nodeName, o.clientCA, mux),
		)
	} else {
		log.G(ctx).Warn("kubelet API disabled: pass --client-ca (recommended) to enable kubectl logs/exec/port-forward")
	}

	n, err := nodeutil.NewNode(o.nodeName, newProvider, opts...)
	if err != nil {
		return err
	}

	go n.Run(ctx) //nolint:errcheck
	if err := n.WaitReady(ctx, 2*time.Minute); err != nil {
		return err
	}
	log.G(ctx).Infof("node %s ready (ip %s)", o.nodeName, ip)

	select {
	case <-ctx.Done():
	case <-n.Done():
		return n.Err()
	}
	return nil
}

func setAuth(nodeName, clientCA string, mux http.Handler) nodeutil.NodeOpt {
	return func(cfg *nodeutil.NodeConfig) error {
		auth := nodeutil.NoAuth()
		if clientCA != "" {
			var err error
			auth, err = nodeutil.WebhookAuth(cfg.Client, nodeName, func(c *nodeutil.WebhookAuthConfig) error {
				var err error
				c.AuthnConfig.ClientCertificateCAContentProvider, err = dynamiccertificates.NewDynamicCAContentFromFile("client-ca", clientCA)
				return err
			})
			if err != nil {
				return err
			}
		}
		cfg.Handler = api.InstrumentHandler(nodeutil.WithAuth(auth, mux))
		return nil
	}
}
