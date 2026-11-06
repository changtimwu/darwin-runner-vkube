package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	taintKey       = "virtual-kubelet.io/provider"
	providerName   = "darwin"
	kubeletVersion = "v1.30.0-darwin-vk"
)

// configureNode fills in the node object virtual-kubelet registers.
func configureNode(n *v1.Node, ip string, port int32, maxPods int, taint bool, runtimeVersion string) {
	n.Labels["kubernetes.io/os"] = "darwin"
	n.Labels["kubernetes.io/arch"] = runtime.GOARCH
	n.Labels["node.kubernetes.io/instance-type"] = "darwin-vk"
	n.Labels[taintKey] = providerName

	if taint {
		n.Spec.Taints = append(n.Spec.Taints, v1.Taint{Key: taintKey, Value: providerName, Effect: v1.TaintEffectNoSchedule})
	}

	mem, _ := unix.SysctlUint64("hw.memsize")
	capacity := v1.ResourceList{
		v1.ResourceCPU:    *resource.NewQuantity(int64(runtime.NumCPU()), resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(int64(mem), resource.BinarySI),
		v1.ResourcePods:   *resource.NewQuantity(int64(maxPods), resource.DecimalSI),
	}
	n.Status.Capacity = capacity
	n.Status.Allocatable = capacity.DeepCopy()

	hostname := n.Name
	n.Status.Addresses = []v1.NodeAddress{
		{Type: v1.NodeInternalIP, Address: ip},
		{Type: v1.NodeHostName, Address: hostname},
	}
	n.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{KubeletEndpoint: v1.DaemonEndpoint{Port: port}}

	osVersion, _ := exec.Command("sw_vers", "-productVersion").Output()
	kernel, _ := unix.Sysctl("kern.osrelease")
	n.Status.NodeInfo = v1.NodeSystemInfo{
		OperatingSystem:         "darwin",
		Architecture:            runtime.GOARCH,
		OSImage:                 "macOS " + strings.TrimSpace(string(osVersion)),
		KernelVersion:           kernel,
		KubeletVersion:          kubeletVersion,
		ContainerRuntimeVersion: runtimeVersion,
	}
}

// detectNodeIP returns the local address used to reach the API server.
func detectNodeIP(kubeconfig string) (string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = kubeconfig
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		return "", err
	}
	u, err := url.Parse(cfg.Host)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}
	conn, err := net.Dial("udp", net.JoinHostPort(host, port))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

// selfSignedCert generates an in-memory serving cert. The API server does not
// verify kubelet serving certs unless --kubelet-certificate-authority is set.
func selfSignedCert(nodeName, ip string) func(*tls.Config) error {
	return func(cfg *tls.Config) error {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(time.Now().UnixNano()),
			Subject:      pkix.Name{CommonName: "system:node:" + nodeName},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().AddDate(1, 0, 0),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			DNSNames:     []string{nodeName},
		}
		if parsed := net.ParseIP(ip); parsed != nil {
			tmpl.IPAddresses = []net.IP{parsed}
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		if err != nil {
			return err
		}
		cfg.Certificates = append(cfg.Certificates, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key})
		return nil
	}
}
