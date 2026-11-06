package rtprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"path"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	runtimev1 "k3sm.io/apis/runtime/v1"
	"k3sm.io/runtimed/pkg/image"
)

// secretCredentials resolves imagePullSecrets for runtimed's pull client. It
// reads the Secrets at pull time, so a fixed Secret is picked up by the next
// retry. Errors name only the Secret, never its content: they reach the node
// log.
type secretCredentials struct {
	client kubernetes.Interface
	log    *slog.Logger
}

// dockerAuth is one entry of a docker config's "auths".
type dockerAuth struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Auth          string `json:"auth"`
	IdentityToken string `json:"identitytoken"`
	RegistryToken string `json:"registrytoken"`
}

// PullCredential returns the most specific matching entry across the pod's
// pull Secrets, earlier Secrets winning ties, or ok=false for an anonymous
// pull. Like the kubelet, a missing Secret is skipped rather than fatal.
func (s secretCredentials) PullCredential(ctx context.Context, namespace string, secrets []*runtimev1.LocalObjectReference, ref string) (*image.RegistryCredential, bool, error) {
	r, err := name.ParseReference(ref)
	if err != nil {
		return nil, false, nil // runtimed reports the bad reference itself
	}
	var best *dockerAuth
	bestLen := -1
	for _, lr := range secrets {
		sec, err := s.client.CoreV1().Secrets(namespace).Get(ctx, lr.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			s.log.Warn("imagePullSecret not found; ignoring it", "namespace", namespace, "secret", lr.GetName())
			continue
		}
		if err != nil {
			return nil, false, fmt.Errorf("get secret %s/%s: %w", namespace, lr.GetName(), err)
		}
		auths, err := dockerAuths(sec)
		if err != nil {
			return nil, false, fmt.Errorf("secret %s/%s: %w", namespace, lr.GetName(), err)
		}
		for key, a := range auths {
			if n, ok := matchRegistryKey(key, r.Context()); ok && n > bestLen {
				best, bestLen = &a, n
			}
		}
	}
	if best == nil {
		return nil, false, nil
	}
	return &image.RegistryCredential{
		Username: best.Username, Password: best.Password, Auth: best.Auth,
		IdentityToken: best.IdentityToken, RegistryToken: best.RegistryToken,
	}, true, nil
}

// dockerAuths decodes a kubernetes.io/dockerconfigjson or dockercfg Secret.
func dockerAuths(sec *corev1.Secret) (map[string]dockerAuth, error) {
	switch sec.Type {
	case corev1.SecretTypeDockerConfigJson:
		var cfg struct {
			Auths map[string]dockerAuth `json:"auths"`
		}
		if err := json.Unmarshal(sec.Data[corev1.DockerConfigJsonKey], &cfg); err != nil {
			return nil, fmt.Errorf("%s is not valid JSON", corev1.DockerConfigJsonKey)
		}
		return cfg.Auths, nil
	case corev1.SecretTypeDockercfg:
		var auths map[string]dockerAuth
		if err := json.Unmarshal(sec.Data[corev1.DockerConfigKey], &auths); err != nil {
			return nil, fmt.Errorf("%s is not valid JSON", corev1.DockerConfigKey)
		}
		return auths, nil
	}
	return nil, fmt.Errorf("type %q is not a docker config Secret", sec.Type)
}

// dockerHubKeys are the spellings of Docker Hub a docker config may carry.
var dockerHubKeys = map[string]bool{
	"docker.io": true, "index.docker.io": true, "registry-1.docker.io": true, "registry.hub.docker.com": true,
}

// matchRegistryKey reports whether a docker config key applies to repo, the
// way the kubelet matches them: the key may carry a scheme and a path, its
// host may use "*" per DNS label, a port must match exactly, and its path must
// be a prefix of the repository on a "/" boundary. The returned length ranks
// matches; longer is more specific.
func matchRegistryKey(key string, repo name.Repository) (int, bool) {
	k := key
	if !strings.Contains(k, "://") {
		k = "https://" + k
	}
	u, err := url.Parse(k)
	if err != nil || u.Host == "" {
		return 0, false
	}
	host, keyPath := u.Host, strings.Trim(u.Path, "/")
	if dockerHubKeys[host] {
		host = name.DefaultRegistry
		if keyPath == "v1" || keyPath == "v2" {
			keyPath = ""
		}
	}
	if !hostMatches(host, repo.RegistryStr()) {
		return 0, false
	}
	repoPath := repo.RepositoryStr()
	if keyPath != "" && repoPath != keyPath && !strings.HasPrefix(repoPath, keyPath+"/") {
		return 0, false
	}
	return len(host) + len(keyPath), true
}

// hostMatches compares host[:port] patterns label by label with glob syntax.
func hostMatches(pattern, host string) bool {
	if dockerHubKeys[host] {
		host = name.DefaultRegistry
	}
	pHost, pPort, _ := strings.Cut(pattern, ":")
	hHost, hPort, _ := strings.Cut(host, ":")
	if pPort != hPort {
		return false
	}
	pl, hl := strings.Split(pHost, "."), strings.Split(hHost, ".")
	if len(pl) != len(hl) {
		return false
	}
	for i := range pl {
		if ok, _ := path.Match(pl[i], hl[i]); !ok {
			return false
		}
	}
	return true
}
