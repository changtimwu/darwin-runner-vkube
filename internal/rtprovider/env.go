package rtprovider

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runtimev1 "k3sm.io/apis/runtime/v1"
)

// The provider resolves container environments itself, as the kubelet does
// once the pod sandbox exists, instead of letting virtual-kubelet do it before
// CreatePod: only then is the pod IP known, so status.podIP can be filled in.

// envError is an environment the pod cannot have yet, such as one naming a
// ConfigMap that does not exist. The kubelet reports it as
// CreateContainerConfigError and retries until the source appears.
type envError struct{ err error }

func (e envError) Error() string { return e.err.Error() }
func (e envError) Unwrap() error { return e.err }

type lenientEnvKey struct{}

// withLenientEnv makes env resolution skip sources it cannot read rather
// than fail. Re-attach uses it: a running pod is not killed because its
// ConfigMap has since gone.
func withLenientEnv(ctx context.Context) context.Context {
	return context.WithValue(ctx, lenientEnvKey{}, true)
}

// setEnv resolves the env of every container in box from the pod spec.
func (p *Provider) setEnv(ctx context.Context, pod *corev1.Pod, podIP string, box *runtimev1.PodBox) error {
	svcEnv, err := p.serviceEnv(ctx, pod)
	if err != nil {
		return envError{fmt.Errorf("list services: %w", err)}
	}
	set := func(specs []corev1.Container, rcs []*runtimev1.Container) error {
		for i := range specs {
			env, err := p.containerEnv(ctx, pod, &specs[i], podIP, svcEnv)
			if err != nil {
				return fmt.Errorf("container %s: %w", specs[i].Name, err)
			}
			rcs[i].Env = env
		}
		return nil
	}
	if err := set(pod.Spec.InitContainers, box.InitContainers); err != nil {
		return err
	}
	return set(pod.Spec.Containers, box.Containers)
}

// containerEnv follows the kubelet's makeEnvironmentVariables: envFrom, then
// env with valueFrom resolved and $(VAR) expanded, then Service links for
// names still unset. The result is sorted by name.
func (p *Provider) containerEnv(ctx context.Context, pod *corev1.Pod, c *corev1.Container, podIP string, svcEnv map[string]string) ([]*runtimev1.EnvVar, error) {
	lenient, _ := ctx.Value(lenientEnvKey{}).(bool)
	env := map[string]string{}
	var invalid []string
	for _, from := range c.EnvFrom {
		var data map[string]string
		switch {
		case from.ConfigMapRef != nil:
			cm, err := p.configMap(ctx, pod.Namespace, from.ConfigMapRef.Name)
			if err != nil {
				if skip(err, from.ConfigMapRef.Optional, lenient) {
					continue
				}
				return nil, err
			}
			data = cm.Data
		case from.SecretRef != nil:
			sec, err := p.secret(ctx, pod.Namespace, from.SecretRef.Name)
			if err != nil {
				if skip(err, from.SecretRef.Optional, lenient) {
					continue
				}
				return nil, err
			}
			data = map[string]string{}
			for k, v := range sec.Data {
				data[k] = string(v)
			}
		}
		for k, v := range data {
			k = from.Prefix + k
			if !validEnvName(k) {
				invalid = append(invalid, k)
				continue
			}
			env[k] = v
		}
	}
	if len(invalid) > 0 {
		slices.Sort(invalid)
		p.event(pod, corev1.EventTypeWarning, "InvalidEnvironmentVariableNames",
			"Keys [%s] from the EnvFrom of container %s were skipped since they are invalid environment variable names.", strings.Join(invalid, ", "), c.Name)
	}

	lookup := func(name string) (string, bool) {
		if v, ok := env[name]; ok {
			return v, true
		}
		v, ok := svcEnv[name]
		return v, ok
	}
	for _, e := range c.Env {
		if e.Value != "" || e.ValueFrom == nil {
			env[e.Name] = expand(e.Value, lookup)
			continue
		}
		v, ok, err := p.valueFrom(ctx, pod, c, e.ValueFrom, podIP)
		if err != nil {
			if lenient {
				continue
			}
			return nil, fmt.Errorf("env %s: %w", e.Name, err)
		}
		if ok {
			env[e.Name] = v
		}
	}
	for k, v := range svcEnv {
		if _, ok := env[k]; !ok {
			env[k] = v
		}
	}

	out := make([]*runtimev1.EnvVar, 0, len(env))
	for _, k := range slices.Sorted(maps.Keys(env)) {
		out = append(out, &runtimev1.EnvVar{Name: k, Value: env[k]})
	}
	return out, nil
}

// valueFrom resolves one env source. ok is false when an optional source is
// absent, which leaves the variable unset.
func (p *Provider) valueFrom(ctx context.Context, pod *corev1.Pod, c *corev1.Container, src *corev1.EnvVarSource, podIP string) (v string, ok bool, err error) {
	switch {
	case src.FieldRef != nil:
		v, err := p.fieldValue(pod, src.FieldRef.FieldPath, podIP)
		return v, err == nil, err
	case src.ResourceFieldRef != nil:
		v, err := p.resourceValue(ctx, pod, c, src.ResourceFieldRef)
		return v, err == nil, err
	case src.ConfigMapKeyRef != nil:
		ref := src.ConfigMapKeyRef
		cm, err := p.configMap(ctx, pod.Namespace, ref.Name)
		if err != nil {
			return "", false, skipErr(err, ref.Optional)
		}
		if v, ok := cm.Data[ref.Key]; ok {
			return v, true, nil
		}
		return "", false, missingKey(fmt.Errorf("couldn't find key %s in ConfigMap %s/%s", ref.Key, pod.Namespace, ref.Name), ref.Optional)
	case src.SecretKeyRef != nil:
		ref := src.SecretKeyRef
		sec, err := p.secret(ctx, pod.Namespace, ref.Name)
		if err != nil {
			return "", false, skipErr(err, ref.Optional)
		}
		if v, ok := sec.Data[ref.Key]; ok {
			return string(v), true, nil
		}
		return "", false, missingKey(fmt.Errorf("couldn't find key %s in Secret %s/%s", ref.Key, pod.Namespace, ref.Name), ref.Optional)
	}
	return "", false, nil
}

// fieldValue is the downward API for env: the kubelet's
// podFieldSelectorRuntimeValue.
func (p *Provider) fieldValue(pod *corev1.Pod, path, podIP string) (string, error) {
	if sub, ok := strings.CutPrefix(path, "metadata.labels['"); ok && strings.HasSuffix(sub, "']") {
		return pod.Labels[strings.TrimSuffix(sub, "']")], nil
	}
	if sub, ok := strings.CutPrefix(path, "metadata.annotations['"); ok && strings.HasSuffix(sub, "']") {
		return pod.Annotations[strings.TrimSuffix(sub, "']")], nil
	}
	switch path {
	case "metadata.name":
		return pod.Name, nil
	case "metadata.namespace":
		return pod.Namespace, nil
	case "metadata.uid":
		return string(pod.UID), nil
	case "spec.nodeName":
		return pod.Spec.NodeName, nil
	case "spec.serviceAccountName":
		return pod.Spec.ServiceAccountName, nil
	case "status.hostIP", "status.hostIPs":
		return p.cfg.NodeIP, nil
	case "status.podIP", "status.podIPs":
		return podIP, nil
	}
	return "", envError{fmt.Errorf("unsupported fieldPath: %s", path)}
}

// resourceValue is the kubelet's containerResourceRuntimeValue: an unset
// limit reads as the node's allocatable.
func (p *Provider) resourceValue(ctx context.Context, pod *corev1.Pod, c *corev1.Container, ref *corev1.ResourceFieldSelector) (string, error) {
	target := c
	if ref.ContainerName != "" && ref.ContainerName != c.Name {
		target = nil
		for _, cs := range [][]corev1.Container{pod.Spec.Containers, pod.Spec.InitContainers} {
			for i := range cs {
				if cs[i].Name == ref.ContainerName {
					target = &cs[i]
				}
			}
		}
		if target == nil {
			return "", envError{fmt.Errorf("resourceFieldRef: no container %q", ref.ContainerName)}
		}
	}
	kind, name, _ := strings.Cut(ref.Resource, ".")
	rn := corev1.ResourceName(name)
	var q resource.Quantity
	switch kind {
	case "limits":
		var ok bool
		if q, ok = target.Resources.Limits[rn]; !ok {
			alloc, err := p.allocatable(ctx)
			if err != nil {
				return "", err
			}
			q = alloc[rn]
		}
	case "requests":
		q = target.Resources.Requests[rn]
	default:
		return "", envError{fmt.Errorf("unsupported resourceFieldRef %q", ref.Resource)}
	}
	divisor := ref.Divisor
	if divisor.IsZero() {
		divisor = resource.MustParse("1")
	}
	switch rn {
	case corev1.ResourceCPU:
		return strconv.FormatInt(int64(math.Ceil(float64(q.MilliValue())/float64(divisor.MilliValue()))), 10), nil
	case corev1.ResourceMemory, corev1.ResourceEphemeralStorage:
		return strconv.FormatInt(int64(math.Ceil(float64(q.Value())/float64(divisor.Value()))), 10), nil
	}
	return "", envError{fmt.Errorf("unsupported resourceFieldRef %q", ref.Resource)}
}

func (p *Provider) allocatable(ctx context.Context) (corev1.ResourceList, error) {
	if p.cfg.Client == nil {
		return nil, envError{errors.New("node allocatable unknown: no API client")}
	}
	n, err := p.cfg.Client.CoreV1().Nodes().Get(ctx, p.cfg.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, envError{fmt.Errorf("get node allocatable: %w", err)}
	}
	return n.Status.Allocatable, nil
}

func (p *Provider) configMap(ctx context.Context, ns, name string) (*corev1.ConfigMap, error) {
	if p.cfg.Client == nil {
		return nil, envError{fmt.Errorf("configmap %q: no API client", name)}
	}
	cm, err := p.cfg.Client.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, envError{err}
	}
	return cm, nil
}

func (p *Provider) secret(ctx context.Context, ns, name string) (*corev1.Secret, error) {
	if p.cfg.Client == nil {
		return nil, envError{fmt.Errorf("secret %q: no API client", name)}
	}
	sec, err := p.cfg.Client.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, envError{err}
	}
	return sec, nil
}

// skip reports whether an unreadable envFrom source is left out: an optional
// one that does not exist, or any one in lenient mode.
func skip(err error, optional *bool, lenient bool) bool {
	return lenient || (apierrors.IsNotFound(err) && optional != nil && *optional)
}

// skipErr is skip for a single key reference: nil leaves the variable unset.
func skipErr(err error, optional *bool) error {
	if skip(err, optional, false) {
		return nil
	}
	return err
}

func missingKey(err error, optional *bool) error {
	if optional != nil && *optional {
		return nil
	}
	return envError{err}
}

// serviceEnv is the kubelet's getServiceEnvVarMap: the docker-link style
// variables of the pod's namespace's Services (with enableServiceLinks, the
// default) and of the master "kubernetes" Service.
func (p *Provider) serviceEnv(ctx context.Context, pod *corev1.Pod) (map[string]string, error) {
	if p.cfg.Client == nil {
		return nil, nil
	}
	links := pod.Spec.EnableServiceLinks == nil || *pod.Spec.EnableServiceLinks
	byName := map[string]*corev1.Service{}
	if links {
		list, err := p.cfg.Client.CoreV1().Services(pod.Namespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			byName[list.Items[i].Name] = &list.Items[i]
		}
	}
	if _, ok := byName["kubernetes"]; !ok {
		svc, err := p.cfg.Client.CoreV1().Services(metav1.NamespaceDefault).Get(ctx, "kubernetes", metav1.GetOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}
		if err == nil {
			byName["kubernetes"] = svc
		}
	}
	out := map[string]string{}
	for _, svc := range byName {
		serviceLinkEnv(svc, out)
	}
	return out, nil
}

// serviceLinkEnv adds one Service's variables, as the kubelet's
// envvars.FromServices.
func serviceLinkEnv(svc *corev1.Service, out map[string]string) {
	ip := svc.Spec.ClusterIP
	if ip == "" || ip == corev1.ClusterIPNone || len(svc.Spec.Ports) == 0 {
		return
	}
	prefix := strings.ToUpper(strings.ReplaceAll(svc.Name, "-", "_"))
	out[prefix+"_SERVICE_HOST"] = ip
	out[prefix+"_SERVICE_PORT"] = strconv.Itoa(int(svc.Spec.Ports[0].Port))
	for i, sp := range svc.Spec.Ports {
		port := strconv.Itoa(int(sp.Port))
		if sp.Name != "" {
			out[prefix+"_SERVICE_PORT_"+strings.ToUpper(strings.ReplaceAll(sp.Name, "-", "_"))] = port
		}
		proto := strings.ToLower(string(sp.Protocol))
		if proto == "" {
			proto = "tcp"
		}
		url := proto + "://" + net.JoinHostPort(ip, port)
		if i == 0 {
			out[prefix+"_PORT"] = url
		}
		pp := fmt.Sprintf("%s_PORT_%d_%s", prefix, sp.Port, strings.ToUpper(proto))
		out[pp] = url
		out[pp+"_PROTO"] = proto
		out[pp+"_PORT"] = port
		out[pp+"_ADDR"] = ip
	}
}

// expand replaces $(VAR) references with lookup's values, as the kubelet's
// expansion package: $$ escapes a $, and an unknown or unterminated
// reference is kept as written.
func expand(s string, lookup func(string) (string, bool)) string {
	if !strings.Contains(s, "$") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '$' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '$':
			b.WriteByte('$')
			i++
		case '(':
			end := strings.IndexByte(s[i+2:], ')')
			if end < 0 {
				b.WriteString(s[i:])
				return b.String()
			}
			name := s[i+2 : i+2+end]
			if v, ok := lookup(name); ok && name != "" {
				b.WriteString(v)
			} else {
				b.WriteString(s[i : i+3+end])
			}
			i += 2 + end
		default:
			b.WriteByte('$')
		}
	}
	return b.String()
}

// validEnvName is the kubelet's relaxed rule: printable ASCII except '='.
func validEnvName(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < ' ' || r > '~' || r == '=' {
			return false
		}
	}
	return true
}
