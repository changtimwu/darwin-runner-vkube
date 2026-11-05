# darwin-runner-vkube

A minimal [virtual-kubelet](https://github.com/virtual-kubelet/virtual-kubelet) provider that joins a Mac to an existing Kubernetes cluster as a worker node. Pods scheduled onto it run their containers as **native macOS processes**. That's handy for CI runners, Xcode builds, and anything else that needs real macOS.

## How it works

| Kubernetes concept | On the Mac |
|---|---|
| Node | Registered as `kubernetes.io/os=darwin`, `kubernetes.io/arch=<arm64/amd64>`, with real CPU and memory capacity |
| Container | `command` + `args` run as a host process in its own process group. **`image` is ignored.** |
| Env | `env` values, including ones from configMap/secret/fieldRef, which virtual-kubelet resolves |
| Init containers | Run one after another before the main containers |
| `restartPolicy` | `Always` / `OnFailure` / `Never`, with exponential backoff from 10s up to 5m |
| Pod IP | The node IP (host network) |
| Logs | stdout+stderr go to `~/.darwin-vk/pods/<ns>/<pod>_<uid>/logs/<ctr>.log`. Supports `kubectl logs` with `-f` and `--tail` |
| `kubectl exec` | Runs the command on the host in the container's working dir and env (TTY supported) |
| `kubectl port-forward` | Forwards to `127.0.0.1:<port>` |
| Delete | SIGTERM to the process group, then SIGKILL after `terminationGracePeriodSeconds` |

The node is tainted with `virtual-kubelet.io/provider=darwin:NoSchedule`, so only pods that tolerate the taint get scheduled there. Linux DaemonSets such as kube-proxy and CNI stay off it because of their `os=linux` selectors.

## Not supported (it's minimal)

There are no container images, filesystem isolation, volumes/volumeMounts, probes, resource limits, or `kubectl attach`. Stats are empty. Pod networking is the host network: two pods that bind the same port will conflict. Processes run as the user that runs `darwin-vk`. If `darwin-vk` crashes, its child processes keep running and aren't adopted when it restarts.

## Setup

**1. On the cluster (admin kubeconfig):**

```sh
kubectl apply -f deploy/rbac.yaml
./deploy/make-kubeconfig.sh            # -> darwin-vk.kubeconfig, cluster-ca.crt
```

Copy both files to the Mac.

**2. On the Mac:**

```sh
go build -o bin/darwin-vk .
./bin/darwin-vk --kubeconfig darwin-vk.kubeconfig --client-ca cluster-ca.crt
```

The apiserver must be able to reach the Mac on TCP 10250 for `logs`, `exec`, and `port-forward`.

**3. Try it:**

```sh
kubectl get nodes -o wide
kubectl apply -f examples/hello.yaml
kubectl logs -f hello-mac
kubectl exec -it hello-mac -- zsh
```

## Flags

| Flag | Default | |
|---|---|---|
| `--nodename` | hostname | Node name |
| `--kubeconfig` | `$KUBECONFIG` | Cluster credentials |
| `--node-ip` | auto-detected | IP advertised for the node and its pods |
| `--client-ca` | | Cluster CA. Turns on mTLS and webhook authn/authz for the kubelet API (port 10250) |
| `--tls-cert` / `--tls-key` | self-signed | Kubelet API serving cert |
| `--insecure-no-auth` | false | Serve the kubelet API without auth. **Gives anyone who can reach the port shell access to the Mac.** |
| `--listen` | `:10250` | Kubelet API address |
| `--data-dir` | `~/.darwin-vk` | Pod working dirs and logs |
| `--max-pods` | 20 | Advertised pod capacity |
| `--taint` | true | Add the NoSchedule taint |

If neither `--client-ca` nor `--insecure-no-auth` is set, the kubelet API stays off. Pods still run, but `logs`, `exec`, and `port-forward` are unavailable.

The `client-ca` mode follows how kubelets normally work: the apiserver presents its kubelet client certificate (signed by the cluster CA), and access is authorized with a SubjectAccessReview on `nodes/proxy` and related resources. This assumes the apiserver's `--kubelet-client-certificate` is signed by the CA you pass.

## Tests

```sh
go test -race ./...
```
