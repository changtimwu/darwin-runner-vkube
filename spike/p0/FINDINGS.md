# P0 spike: k3sm runtimed in-process, no Kubernetes

Part of #1. Host: MacBook Pro M1 Max (32 GiB), macOS 26.5.1, Go 1.26.1. Run as an ordinary user (uid 501), not root. Pins: runtimed `32fe2ec`, apis `f6ce8a4`.

## Reproduce

```sh
./fetch-deps.sh                                        # pinned runtimed + apis checkouts
CGO_ENABLED=1 go build -o bin/p0 .
(cd third_party/runtimed && CGO_ENABLED=1 go build -o ../../bin/k3sm-execshim ./cmd/k3sm-execshim)
./bin/p0                                               # runtime info + native pod

# OCI + GPU
swiftc -O -o bin/metal-probe testimage/metal-probe/main.swift
go build -o bin/mkimage ./testimage/mkimage
docker run -d --rm --name p0-registry -p 127.0.0.1:5050:5000 registry:2
./bin/mkimage -bin bin/metal-probe -ref localhost:5050/metal-probe:p0
./bin/p0 -image localhost:5050/metal-probe:p0 -gpu=false   # expect exit 2: no Metal device
./bin/p0 -image localhost:5050/metal-probe:p0 -gpu         # expect GPU vector add OK
```

## Results

| Check | Result |
|---|---|
| Build runtimed + apis with CGO=1 on macOS 26 / arm64 | ✅ after a workaround (see finding 1) |
| `runtime.New` as **non-root** (root under `$HOME`) | ✅ Backend `seatbelt-execshim`. `k3sm-execshim` must sit beside the binary. |
| `GetRuntimeInfo` | ✅ healthy. GPU facts: `Apple M1 Max`, family `M1`, mem 32 GiB, recommendedMaxWorkingSet 26.8 GB, `metalAvailable` and `sandboxGpuSupported` both true. Rosetta (host) available. **vm backend unavailable** (binary not entitled for Virtualization.framework). |
| `image: native` pod (`/bin/sh -c …`) | ✅ CreatePod ≈0.7 s cold / ≈0.1 s warm. Running → Succeeded. CRI log at `ContainerStatus.log_path`. |
| Seatbelt confinement | ✅ pod dir writable. `ls /Users/timwu` → *Operation not permitted*. `touch /tmp/x` → *Operation not permitted*. |
| OCI pull of a `darwin/arm64` image from `localhost:5050` (plain HTTP) | ✅ pulled in ~50 ms, entrypoint resolved inside the image root, `imageId` digest-pinned. |
| GPU **denied** without `allow_gpu` | ✅ `MTLCreateSystemDefaultDevice()` returns nil inside the sandbox, so the pod exits 2. |
| GPU **granted** with `allow_gpu` | ✅ Metal kernel compiled from source and a 1M-float vector add verified on the GPU, inside the sandbox. |
| SIP/TCC prompts | None appeared. |

## Findings that change the plan

1. **runtimed can't be fetched as a Go module.** `internal/execshim/nul.go` makes the module zip invalid ("nul" is reserved on Windows), so `go get`/`replace => github.com/...` fails. We have to build from pinned source checkouts (`fetch-deps.sh` + `replace => ./third_party/...`), or vendor them as git submodules. It's worth reporting upstream (rename the file).
2. **Root isn't required for a working, confined node.** Non-root gives Seatbelt confinement, OCI and GPU, but every pod shares the daemon's uid. Root adds per-pod uid/gid. That makes non-root a viable dev mode and root/LaunchDaemon the production mode.
3. **The node owns the pod log tree.** runtimed requires `log_directory` and derives `restartCount` and the `N.log` index from files already there. P1 must create `<podLogs>/<ns>_<name>_<uid>/<ctr>/` before CreatePod and remove it after delete. Logs are read from disk (`GetLogs` RPC is `Unimplemented` by design).
4. **`data_volume_path` must be exactly `<root>/pods/<pod_id>` or `…/rootfs`.** The pod ID must match `^[a-z0-9][a-z0-9._-]{0,127}$`. A k8s UID qualifies.
5. **The vm (Linux image) path needs a signed binary with the `com.apple.security.virtualization` entitlement** plus guest artifacts. It stays in P5.
6. Image `Entrypoint` paths resolve inside the image root (a leading `/` means the image root), so ordinary OCI images with `/bin/<app>` work unchanged. A cosmetic `/private/var/select/sh` denial appears on stderr when `/bin/sh` resolves its selector; it's harmless.

## Not yet covered (moves to P1)
- `WatchPodStatus` streaming, exec, port-forward, `ListPodStats`, restart re-attach after the daemon restarts.
- Registry auth (`imagePullSecrets`) and HTTPS registries.
- Running under launchd as root.
