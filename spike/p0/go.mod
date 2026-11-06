module github.com/changtimwu/darwin-runner-vkube/spike/p0

go 1.26.1

// k3sm.io modules are unpublished and runtimed cannot be zipped by the module
// proxy; build against the pinned checkouts that fetch-deps.sh creates.
replace (
	k3sm.io/apis => ../../third_party/apis
	k3sm.io/runtimed => ../../third_party/runtimed
)

require (
	google.golang.org/protobuf v1.36.12
	k3sm.io/apis v0.0.0-00010101000000-000000000000
	k3sm.io/runtimed v0.0.0-00010101000000-000000000000
)

require (
	github.com/docker/cli v29.4.3+incompatible // indirect
	github.com/docker/docker-credential-helpers v0.9.3 // indirect
	github.com/google/go-containerregistry v0.21.6 // indirect
	github.com/klauspost/compress v1.18.7 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.1.1 // indirect
	github.com/sirupsen/logrus v1.9.4 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260618152121-87f3d3e198d3 // indirect
	google.golang.org/grpc v1.83.2 // indirect
)
