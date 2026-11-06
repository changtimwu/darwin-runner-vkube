# darwin-vk needs cgo (runtimed's darwin SPI) and the pinned k3sm checkouts.
export CGO_ENABLED := 1

.PHONY: build deps test clean
build: deps
	go build -o bin/darwin-vk .
	go build -o bin/mkimage ./cmd/mkimage
	cd third_party/runtimed && go build -o ../../bin/k3sm-execshim ./cmd/k3sm-execshim

deps:
	@test -d third_party/runtimed/.git -a -d third_party/apis/.git || hack/fetch-deps.sh

test: deps
	go test -race ./ ./internal/...

clean:
	rm -rf bin
