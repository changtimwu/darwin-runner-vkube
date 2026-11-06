// Command mkimage packages darwin/arm64 executables as a single-layer OCI
// image and pushes it, so a Mac node can pull it like any other image.
//
//	mkimage -ref registry.example.com/team/tool:1 ./tool [./helper ...]
//
// Each file lands in /bin of the image; the first one is the entrypoint. A
// pod's command overrides it as usual. Registry credentials come from
// `docker login` (~/.docker/config.json).
package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

func main() {
	ref := flag.String("ref", "", "image reference to push, e.g. registry.example.com/team/tool:1")
	insecure := flag.Bool("insecure", false, "push over plain HTTP (loopback and RFC 1918 registries already are)")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: mkimage -ref <image> [-insecure] <executable> [more files...]\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if *ref == "" || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*ref, *insecure, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "mkimage:", err)
		os.Exit(1)
	}
}

func run(ref string, insecure bool, files []string) error {
	var opts []name.Option
	if insecure {
		opts = append(opts, name.Insecure)
	}
	r, err := name.ParseReference(ref, opts...)
	if err != nil {
		return err
	}
	layer, err := binLayer(files)
	if err != nil {
		return err
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return err
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return err
	}
	cfg = cfg.DeepCopy()
	cfg.OS, cfg.Architecture = "darwin", "arm64"
	cfg.Config.Entrypoint = []string{"/bin/" + filepath.Base(files[0])}
	if img, err = mutate.ConfigFile(img, cfg); err != nil {
		return err
	}
	if err := remote.Write(r, img, remote.WithAuthFromKeychain(authn.DefaultKeychain)); err != nil {
		return err
	}
	d, err := img.Digest()
	if err != nil {
		return err
	}
	fmt.Printf("pushed %s@%s\n", r, d)
	return nil
}

// binLayer tars files into /bin, executable.
func binLayer(files []string) (v1.Layer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "bin/", Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			return nil, err
		}
		base := filepath.Base(f)
		if seen[base] {
			return nil, fmt.Errorf("two files named %s", base)
		}
		seen[base] = true
		if err := tw.WriteHeader(&tar.Header{Name: "bin/" + base, Mode: 0o755, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf.Bytes())), nil })
}
