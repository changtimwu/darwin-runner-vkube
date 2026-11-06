// Command mkimage packages one darwin/arm64 binary as a single-layer OCI image
// and pushes it, for exercising runtimed's OCI pull path.
//
//	mkimage -bin ./metal-probe -ref localhost:5050/metal-probe:p0
package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

func main() {
	bin := flag.String("bin", "", "darwin/arm64 executable to package")
	ref := flag.String("ref", "", "image reference to push")
	flag.Parse()
	if err := run(*bin, *ref); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(bin, ref string) error {
	data, err := os.ReadFile(bin)
	if err != nil {
		return err
	}
	base := filepath.Base(bin)
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "bin/" + base, Mode: 0o755, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
		return err
	}
	if _, err := tw.Write(data); err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf.Bytes())), nil })
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
	cfg.Config.Entrypoint = []string{"/bin/" + base}
	if img, err = mutate.ConfigFile(img, cfg); err != nil {
		return err
	}
	r, err := name.ParseReference(ref)
	if err != nil {
		return err
	}
	if err := remote.Write(r, img); err != nil {
		return err
	}
	d, _ := img.Digest()
	fmt.Printf("pushed %s@%s\n", ref, d)
	return nil
}
