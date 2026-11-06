package main

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"k3sm.io/runtimed/pkg/shadowset"
)

// installShadowBin writes ad-hoc re-signed copies of the host shells and
// coreutils into dir. They are not platform binaries, so dyld keeps the
// path-rebase shim loaded in them and pods running /bin/sh see their volume
// mounts. runtimed uses a copy only if dir and file are root-owned and not
// group- or world-writable, so this must run as root.
func installShadowBin(dir string) error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("install-shadow-bin must run as root: runtimed only trusts root-owned copies")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		return err
	}
	for _, e := range shadowset.Entries() {
		dst := filepath.Join(dir, e.Copy)
		if err := copyFile(e.Source, dst); err != nil {
			if os.IsNotExist(err) {
				continue // not on this macOS release; the host binary runs instead
			}
			return err
		}
		if out, err := exec.Command("/usr/bin/codesign", "--force", "--sign", "-", dst).CombinedOutput(); err != nil {
			return fmt.Errorf("codesign %s: %v: %s", dst, err, out)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
