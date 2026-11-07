package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTraversablePodsDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "runtime", "pods")
	if err := os.MkdirAll(dir, 0o750); err != nil { // as runtimed's vm path leaves it
		t.Fatal(err)
	}
	if err := traversablePodsDir(dir); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(dir); fi.Mode().Perm() != 0o755 {
		t.Errorf("mode = %o, want 755", fi.Mode().Perm())
	}
	fresh := filepath.Join(t.TempDir(), "pods")
	if err := traversablePodsDir(fresh); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(fresh); fi.Mode().Perm() != 0o755 {
		t.Errorf("fresh mode = %o, want 755", fi.Mode().Perm())
	}
}
