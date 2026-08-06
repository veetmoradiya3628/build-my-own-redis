package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureAOFFilePathUsesConfiguredDirectory(t *testing.T) {
	tempDir := t.TempDir()
	config := ServerConfig{
		dir:           tempDir,
		appenddirname: "blueberry",
		appendfilename: "raspberry.aof",
	}

	path, err := ensureAOFFilePath(config)
	if err != nil {
		t.Fatalf("ensureAOFFilePath returned error: %v", err)
	}

	expectedPath := filepath.Join(tempDir, "blueberry", "raspberry.aof.1.incr.aof")
	if path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, path)
	}

	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("expected directory to be created: %v", err)
	}
}

func TestReadAOFFilePathFromManifest(t *testing.T) {
	tempDir := t.TempDir()
	aofDir := filepath.Join(tempDir, "blueberry")
	if err := os.MkdirAll(aofDir, 0o755); err != nil {
		t.Fatalf("mkdir aof dir: %v", err)
	}

	manifestPath := filepath.Join(aofDir, "raspberry.aof.manifest")
	if err := os.WriteFile(manifestPath, []byte("file custom.aof.1.incr.aof seq 1 type i\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	config := ServerConfig{
		dir:           tempDir,
		appenddirname: "blueberry",
		appendfilename: "raspberry.aof",
	}

	path, err := readAOFFilePathFromManifest(config)
	if err != nil {
		t.Fatalf("readAOFFilePathFromManifest returned error: %v", err)
	}

	expectedPath := filepath.Join(aofDir, "custom.aof.1.incr.aof")
	if path != expectedPath {
		t.Fatalf("expected path %q, got %q", expectedPath, path)
	}
}
