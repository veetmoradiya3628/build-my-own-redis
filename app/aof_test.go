package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestEnsureAOFFilePathUsesConfiguredDirectory(t *testing.T) {
	tempDir := t.TempDir()
	config := ServerConfig{
		dir:          tempDir,
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
