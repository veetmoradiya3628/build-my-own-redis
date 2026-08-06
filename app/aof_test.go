package main

import (
	"os"
	"path/filepath"
	"strings"
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

func TestEnsureAOFStatePreservesExistingManifest(t *testing.T) {
	tempDir := t.TempDir()
	aofDir := filepath.Join(tempDir, "blueberry")
	if err := os.MkdirAll(aofDir, 0o755); err != nil {
		t.Fatalf("mkdir aof dir: %v", err)
	}

	manifestPath := filepath.Join(aofDir, "raspberry.aof.manifest")
	customManifest := "file strawberry.aof.1.incr.aof seq 1 type i\n"
	if err := os.WriteFile(manifestPath, []byte(customManifest), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	customAOFPath := filepath.Join(aofDir, "strawberry.aof.1.incr.aof")
	if err := os.WriteFile(customAOFPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write custom aof file: %v", err)
	}

	config := ServerConfig{
		dir:           tempDir,
		appenddirname: "blueberry",
		appendfilename: "raspberry.aof",
		appendonly:    "yes",
	}

	aofPath, manifest, err := ensureAOFState(config)
	if err != nil {
		t.Fatalf("ensureAOFState returned error: %v", err)
	}

	if aofPath != customAOFPath {
		t.Fatalf("expected existing AOF path %q, got %q", customAOFPath, aofPath)
	}
	if manifest != manifestPath {
		t.Fatalf("expected manifest path %q, got %q", manifestPath, manifest)
	}

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	if string(data) != customManifest {
		t.Fatalf("expected manifest content %q, got %q", customManifest, string(data))
	}
}

func TestReplayAOFCommands(t *testing.T) {
	tempDir := t.TempDir()
	aofDir := filepath.Join(tempDir, "blueberry")
	if err := os.MkdirAll(aofDir, 0o755); err != nil {
		t.Fatalf("mkdir aof dir: %v", err)
	}

	manifestPath := filepath.Join(aofDir, "raspberry.aof.manifest")
	if err := os.WriteFile(manifestPath, []byte("file custom.aof.1.incr.aof seq 1 type i\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	aofPath := filepath.Join(aofDir, "custom.aof.1.incr.aof")
	payload := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n"
	if err := os.WriteFile(aofPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("write aof file: %v", err)
	}

	config := ServerConfig{
		dir:           tempDir,
		appenddirname: "blueberry",
		appendfilename: "raspberry.aof",
		appendonly:    "yes",
	}

	store := NewStore(nil, nil)
	if err := replayAOFCommands(config, store); err != nil {
		t.Fatalf("replayAOFCommands returned error: %v", err)
	}

	value, ok := store.Get("foo")
	if !ok {
		t.Fatalf("expected replayed key to exist")
	}
	if value != "100" {
		t.Fatalf("expected replayed value %q, got %v", "100", value)
	}
}

func TestAppendRESPCommandToAOFAppendsMultipleCommands(t *testing.T) {
	tempDir := t.TempDir()
	aofDir := filepath.Join(tempDir, "blueberry")
	if err := os.MkdirAll(aofDir, 0o755); err != nil {
		t.Fatalf("mkdir aof dir: %v", err)
	}

	manifestPath := filepath.Join(aofDir, "raspberry.aof.manifest")
	if err := os.WriteFile(manifestPath, []byte("file custom.aof.1.incr.aof seq 1 type i\n"), 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	aofPath := filepath.Join(aofDir, "custom.aof.1.incr.aof")
	if err := os.WriteFile(aofPath, []byte{}, 0o644); err != nil {
		t.Fatalf("write empty aof file: %v", err)
	}

	config := ServerConfig{
		dir:           tempDir,
		appenddirname: "blueberry",
		appendfilename: "raspberry.aof",
		appendonly:    "yes",
		appendfsync:   "always",
	}

	first := []any{"SET", "foo", "100"}
	second := []any{"SET", "bar", "200"}
	if err := appendRESPCommandToAOF(config, first); err != nil {
		t.Fatalf("append first command: %v", err)
	}
	if err := appendRESPCommandToAOF(config, second); err != nil {
		t.Fatalf("append second command: %v", err)
	}

	data, err := os.ReadFile(aofPath)
	if err != nil {
		t.Fatalf("read aof file: %v", err)
	}

	content := string(data)
	firstEncoded := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n"
	secondEncoded := "*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n200\r\n"
	if !strings.Contains(content, firstEncoded) {
		t.Fatalf("expected first command in AOF file, got %q", content)
	}
	if !strings.Contains(content, secondEncoded) {
		t.Fatalf("expected second command in AOF file, got %q", content)
	}
	if strings.Index(content, firstEncoded) > strings.Index(content, secondEncoded) {
		t.Fatalf("expected commands to be appended in order, got %q", content)
	}
}
