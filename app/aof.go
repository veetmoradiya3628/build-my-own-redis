package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func ensureAOFManifest(config ServerConfig, aofFilePath string) (string, error) {
	if config.appendfilename == "" {
		return "", fmt.Errorf("appendfilename must be provided when appendonly is enabled")
	}

	manifestPath := filepath.Join(filepath.Dir(aofFilePath), config.appendfilename+".manifest")
	manifestContent := fmt.Sprintf("file %s seq 1 type i\n", filepath.Base(aofFilePath))
	if err := os.WriteFile(manifestPath, []byte(manifestContent), 0o644); err != nil {
		return "", fmt.Errorf("write append-only manifest: %w", err)
	}

	return manifestPath, nil
}

func ensureAOFDirectory(config ServerConfig) (string, error) {
	aofDirPath := filepath.Join(config.dir, config.appenddirname)
	if err := os.MkdirAll(aofDirPath, 0o755); err != nil {
		return "", fmt.Errorf("create append-only directory: %w", err)
	}
	return aofDirPath, nil
}

func ensureAOFFilePath(config ServerConfig) (string, error) {
	if config.appendfilename == "" {
		return "", fmt.Errorf("appendfilename must be provided when appendonly is enabled")
	}

	aofDirPath, err := ensureAOFDirectory(config)
	if err != nil {
		return "", err
	}

	aofFilePath := filepath.Join(aofDirPath, config.appendfilename+".1.incr.aof")
	if err := os.MkdirAll(filepath.Dir(aofFilePath), 0o755); err != nil {
		return "", fmt.Errorf("create append-only file directory: %w", err)
	}

	return aofFilePath, nil
}
