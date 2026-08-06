package main

import (
	"fmt"
	"os"
	"path/filepath"
)

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
