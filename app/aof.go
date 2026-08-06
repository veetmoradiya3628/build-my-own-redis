package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func replayAOFCommands(config ServerConfig, store *Store) error {
	if config.appendonly != "yes" {
		return nil
	}

	aofFilePath, err := readAOFFilePathFromManifest(config)
	if err != nil {
		return err
	}

	file, err := os.Open(aofFilePath)
	if err != nil {
		return fmt.Errorf("open append-only file for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		val, err := ParseRESP(reader)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("parse append-only file: %w", err)
		}

		arr, ok := val.([]any)
		if !ok || len(arr) == 0 {
			continue
		}

		handleCommand(nil, arr, store, config)
	}

	return nil
}

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

func readAOFFilePathFromManifest(config ServerConfig) (string, error) {
	if config.appendfilename == "" {
		return "", fmt.Errorf("appendfilename must be provided when appendonly is enabled")
	}

	manifestPath := filepath.Join(config.dir, config.appenddirname, config.appendfilename+".manifest")
	file, err := os.Open(manifestPath)
	if err != nil {
		return "", fmt.Errorf("open append-only manifest: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 6 && parts[0] == "file" && parts[2] == "seq" && parts[4] == "type" {
			filename := parts[1]
			if filename == "" {
				return "", fmt.Errorf("manifest entry is missing file name")
			}
			return filepath.Join(config.dir, config.appenddirname, filename), nil
		}
	}

	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("read append-only manifest: %w", err)
	}

	return "", fmt.Errorf("no active AOF file found in manifest")
}

func appendRESPCommandToAOF(config ServerConfig, arr []any) error {
	aofFilePath, err := readAOFFilePathFromManifest(config)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(aofFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open append-only file: %w", err)
	}
	defer file.Close()

	payload := encodeRESPArray(arr)
	if _, err := file.Write(payload); err != nil {
		return fmt.Errorf("append to append-only file: %w", err)
	}

	if config.appendfsync == "always" {
		if err := file.Sync(); err != nil {
			return fmt.Errorf("fsync append-only file: %w", err)
		}
	}

	return nil
}

func encodeRESPArray(arr []any) []byte {
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(fmt.Sprintf("%d", len(arr)))
	builder.WriteString("\r\n")
	for _, item := range arr {
		switch v := item.(type) {
		case string:
			builder.WriteString("$")
			builder.WriteString(fmt.Sprintf("%d", len(v)))
			builder.WriteString("\r\n")
			builder.WriteString(v)
			builder.WriteString("\r\n")
		case []any:
			builder.WriteString(string(encodeRESPArray(v)))
		case nil:
			builder.WriteString("$-1\r\n")
		default:
			text := fmt.Sprintf("%v", v)
			builder.WriteString("$")
			builder.WriteString(fmt.Sprintf("%d", len(text)))
			builder.WriteString("\r\n")
			builder.WriteString(text)
			builder.WriteString("\r\n")
		}
	}
	return []byte(builder.String())
}
