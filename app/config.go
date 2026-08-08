package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func loadConfigFromFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	values := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key != "" {
			values[key] = val
		}
	}
	return values, nil
}

func applyConfigFileValues(config *ServerConfig, values map[string]string) error {
	if len(values) == 0 {
		return nil
	}

	if v, ok := values["dir"]; ok && v != "" {
		config.dir = v
	}
	if v, ok := values["dbfilename"]; ok && v != "" {
		config.dbfilename = v
	}
	if v, ok := values["appendonly"]; ok && v != "" {
		config.appendonly = v
	}
	if v, ok := values["appenddirname"]; ok && v != "" {
		config.appenddirname = v
	}
	if v, ok := values["appendfilename"]; ok && v != "" {
		config.appendfilename = v
	}
	if v, ok := values["appendfsync"]; ok && v != "" {
		config.appendfsync = v
	}
	if v, ok := values["requirepass"]; ok && v != "" {
		config.requirepass = v
	}
	if v, ok := values["port"]; ok && v != "" {
		config.port = v
	}
	if v, ok := values["replicaof"]; ok && v != "" {
		config.replicaof = v
	}
	if v, ok := values["role"]; ok && v != "" {
		config.role = v
	}
	if v, ok := values["master_repl_id"]; ok && v != "" {
		config.masterReplID = v
	}
	if v, ok := values["master_repl_offset"]; ok && v != "" {
		offset, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid master_repl_offset: %w", err)
		}
		config.masterReplOffset = offset
	}
	return nil
}

func resolveConfigPath(configPath string) string {
	if configPath == "" {
		return ""
	}
	if filepath.IsAbs(configPath) {
		return configPath
	}
	return filepath.Join(".", configPath)
}
