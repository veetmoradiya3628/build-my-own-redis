package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ServerConfig struct {
	dir              string
	dbfilename       string
	appendonly       string
	appenddirname    string
	appendfilename   string
	appendfsync      string
	port             string
	role             string
	replicaof        string
	masterReplID     string
	masterReplOffset int64
}

func parseReplicaTarget(replicaof string) (string, string, error) {
	replicaof = strings.TrimSpace(replicaof)
	if replicaof == "" {
		return "", "", fmt.Errorf("replicaof is empty")
	}

	if strings.Contains(replicaof, " ") {
		parts := strings.Fields(replicaof)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid replicaof target %q", replicaof)
		}
		return parts[0], parts[1], nil
	}

	if host, port, err := net.SplitHostPort(replicaof); err == nil {
		return host, port, nil
	}

	return "", "", fmt.Errorf("invalid replicaof target %q", replicaof)
}

func performReplicaHandshake(host, port string) error {
	if host == "" || port == "" {
		return fmt.Errorf("invalid replica target")
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	return err
}

func main() {
	slog.Info("Logs from your program will appear here!")

	cwd, err := os.Getwd()
	if err != nil {
		slog.Error("failed to get current working directory", "err", err)
		os.Exit(1)
	}

	dirFlag := flag.String("dir", cwd, "Directory where RDB files are stored")
	dbFlag := flag.String("dbfilename", "", "Name of the RDB file name")
	appendonlyFlag := flag.String("appendonly", "no", "Enable append-only mode")
	appenddirnameFlag := flag.String("appenddirname", "appendonlydir", "Directory where AOF files are stored")
	appendfilenameFlag := flag.String("appendfilename", "appendonly.aof", "Name of the AOF file")
	appendfsyncFlag := flag.String("appendfsync", "everysec", "AOF fsync policy")
	portFlag := flag.String("port", "6379", "Port to listen on")
	replicaof := flag.String("replicaof", "", "Address of the master server to replicate from (host:port)")
	flag.Parse()

	role := "master"
	if *replicaof != "" {
		role = "slave"
	}

	config := ServerConfig{
		dir:            *dirFlag,
		dbfilename:     *dbFlag,
		appendonly:     *appendonlyFlag,
		appenddirname:  *appenddirnameFlag,
		appendfilename: *appendfilenameFlag,
		appendfsync:    *appendfsyncFlag,
		port:           *portFlag,
		role:           role,
		replicaof:      *replicaof,

		masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb", // This is a placeholder for the master replication ID. In a real-world scenario, this would be dynamically generated or retrieved from the master server.
		masterReplOffset: 0,                                          // This is a placeholder for the master replication offset. In a real-world scenario, this would be dynamically updated based on the replication state.
	}
	slog.Debug("Starting server", "config", config)

	// 1. Check if both flags are provided
	var initialData map[string]any
	var initialExpiry map[string]time.Time

	if config.dir != "" && config.dbfilename != "" {
		rdbPath := filepath.Join(config.dir, config.dbfilename)
		slog.Info("Loading RDB file", "path", rdbPath)

		// Load both maps
		initialData, initialExpiry = LoadRDB(rdbPath)
		slog.Info("RDB load summary", "keys_loaded", len(initialData), "expiry_entries", len(initialExpiry))
	} else {
		initialData = make(map[string]any)
		initialExpiry = make(map[string]time.Time)
	}

	// 2. Check if appendonly is enabled create the <dir>/<appenddirname> directory if it doesn't exist
	if config.appendonly == "yes" {
		aofFilePath, manifestPath, err := ensureAOFState(config)
		if err != nil {
			slog.Error("failed to prepare append-only file", "err", err)
			os.Exit(1)
		}

		aofDirPath := filepath.Join(config.dir, config.appenddirname)
		slog.Info("Append-only directory ensured", "path", aofDirPath)
		slog.Info("Append-only mode enabled", "file", aofFilePath, "manifest", manifestPath)
	} else {
		slog.Info("Append-only mode disabled")
	}

	// Update NewStore call
	store := NewStore(initialData, initialExpiry)

	if config.appendonly == "yes" {
		if err := replayAOFCommands(config, store); err != nil {
			slog.Error("failed to replay append-only file", "err", err)
			os.Exit(1)
		}
	}

	if config.role == "slave" {
		host, port, err := parseReplicaTarget(config.replicaof)
		if err != nil {
			slog.Error("invalid replicaof target", "replicaof", config.replicaof, "err", err)
			os.Exit(1)
		}
		if err := performReplicaHandshake(host, port); err != nil {
			slog.Error("failed to perform replica handshake", "host", host, "port", port, "err", err)
			os.Exit(1)
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+config.port)
	if err != nil {
		slog.Error("failed to bind to port "+config.port, "err", err)
		os.Exit(1)
	}
	defer l.Close()

	slog.Info("listening connection on port : " + config.port)

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("error accepting connection", "err", err)
			continue
		}

		slog.Debug("accepted connection", "remote", conn.RemoteAddr())
		// Pass the shared store to the handler
		go handleConnection(conn, store, config)
	}
}
