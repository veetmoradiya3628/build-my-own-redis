package main

import (
	"flag"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

type ServerConfig struct {
	dir            string
	dbfilename     string
	appendonly     string
	appenddirname  string
	appendfilename string
	appendfsync    string
	port           string
	role           string
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
	flag.Parse()

	role := "master"

	config := ServerConfig{
		dir:            *dirFlag,
		dbfilename:     *dbFlag,
		appendonly:     *appendonlyFlag,
		appenddirname:  *appenddirnameFlag,
		appendfilename: *appendfilenameFlag,
		appendfsync:    *appendfsyncFlag,
		port:           *portFlag,
		role:           role,
	}
	slog.Debug("Starting server", "dir", config.dir, "dbfilename", config.dbfilename, "appendonly", config.appendonly, "appenddirname", config.appenddirname, "appendfilename", config.appendfilename, "appendfsync", config.appendfsync, "port", config.port, "role", config.role)

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
