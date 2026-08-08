package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ServerConfig struct {
	dir            string
	dbfilename     string
	appendonly     string
	appenddirname  string
	appendfilename string
	appendfsync    string
	requirepass    string

	port             string
	role             string
	replicaof        string
	masterReplID     string
	masterReplOffset int64
}

// Global or Manager state for Replicas
type ReplicationManager struct {
	mu             sync.Mutex
	replicas       []net.Conn
	masterOffset   int
	replicaOffsets map[net.Conn]int
}

var globalReplManager = &ReplicationManager{
	replicas:       make([]net.Conn, 0),
	replicaOffsets: make(map[net.Conn]int),
}

func (rm *ReplicationManager) addReplica(conn net.Conn) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.replicas = append(rm.replicas, conn)
}

func (rm *ReplicationManager) removeReplica(conn net.Conn) {
	if conn == nil {
		return
	}
	rm.mu.Lock()
	defer rm.mu.Unlock()
	for i, c := range rm.replicas {
		if c == conn {
			// Remove connection from the slice
			rm.replicas = append(rm.replicas[:i], rm.replicas[i+1:]...)
			break
		}
	}
	delete(rm.replicaOffsets, conn)
}

// 1. Update propagate to increment the masterOffset by the bytes sent
func (rm *ReplicationManager) propagate(arr []any) {
	payload := encodeRESPArray(arr)
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Track bytes propagated
	rm.masterOffset += len(payload)

	// Send the RESP command to all registered replicas
	for _, conn := range rm.replicas {
		_, _ = conn.Write(payload)
	}
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

func performReplicaHandshake(host, masterPort, replicaPort string) (net.Conn, *bufio.Reader, error) {
	if host == "" || masterPort == "" || replicaPort == "" {
		return nil, nil, fmt.Errorf("invalid replica target")
	}

	conn, err := net.Dial("tcp", net.JoinHostPort(host, masterPort))
	if err != nil {
		return nil, nil, err
	}

	// DO NOT close the connection here!
	reader := bufio.NewReader(conn)

	// PING
	if _, err = conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return nil, nil, err
	}
	if _, err = ParseRESP(reader); err != nil {
		return nil, nil, err
	}

	// REPLCONF listening-port
	if _, err = fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(replicaPort), replicaPort); err != nil {
		return nil, nil, err
	}
	if _, err = ParseRESP(reader); err != nil {
		return nil, nil, err
	}

	// REPLCONF capa
	if _, err = conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")); err != nil {
		return nil, nil, err
	}
	if _, err = ParseRESP(reader); err != nil {
		return nil, nil, err
	}

	// PSYNC
	if _, err = conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")); err != nil {
		return nil, nil, err
	}

	return conn, reader, nil
}

func main() {
	slog.Info("Logs from your program will appear here!")

	cwd, err := os.Getwd()
	if err != nil {
		slog.Error("failed to get current working directory", "err", err)
		os.Exit(1)
	}

	configPathFlag := flag.String("config", "", "Path to an optional config file")
	dirFlag := flag.String("dir", cwd, "Directory where RDB files are stored")
	dbFlag := flag.String("dbfilename", "", "Name of the RDB file name")
	appendonlyFlag := flag.String("appendonly", "no", "Enable append-only mode")
	appenddirnameFlag := flag.String("appenddirname", "appendonlydir", "Directory where AOF files are stored")
	appendfilenameFlag := flag.String("appendfilename", "appendonly.aof", "Name of the AOF file")
	appendfsyncFlag := flag.String("appendfsync", "everysec", "AOF fsync policy")
	portFlag := flag.String("port", "6379", "Port to listen on")
	replicaof := flag.String("replicaof", "", "Address of the master server to replicate from (host:port)")
	requirepassFlag := flag.String("requirepass", "", "Password required for clients to authenticate")
	flag.Parse()

	explicitFlags := map[string]bool{}
	flag.Visit(func(f *flag.Flag) {
		explicitFlags[f.Name] = true
	})

	config := ServerConfig{
		dir:              cwd,
		dbfilename:       "",
		appendonly:       "no",
		appenddirname:    "appendonlydir",
		appendfilename:   "appendonly.aof",
		appendfsync:      "everysec",
		requirepass:      "",
		port:             "6379",
		role:             "master",
		replicaof:        "",
		masterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		masterReplOffset: 0,
	}

	if resolvedConfigPath := resolveConfigPath(*configPathFlag); resolvedConfigPath != "" {
		if values, err := loadConfigFromFile(resolvedConfigPath); err != nil {
			slog.Warn("failed to load config file, continuing with defaults", "path", resolvedConfigPath, "err", err)
		} else if err := applyConfigFileValues(&config, values); err != nil {
			slog.Warn("invalid config values, continuing with defaults", "path", resolvedConfigPath, "err", err)
		}
	}

	if explicitFlags["dir"] {
		config.dir = *dirFlag
	}
	if explicitFlags["dbfilename"] {
		config.dbfilename = *dbFlag
	}
	if explicitFlags["appendonly"] {
		config.appendonly = *appendonlyFlag
	}
	if explicitFlags["appenddirname"] {
		config.appenddirname = *appenddirnameFlag
	}
	if explicitFlags["appendfilename"] {
		config.appendfilename = *appendfilenameFlag
	}
	if explicitFlags["appendfsync"] {
		config.appendfsync = *appendfsyncFlag
	}
	if explicitFlags["requirepass"] {
		config.requirepass = *requirepassFlag
	}
	if explicitFlags["port"] {
		config.port = *portFlag
	}
	if explicitFlags["replicaof"] {
		config.replicaof = *replicaof
	}
	if config.replicaof != "" {
		config.role = "slave"
	} else {
		config.role = "master"
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
		go func() {
			host, port, err := parseReplicaTarget(config.replicaof)
			if err != nil {
				slog.Error("invalid replicaof target", "replicaof", config.replicaof, "err", err)
				os.Exit(1)
			}

			conn, reader, err := performReplicaHandshake(host, port, config.port)
			if err != nil {
				slog.Error("failed to perform replica handshake", "host", host, "port", port, "err", err)
				os.Exit(1)
			}
			defer conn.Close()

			// 1. Read +FULLRESYNC
			val, err := ParseRESP(reader)
			if err != nil {
				slog.Error("failed to read FULLRESYNC", "err", err)
				return
			}
			slog.Debug("received PSYNC response", "val", val)

			// 2. Read RDB file
			line, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(line, "$") {
				slog.Error("failed to read RDB length", "err", err)
				return
			}
			lengthStr := strings.TrimSpace(line[1:])
			length, err := strconv.Atoi(lengthStr)
			if err != nil {
				slog.Error("invalid RDB length", "err", err)
				return
			}

			rdbBuf := make([]byte, length)
			if _, err := io.ReadFull(reader, rdbBuf); err != nil {
				slog.Error("failed to read RDB file", "err", err)
				return
			}

			// Track processed replication offset on replica
			var replicaOffset int64 = 0

			// 3. Continuously process propagated commands from master
			// 3. Continuously process propagated commands from master
			for {
				bufReader := reader

				val, err := ParseRESP(bufReader)
				if err != nil {
					slog.Error("master connection closed or parse error", "err", err)
					break
				}

				arr, ok := val.([]any)
				if !ok || len(arr) == 0 {
					continue
				}

				// Calculate exact bytes of encoded RESP command read
				cmdBytes := int64(len(encodeRESPArray(arr)))
				cmdRaw, _ := asString(arr[0])

				// Intercept GETACK directly on the replica side
				if strings.ToUpper(cmdRaw) == "REPLCONF" && len(arr) >= 3 {
					arg1, _ := asString(arr[1])
					if strings.ToUpper(arg1) == "GETACK" {
						offsetStr := strconv.FormatInt(replicaOffset, 10)
						_ = writeArrayResponse(conn, []string{"REPLCONF", "ACK", offsetStr})
						replicaOffset += cmdBytes
						continue
					}
				}

				// Execute command silently
				handleCommand(nil, arr, store, config)
				replicaOffset += cmdBytes
			}
		}()
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
