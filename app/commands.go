package main

import (
	"fmt"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

// Helper: safely assert any to string
func asString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// Helper: write common replies
func writeOK(conn net.Conn) {
	if conn == nil {
		return
	}
	_, _ = conn.Write([]byte("+OK\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeOK", "remote", addr.String())
	}
}

func writeErr(conn net.Conn, msg string) {
	if conn == nil {
		return
	}
	_, _ = conn.Write([]byte("-ERR " + msg + "\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Warn("writeErr", "remote", addr.String(), "msg", msg)
	}
}

func writeNullBulk(conn net.Conn) {
	if conn == nil {
		return
	}
	_, _ = conn.Write([]byte("$-1\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeNullBulk", "remote", addr.String())
	}
}

func writeInteger(conn net.Conn, value int) {
	if conn == nil {
		return
	}
	_, _ = conn.Write([]byte(":" + strconv.Itoa(value) + "\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeInteger", "remote", addr.String(), "value", value)
	}
}

func writeString(conn net.Conn, value string) {
	if conn == nil {
		return
	}
	_, _ = conn.Write([]byte("+" + value + "\r\n"))
	if addr := conn.RemoteAddr(); addr != nil {
		slog.Debug("writeString", "remote", addr.String(), "value", value)
	}
}

func writeArrayResponse(conn net.Conn, items []string) error {
	if conn == nil {
		return nil
	}
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(items)))
	builder.WriteString("\r\n")
	for _, item := range items {
		builder.WriteString("$")
		builder.WriteString(strconv.Itoa(len(item)))
		builder.WriteString("\r\n")
		builder.WriteString(item)
		builder.WriteString("\r\n")
	}
	_, err := conn.Write([]byte(builder.String()))
	if err != nil {
		slog.Error("failed to write array response", "err", err)
	} else {
		if addr := conn.RemoteAddr(); addr != nil {
			slog.Debug("wrote array response", "remote", addr.String(), "count", len(items))
		}
	}
	return err
}

func persistWriteCommand(conn net.Conn, config ServerConfig, arr []any) bool {
	globalReplManager.propagate(arr)

	if conn == nil || config.appendonly != "yes" {
		return true
	}

	if err := appendRESPCommandToAOF(config, arr); err != nil {
		slog.Error("failed to append command to AOF", "err", err, "cmd", arr[0])
		writeErr(conn, "ERR failed to append to AOF")
		return false
	}

	return true
}

// parseTTL checks for EX/PX options in SET arguments and returns
// ttl in milliseconds and true if present and valid.
func parseTTL(arr []any) (int, bool) {
	if len(arr) < 5 {
		return 0, false
	}
	optRaw, ok := asString(arr[3])
	if !ok {
		return 0, false
	}
	opt := strings.ToUpper(optRaw)
	if opt != "EX" && opt != "PX" {
		return 0, false
	}
	ttlStr, ok := asString(arr[4])
	if !ok {
		return 0, false
	}
	ttl, err := strconv.Atoi(ttlStr)
	if err != nil {
		return 0, false
	}
	if opt == "EX" {
		ttl = ttl * 1000 // convert seconds to milliseconds
	}
	return ttl, true
}

// handleCommand dispatches a parsed RESP array (as []any) to
// the appropriate command handler. It writes responses directly to conn.
func handleCommand(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) == 0 {
		return
	}
	cmdRaw, ok := asString(arr[0])
	if !ok {
		return
	}
	cmd := strings.ToUpper(cmdRaw)

	// Log the incoming command for debugging/tracing.
	remote := "unknown"
	if conn != nil {
		if addr := conn.RemoteAddr(); addr != nil {
			remote = addr.String()
		}
	}
	slog.Debug("handleCommand", "remote", remote, "raw_cmd", cmd)

	// Intercept commands if the client is in Subscribed mode
	if conn != nil && store.IsSubscribed(conn) {
		switch cmd {
		case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT", "RESET":
		default:
			errMsg := "Can't execute '" + strings.ToLower(cmd) + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
			writeErr(conn, errMsg)
			return
		}
	}

	// Intercept commands if the client is in a Transaction (MULTI) block
	if conn != nil && store.IsInTx(conn) {
		switch cmd {
		case "EXEC", "DISCARD", "MULTI", "QUIT", "WATCH":
			// Let these commands pass through to be handled normally
		default:
			// Queue the command and return +QUEUED
			store.QueueCommand(conn, arr)
			if conn != nil {
				conn.Write([]byte("+QUEUED\r\n"))
			}
			return
		}
	}

	switch cmd {
	case "PING":
		handlePING(conn, arr, store)
	case "REPLCONF":
		handleREPLCONF(conn, arr)
	case "WAIT":
		handleWAIT(conn, arr)
	case "PSYNC":
		handlePSYNC(conn, config)
	case "ECHO":
		handleECHO(conn, arr)
	case "SET":
		handleSET(conn, arr, store, config)
	case "GET":
		handleGET(conn, arr, store)
	case "LPUSH":
		handleLPUSH(conn, arr, store, config)
	case "RPUSH":
		handleRPUSH(conn, arr, store, config)
	case "LRANGE":
		handleLRANGE(conn, arr, store)
	case "LLEN":
		handleLLEN(conn, arr, store)
	case "LPOP":
		handleLPOP(conn, arr, store, config)
	case "RPOP":
		handleRPOP(conn, arr, store, config)
	case "BLPOP":
		handleBLPOP(conn, arr, store, config)
	case "CONFIG":
		handleCONFIG(conn, arr, config)
	case "KEYS":
		handleKEYS(conn, arr, store)
	case "ZADD":
		handleZADD(conn, arr, store, config)
	case "ZRANGE":
		handleZRANGE(conn, arr, store)
	case "ZRANK":
		handleZRANK(conn, arr, store)
	case "ZCARD":
		handleZCARD(conn, arr, store)
	case "XADD":
		handleXADD(conn, arr, store, config)
	case "ZSCORE":
		handleZSCORE(conn, arr, store)
	case "ZREM":
		handleZREM(conn, arr, store)
	case "SUBSCRIBE":
		handleSUBSCRIBE(conn, arr, store)
	case "PUBLISH":
		handlePUBLISH(conn, arr, store)
	case "UNSUBSCRIBE":
		handleUNSUBSCRIBE(conn, arr, store)
	case "INCR":
		handleINCR(conn, arr, store, config)
	case "MULTI":
		handleMULTI(conn, arr, store)
	case "EXEC":
		handleEXEC(conn, arr, store, config)
	case "DISCARD":
		handleDISCARD(conn, arr, store)
	case "WATCH":
		handleWATCH(conn, arr, store)
	case "UNWATCH":
		handleUNWATCH(conn, arr, store)
	case "TYPE":
		handleTYPE(conn, arr, store)
	case "XRANGE":
		handleXRANGE(conn, arr, store)
	case "XREAD":
		handleXREAD(conn, arr, store)
	case "GEOADD":
		handleGEOADD(conn, arr, store, config)
	case "GEOPOS":
		handleGEOPOS(conn, arr, store)
	case "GEODIST":
		handleGEODIST(conn, arr, store)
	case "GEOSEARCH":
		handleGEOSEARCH(conn, arr, store)
	case "INFO":
		handleINFO(conn, arr, store, config)
	case "ACL":
		handleACL(conn, arr, store, config)
	default:
		slog.Warn("unknown command", "cmd", cmd, "remote", remote)
		writeErr(conn, "unknown command")
	}
}

// handlePING replies with PONG for a PING command.
func handlePING(conn net.Conn, arr []any, store *Store) {
	if len(arr) == 1 {
		if store.IsSubscribed(conn) {
			writeArrayResponse(conn, []string{"pong", ""})
		} else {
			writeString(conn, "PONG")
		}
	}
}

func handleREPLCONF(conn net.Conn, arr []any) {
	if len(arr) >= 3 {
		arg1, _ := asString(arr[1])
		if strings.ToUpper(arg1) == "ACK" {
			// The master receives the ACK from the replica.
			offsetStr, _ := asString(arr[2])
			offset, _ := strconv.Atoi(offsetStr)

			// Update the offset for this specific replica connection
			globalReplManager.mu.Lock()
			globalReplManager.replicaOffsets[conn] = offset
			globalReplManager.mu.Unlock()

			// ACKs are silent, do not write OK back.
			return
		}
	}
	writeOK(conn)
}

func handleWAIT(conn net.Conn, arr []any) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'WAIT' command")
		return
	}

	numReplicasStr, _ := asString(arr[1])
	timeoutStr, _ := asString(arr[2])

	expectedReplicas, _ := strconv.Atoi(numReplicasStr)
	timeout, _ := strconv.Atoi(timeoutStr)

	globalReplManager.mu.Lock()
	masterOffset := globalReplManager.masterOffset
	// Create a safe copy of active connections
	activeReplicas := make([]net.Conn, len(globalReplManager.replicas))
	copy(activeReplicas, globalReplManager.replicas)
	globalReplManager.mu.Unlock()

	// If no replicas requested or no commands were ever propagated, return immediately
	if expectedReplicas == 0 || masterOffset == 0 {
		writeInteger(conn, len(activeReplicas))
		return
	}

	// Ping all replicas for their offsets (bypassing propagate so we don't increase masterOffset)
	getAckPayload := encodeRESPArray([]any{"REPLCONF", "GETACK", "*"})
	for _, c := range activeReplicas {
		_, _ = c.Write(getAckPayload)
	}

	// Poll until we have enough ACKs or the timeout expires
	startTime := time.Now()
	for {
		ackCount := 0
		globalReplManager.mu.Lock()
		for _, c := range activeReplicas {
			if offset, ok := globalReplManager.replicaOffsets[c]; ok && offset >= masterOffset {
				ackCount++
			}
		}
		globalReplManager.mu.Unlock()

		if ackCount >= expectedReplicas {
			writeInteger(conn, ackCount)
			return
		}

		if timeout > 0 && time.Since(startTime).Milliseconds() >= int64(timeout) {
			writeInteger(conn, ackCount)
			return
		}

		time.Sleep(10 * time.Millisecond) // brief poll interval
	}
}

func handlePSYNC(conn net.Conn, config ServerConfig) {
	_, _ = conn.Write([]byte("+FULLRESYNC " + config.masterReplID + " 0\r\n"))

	emptyRDB := []byte("REDIS0006")
	emptyRDB = append(emptyRDB, 0xFF)
	emptyRDB = append(emptyRDB, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)
	_, _ = conn.Write(fmt.Appendf(nil, "$%d\r\n", len(emptyRDB)))
	_, _ = conn.Write(emptyRDB)

	globalReplManager.addReplica(conn)
}

// handleECHO replies with the provided argument as a bulk string.
func handleECHO(conn net.Conn, arr []any) {
	if len(arr) < 2 {
		writeNullBulk(conn)
		return
	}
	if arg, ok := asString(arr[1]); ok {
		writeBulkString(conn, arg)
	} else {
		writeNullBulk(conn)
	}
}

// handleSET stores a key/value pair and supports optional EX/PX TTL.
func handleSET(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'SET' command")
		return
	}
	key, _ := asString(arr[1])
	value, _ := asString(arr[2])
	if ttl, ok := parseTTL(arr); ok {
		slog.Info("SET with TTL", "key", key, "ttl_ms", ttl)
		store.SetWithTTL(key, value, ttl)
	} else {
		slog.Debug("SET", "key", key)
		store.Set(key, value)
	}

	if !persistWriteCommand(conn, config, arr) {
		return
	}

	writeOK(conn)
}

// handleGET retrieves a key and returns it as a bulk string or null.
func handleGET(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'GET' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		slog.Debug("GET hit", "key", key)
		writeBulkString(conn, v.(string))
	} else {
		slog.Debug("GET miss", "key", key)
		writeNullBulk(conn)
	}
}

func handleLPUSH(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'LPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	slog.Debug("LPUSH", "key", key)
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "LPUSH values must be strings")
			return
		}
	}

	var initialLen int
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			initialLen = len(list)
		}
	}
	expectedLen := initialLen + len(values)

	store.mu.Lock()
	var remaining []string
	for _, val := range values {
		if queues, exists := store.waiters[key]; exists && len(queues) > 0 {
			waiterChan := queues[0]
			store.waiters[key] = queues[1:]

			if len(store.waiters[key]) == 0 {
				delete(store.waiters, key)
			}
			waiterChan <- val
		} else {
			remaining = append(remaining, val)
		}
	}
	store.mu.Unlock()

	if len(remaining) > 0 {
		store.LPush(key, remaining...)
	}

	if !persistWriteCommand(conn, config, arr) {
		return
	}

	_, _ = conn.Write([]byte(":" + strconv.Itoa(expectedLen) + "\r\n"))
}

func handleRPUSH(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'RPUSH' command")
		return
	}
	key, _ := asString(arr[1])
	slog.Debug("RPUSH", "key", key)
	values := make([]string, 0, len(arr)-2)
	for _, v := range arr[2:] {
		if str, ok := asString(v); ok {
			values = append(values, str)
		} else {
			writeErr(conn, "RPUSH values must be strings")
			return
		}
	}

	var initialLen int
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			initialLen = len(list)
		}
	}
	expectedLen := initialLen + len(values)

	store.mu.Lock()
	var remaining []string
	for _, val := range values {
		if queues, exists := store.waiters[key]; exists && len(queues) > 0 {
			waiterChan := queues[0]
			store.waiters[key] = queues[1:]

			if len(store.waiters[key]) == 0 {
				delete(store.waiters, key)
			}
			waiterChan <- val
		} else {
			remaining = append(remaining, val)
		}
	}
	store.mu.Unlock()

	if len(remaining) > 0 {
		store.RPush(key, remaining...)
	}

	if !persistWriteCommand(conn, config, arr) {
		return
	}

	_, _ = conn.Write([]byte(":" + strconv.Itoa(expectedLen) + "\r\n"))
}

func handleLRANGE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'LRANGE' command")
		return
	}
	key, _ := asString(arr[1])
	startStr, _ := asString(arr[2])
	endStr, _ := asString(arr[3])
	start, err1 := strconv.Atoi(startStr)
	end, err2 := strconv.Atoi(endStr)
	if err1 != nil || err2 != nil {
		writeErr(conn, "LRANGE start and end must be integers")
		return
	}
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			length := len(list)
			if start < 0 {
				start = length + start
			}
			if end < 0 {
				end = length + end
			}
			if start < 0 {
				start = 0
			}
			if end >= length {
				end = length - 1
			}
			if start > end || start >= length {
				conn.Write([]byte("*0\r\n"))
				return
			}
			sublist := list[start : end+1]
			if err := writeArrayResponse(conn, sublist); err != nil {
				return
			}
		} else {
			writeErr(conn, "LRANGE key is not a list")
		}
	} else {
		conn.Write([]byte("*0\r\n"))
	}
}

func handleLLEN(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'LLEN' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			length := len(list)
			_, _ = conn.Write([]byte(":" + strconv.Itoa(length) + "\r\n"))
		} else {
			writeErr(conn, "LLEN key is not a list")
		}
	} else {
		_, _ = conn.Write([]byte(":0\r\n"))
	}
}

func handleLPOP(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'LPOP' command")
		return
	}
	key, _ := asString(arr[1])
	cnt := 1
	if len(arr) == 3 {
		cnt, _ = strconv.Atoi(arr[2].(string))
	}
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			if len(list) == 0 {
				writeNullBulk(conn)
				return
			}
			if cnt <= 0 {
				writeErr(conn, "LPOP count must be a positive integer")
				return
			}
			if cnt > len(list) {
				cnt = len(list)
			}
			poppedValues := list[:cnt]
			store.Set(key, list[cnt:])
			if !persistWriteCommand(conn, config, arr) {
				return
			}
			if len(poppedValues) == 1 {
				writeBulkString(conn, poppedValues[0])
			} else {
				writeArrayResponse(conn, poppedValues)
			}
		} else {
			writeErr(conn, "LPOP key is not a list")
		}
	} else {
		writeNullBulk(conn)
	}
}

func handleRPOP(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'RPOP' command")
		return
	}
	key, _ := asString(arr[1])
	if v, ok := store.Get(key); ok {
		if list, ok := v.([]string); ok {
			if len(list) == 0 {
				writeNullBulk(conn)
				return
			}
			poppedValue := list[len(list)-1]
			store.Set(key, list[:len(list)-1])
			if !persistWriteCommand(conn, config, arr) {
				return
			}
			writeBulkString(conn, poppedValue)
		} else {
			writeErr(conn, "RPOP key is not a list")
		}
	} else {
		writeNullBulk(conn)
	}
}

// timeout is always 0 as of now so clean up logic not added for waiters
func handleBLPOP(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'BLPOP' command")
		return
	}

	key, _ := asString(arr[1])
	timeoutStr, _ := asString(arr[len(arr)-1])

	slog.Debug("BLPOP", "key", key, "timeout", timeoutStr)

	timeoutSeconds, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil {
		writeErr(conn, "timeout is not a float or integer")
		return
	}

	store.mu.Lock()
	if list, exists := store.cache[key].([]string); exists && len(list) > 0 {
		val := list[0]
		store.cache[key] = list[1:]
		if len(store.cache[key].([]string)) == 0 {
			delete(store.cache, key)
		}

		store.markDirty(key) // Notify watches for list pops!

		if !persistWriteCommand(conn, config, arr) {
			return
		}

		writeArrayResponse(conn, []string{key, val})
		store.mu.Unlock()
		return
	}

	waiterChan := make(chan string, 1)
	store.waiters[key] = append(store.waiters[key], waiterChan)
	store.mu.Unlock()

	if timeoutSeconds > 0 {
		// Wait for either the value or the timeout
		select {
		case val := <-waiterChan:
			writeArrayResponse(conn, []string{key, val})
		case <-time.After(time.Duration(timeoutSeconds * float64(time.Second))):
			// On timeout, return a null array
			conn.Write([]byte("*-1\r\n"))

			// Clean up the expired channel from the waiters map
			store.CleanUpExpiredKeyWaiter(key, waiterChan)
		}
	} else {
		// Timeout is 0: wait infinitely
		val := <-waiterChan
		writeArrayResponse(conn, []string{key, val})
	}
}

func handleCONFIG(conn net.Conn, arr []any, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'CONFIG' command")
		return // Added return to prevent out-of-bounds panics
	}

	if cmd, ok := asString(arr[1]); ok {
		// It's good practice to make the command case-insensitive
		switch strings.ToUpper(cmd) {
		case "GET":
			if len(arr) < 3 {
				writeErr(conn, "wrong number of arguments for 'CONFIG GET' command")
				return
			}

			param, ok := asString(arr[2])
			if !ok {
				writeErr(conn, "invalid parameter format")
				return
			}

			var value string
			// Match parameter (Redis parameter names are generally case-insensitive)
			switch strings.ToLower(param) {
			case "dir":
				value = config.dir
			case "dbfilename":
				value = config.dbfilename
			case "appendonly":
				value = config.appendonly
			case "appenddirname":
				value = config.appenddirname
			case "appendfilename":
				value = config.appendfilename
			case "appendfsync":
				value = config.appendfsync
			default:
				value = ""
			}

			writeArrayResponse(conn, []string{param, value})
		default:
			writeErr(conn, "unknown CONFIG subcommand")
		}
	}
}

// handleKEYS returns all keys matching a pattern. Currently only supports "*".
func handleKEYS(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'KEYS' command")
		return
	}

	pattern, ok := asString(arr[1])
	if !ok {
		writeErr(conn, "invalid pattern format")
		return
	}

	// We only support the "*" pattern for this stage
	if pattern == "*" {
		// Call the thread-safe Keys method on the store
		keys := store.Keys(pattern)

		// Use your existing helper to write the array response
		writeArrayResponse(conn, keys)
	} else {
		// Return an empty RESP array for unsupported patterns
		conn.Write([]byte("*0\r\n"))
	}
}

func handleZADD(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 4 || len(arr)%2 != 0 {
		writeErr(conn, "wrong number of arguments for 'ZADD' command")
		return
	}

	key, _ := asString(arr[1])
	addedCount := 0

	for i := 2; i < len(arr); i += 2 {
		scoreStr, _ := asString(arr[i])
		member, _ := asString(arr[i+1])

		// Parse the score into a float64
		score, err := strconv.ParseFloat(scoreStr, 64)
		if err != nil {
			writeErr(conn, "ERR value is not a valid float")
			return
		}

		addedCount += store.ZAdd(key, score, member)
	}
	if !persistWriteCommand(conn, config, arr) {
		return
	}
	writeInteger(conn, addedCount)
}

func handleZRANGE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'ZRANGE' command")
		return
	}

	key, _ := asString(arr[1])
	startStr, _ := asString(arr[2])
	stopStr, _ := asString(arr[3])

	// Parse indices
	start, err1 := strconv.Atoi(startStr)
	stop, err2 := strconv.Atoi(stopStr)
	if err1 != nil || err2 != nil {
		writeErr(conn, "ERR value is not an integer or out of range")
		return
	}

	result := store.ZRange(key, start, stop)
	writeArrayResponse(conn, result)
}

func handleZRANK(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "Wrong number of arguments for 'ZRANK' command")
		return
	}

	key, _ := asString(arr[1])
	member, _ := asString(arr[2])

	rank := store.ZRank(key, member)
	if rank != -1 {
		writeInteger(conn, rank)
	} else {
		writeNullBulk(conn)
	}
}

func handleZCARD(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 1 {
		writeErr(conn, "Wrong number of arguments for 'ZCARD' command")
		return
	}

	key, _ := asString(arr[1])
	cnt := store.ZCard(key)
	writeInteger(conn, cnt)
}

func handleZSCORE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "Wrong number of arguments for 'ZSCORE' command")
		return
	}

	key, _ := asString(arr[1])
	member, _ := asString(arr[2])
	value, found := store.getZscoreValue(key, member)
	if !found {
		writeNullBulk(conn)
		return
	}
	writeBulkString(conn, strconv.FormatFloat(value, 'f', -1, 64))
}

func handleZREM(conn net.Conn, arr []any, store *Store) {
	// ZREM needs at least 3 arguments: ZREM, key, and at least one member
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'ZREM' command")
		return
	}

	key, _ := asString(arr[1])

	members := make([]string, 0, len(arr)-2)
	for i := 2; i < len(arr); i++ {
		if member, ok := asString(arr[i]); ok {
			members = append(members, member)
		}
	}

	// Execute removal and get the count
	removedCount := store.ZRem(key, members)
	writeInteger(conn, removedCount)
}

func handleSUBSCRIBE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'SUBSCRIBE' command")
		return
	}

	for i := 1; i < len(arr); i++ {
		channel, ok := asString(arr[i])
		if !ok {
			continue
		}

		subCount := store.Subscribe(conn, channel)
		slog.Info("SUBSCRIBE", "remote", conn.RemoteAddr(), "channel", channel, "count", subCount)

		// format : *3\r\n$9\r\nsubscribe\r\n$<len(channel)>\r\n<channel>\r\n:<i>\r\n
		resp := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subCount)
		conn.Write([]byte(resp))
	}
}

func handleUNSUBSCRIBE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'UNSUBSCRIBE' command")
		return
	}

	for i := 1; i < len(arr); i++ {
		channel, ok := asString(arr[i])
		if !ok {
			continue
		}

		subCount := store.Unsubscribe(conn, channel)
		slog.Info("UNSUBSCRIBE", "remote", conn.RemoteAddr(), "channel", channel, "count", subCount)

		// format : *3\r\n$11\r\nunsubscribe\r\n$<len(channel)>\r\n<channel>\r\n:<i>\r\n
		resp := fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, subCount)
		conn.Write([]byte(resp))
	}
}

func handlePUBLISH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'PUBLISH' command")
		return
	}

	channelName, _ := asString(arr[1])
	messageContent, _ := asString(arr[2])

	slog.Info("PUBLISH", "channel", channelName, "message", messageContent)
	publishCnt := store.PublishMessageOnChannel(channelName, messageContent)
	writeInteger(conn, publishCnt)
}
func handleINCR(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'INCR' command")
		return
	}
	key, _ := asString(arr[1])

	val, err := store.Incr(key)
	if err != nil {
		// Standard Redis error for invalid integer operations
		writeErr(conn, "value is not an integer or out of range")
		return
	}

	slog.Debug("INCR result", "key", key, "value", val)
	if !persistWriteCommand(conn, config, arr) {
		return
	}
	writeInteger(conn, val)
}
func handleMULTI(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'MULTI' command")
		return
	}

	// Attempt to start the transaction
	if !store.StartTx(conn) {
		slog.Warn("MULTI nested attempt", "remote", conn.RemoteAddr())
		writeErr(conn, "ERR MULTI calls can not be nested")
		return
	}

	slog.Debug("MULTI started", "remote", conn.RemoteAddr())
	writeOK(conn)
}

// handleEXEC executes queued commands.
// For this stage, it only handles the error case when MULTI hasn't been called.
func handleEXEC(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'EXEC' command")
		return
	}
	queue, wasInTx := store.GetAndClearTx(conn)

	// Check if the connection is currently in a transaction block
	if !wasInTx {
		slog.Warn("EXEC without MULTI", "remote", conn.RemoteAddr())
		writeErr(conn, "EXEC without MULTI")
		return
	}

	isDirty := store.IsDirty(conn)
	store.Unwatch(conn) // EXEC clears watched keys unconditionally

	// If keys were modified, abort transaction and return a RESP null array
	if isDirty {
		slog.Debug("EXEC aborted due to modified watched keys", "remote", conn.RemoteAddr())
		conn.Write([]byte("*-1\r\n"))
		return
	}

	if len(queue) == 0 {
		slog.Debug("EXEC empty queue", "remote", conn.RemoteAddr())
		conn.Write([]byte("*0\r\n"))
		return
	}

	conn.Write([]byte("*" + strconv.Itoa(len(queue)) + "\r\n"))
	for _, cmdArr := range queue {
		handleCommand(conn, cmdArr, store, config)
	}
}

func handleDISCARD(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'DISCARD' command")
		return
	}

	if !store.IsInTx(conn) {
		slog.Warn("DISCARD without MULTI", "remote", conn.RemoteAddr())
		writeErr(conn, "DISCARD without MULTI")
		return
	}

	store.ClearTx(conn)
	store.Unwatch(conn) // DISCARD also clears watched keys
	slog.Debug("DISCARD executed", "remote", conn.RemoteAddr())
	writeOK(conn)
}

func handleWATCH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'WATCH' command")
		return
	}
	if store.IsInTx(conn) {
		writeErr(conn, "WATCH inside MULTI is not allowed")
		return
	}

	slog.Info("WATCH", "remote", conn.RemoteAddr(), "keys", arr[1:])
	keys := make([]string, 0, len(arr)-1)
	for _, v := range arr[1:] {
		if key, ok := asString(v); ok {
			keys = append(keys, key)
		}
	}

	store.Watch(conn, keys)
	writeOK(conn)
}

func handleUNWATCH(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 1 {
		writeErr(conn, "wrong number of arguments for 'UNWATCH' command")
		return
	}
	slog.Info("UNWATCH", "remote", conn.RemoteAddr())
	store.Unwatch(conn)
	writeOK(conn)
}

func handleTYPE(conn net.Conn, arr []any, store *Store) {
	if len(arr) != 2 {
		writeErr(conn, "wrong number of arguments for 'TYPE' command")
		return
	}

	key, _ := asString(arr[1])
	val, exists := store.Get(key)
	if !exists {
		writeString(conn, "none")
		return
	}
	var typeStr string
	switch val.(type) {
	case string:
		typeStr = "string"
	case []string:
		typeStr = "list"
	case map[string]float64:
		typeStr = "zset"
	case []StreamEntry:
		typeStr = "stream"
	default:
		typeStr = "unknown"
	}
	writeString(conn, typeStr)
}

// handleXADD appends an entry to a stream. Syntax: XADD key id field value [field value ...]
func handleXADD(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 5 {
		writeErr(conn, "wrong number of arguments for 'XADD' command")
		return
	}
	key, _ := asString(arr[1])
	id, _ := asString(arr[2])

	// remaining args must be field value pairs
	if (len(arr)-3)%2 != 0 {
		writeErr(conn, "wrong number of arguments for 'XADD' command: fields must be key value pairs")
		return
	}

	// Build ordered flat fields slice: [field1, value1, field2, value2, ...]
	fieldsSlice := make([]string, 0, (len(arr) - 3))
	for i := 3; i < len(arr); i += 2 {
		f, ok1 := asString(arr[i])
		v, ok2 := asString(arr[i+1])
		if !ok1 || !ok2 {
			writeErr(conn, "XADD fields and values must be strings")
			return
		}
		fieldsSlice = append(fieldsSlice, f, v)
	}

	// If key exists and is not a stream, return WRONGTYPE error
	if v, exists := store.Get(key); exists {
		if _, isStream := v.([]StreamEntry); !isStream {
			writeErr(conn, "WRONGTYPE Operation against a key holding the wrong kind of value")
			return
		}
	}

	// Append to stream
	newID, err := store.XAdd(key, id, fieldsSlice)
	if err != nil {
		writeErr(conn, err.Error())
		return
	}
	// Return the ID as a bulk string
	writeBulkString(conn, newID)
}

// handleXRANGE retrieves entries between start and end IDs (inclusive).
// Syntax: XRANGE key start end
func handleXRANGE(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'XRANGE' command")
		return
	}
	key, _ := asString(arr[1])
	startID, _ := asString(arr[2])
	endID, _ := asString(arr[3])

	entries, err := store.XRange(key, startID, endID)
	if err != nil {
		writeErr(conn, err.Error())
		return
	}

	// Write outer array header
	_, _ = conn.Write([]byte("*" + strconv.Itoa(len(entries)) + "\r\n"))

	for _, e := range entries {
		// Each entry is an array of two elements
		_, _ = conn.Write([]byte("*2\r\n"))
		// ID as bulk string
		writeBulkString(conn, e.ID)
		// fields as array of bulk strings
		if err := writeArrayResponse(conn, e.Fields); err != nil {
			return
		}
	}
}

// handleXREAD implements: XREAD STREAMS key [key ...] id [id ...]
func handleXREAD(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'XREAD' command")
		return
	}

	idx := 1
	block := false
	var timeoutMs int

	token1, ok := asString(arr[idx])
	if !ok {
		writeErr(conn, "syntax error")
		return
	}

	if strings.ToUpper(token1) == "BLOCK" {
		// Expect timeout then STREAMS
		if len(arr) < 6 {
			writeErr(conn, "wrong number of arguments for 'XREAD' command")
			return
		}
		timeoutStr, ok := asString(arr[idx+1])
		if !ok {
			writeErr(conn, "syntax error")
			return
		}
		t, err := strconv.Atoi(timeoutStr)
		if err != nil {
			writeErr(conn, "syntax error")
			return
		}
		block = true
		timeoutMs = t
		idx += 2
	}

	// next token must be STREAMS
	token2, ok := asString(arr[idx])
	if !ok || strings.ToUpper(token2) != "STREAMS" {
		writeErr(conn, "syntax error")
		return
	}
	idx++

	// remaining tokens: keys then ids (split evenly)
	rem := arr[idx:]
	if len(rem) == 0 || len(rem)%2 != 0 {
		writeErr(conn, "syntax error")
		return
	}
	n := len(rem) / 2
	keys := make([]string, 0, n)
	rawIds := make([]string, 0, n)
	for i := 0; i < n; i++ {
		if k, ok := asString(rem[i]); ok {
			keys = append(keys, k)
		} else {
			writeErr(conn, "XREAD keys must be strings")
			return
		}
	}
	for i := n; i < 2*n; i++ {
		if id, ok := asString(rem[i]); ok {
			rawIds = append(rawIds, id)
		} else {
			writeErr(conn, "XREAD ids must be strings")
			return
		}
	}

	// Non-blocking: substitute '$' with current stream top and read
	if !block {
		ids := make([]string, n)
		for i := 0; i < n; i++ {
			if rawIds[i] == "$" {
				// get last ID for key
				if v, ok := store.Get(keys[i]); ok {
					if entries, isStream := v.([]StreamEntry); isStream && len(entries) > 0 {
						ids[i] = entries[len(entries)-1].ID
						continue
					}
				}
				ids[i] = "0-0"
			} else {
				ids[i] = rawIds[i]
			}
		}

		data, err := store.XRead(keys, ids)
		if err != nil {
			writeErr(conn, err.Error())
			return
		}

		// Build RESP response: array of streams
		_, _ = conn.Write([]byte("*" + strconv.Itoa(len(keys)) + "\r\n"))
		for _, k := range keys {
			entries := data[k]
			// Each stream: array of 2 elements: key, entries array
			_, _ = conn.Write([]byte("*2\r\n"))
			writeBulkString(conn, k)
			// entries array
			_, _ = conn.Write([]byte("*" + strconv.Itoa(len(entries)) + "\r\n"))
			for _, e := range entries {
				// entry array: [id, [fields...]]
				_, _ = conn.Write([]byte("*2\r\n"))
				writeBulkString(conn, e.ID)
				// fields array
				if err := writeArrayResponse(conn, e.Fields); err != nil {
					return
				}
			}
		}
		return
	}

	// Blocking: register waiter channel first and capture current stream tops for '$'
	ch := make(chan string, 1)
	lastIDs := make([]string, n)
	for i, k := range keys {
		store.mu.Lock()
		if v, exists := store.cache[k]; exists {
			if entries, ok := v.([]StreamEntry); ok && len(entries) > 0 {
				lastIDs[i] = entries[len(entries)-1].ID
			} else {
				lastIDs[i] = "0-0"
			}
		} else {
			lastIDs[i] = "0-0"
		}
		store.waiters[k] = append(store.waiters[k], ch)
		store.mu.Unlock()
	}

	// Build ids to use for initial check after registration
	idsToUse := make([]string, n)
	for i := 0; i < n; i++ {
		if rawIds[i] == "$" {
			idsToUse[i] = lastIDs[i]
		} else {
			idsToUse[i] = rawIds[i]
		}
	}

	data, err := store.XRead(keys, idsToUse)
	if err != nil {
		// clean up waiters
		for _, k := range keys {
			store.CleanUpExpiredKeyWaiter(k, ch)
		}
		writeErr(conn, err.Error())
		return
	}

	hasAny := false
	for _, k := range keys {
		if len(data[k]) > 0 {
			hasAny = true
			break
		}
	}

	if !hasAny {
		// wait
		timedOut := false
		if timeoutMs == 0 {
			<-ch
		} else {
			select {
			case <-time.After(time.Duration(timeoutMs) * time.Millisecond):
				timedOut = true
			case <-ch:
			}
		}

		// Clean up the waiter from all keys
		for _, k := range keys {
			store.CleanUpExpiredKeyWaiter(k, ch)
		}

		if timedOut {
			conn.Write([]byte("*-1\r\n"))
			return
		}

		// Re-read after wakeup
		data, err = store.XRead(keys, idsToUse)
		if err != nil {
			writeErr(conn, err.Error())
			return
		}
	}

	// Build RESP response: array of streams
	_, _ = conn.Write([]byte("*" + strconv.Itoa(len(keys)) + "\r\n"))
	for _, k := range keys {
		entries := data[k]
		// Each stream: array of 2 elements: key, entries array
		_, _ = conn.Write([]byte("*2\r\n"))
		writeBulkString(conn, k)
		// entries array
		_, _ = conn.Write([]byte("*" + strconv.Itoa(len(entries)) + "\r\n"))
		for _, e := range entries {
			// entry array: [id, [fields...]]
			_, _ = conn.Write([]byte("*2\r\n"))
			writeBulkString(conn, e.ID)
			// fields array
			if err := writeArrayResponse(conn, e.Fields); err != nil {
				return
			}
		}
	}
}
func handleGEOADD(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 5 || (len(arr)-2)%3 != 0 {
		writeErr(conn, "wrong number of arguments for 'GEOADD' command")
		return
	}

	key, _ := asString(arr[1])
	addedCount := 0

	for i := 2; i < len(arr); i += 3 {
		longitudeStr, _ := asString(arr[i])
		latitudeStr, _ := asString(arr[i+1])
		member, _ := asString(arr[i+2])

		longitude, err1 := strconv.ParseFloat(longitudeStr, 64)
		latitude, err2 := strconv.ParseFloat(latitudeStr, 64)

		if err1 != nil || err2 != nil {
			writeErr(conn, "invalid longitude or latitude")
			return
		}

		if longitude < -180 || longitude > 180 || latitude < -85.05112878 || latitude > 85.05112878 {
			writeErr(conn, fmt.Sprintf("invalid longitude,latitude pair %.8f,%f", longitude, latitude))
			return
		}

		score := float64(encode(latitude, longitude))
		slog.Debug("GEOADD", "key", key, "longitude", longitude, "latitude", latitude, "member", member, "score", score)
		addedCount += store.ZAdd(key, score, member)
	}

	writeInteger(conn, addedCount)
}

func handleGEOPOS(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 3 {
		writeErr(conn, "wrong number of arguments for 'GEOPOS' command")
		return
	}

	key, _ := asString(arr[1])

	if _, err := conn.Write([]byte("*" + strconv.Itoa(len(arr)-2) + "\r\n")); err != nil {
		return
	}

	for i := 2; i < len(arr); i++ {
		member, ok := asString(arr[i])
		if !ok {
			_, _ = conn.Write([]byte("*-1\r\n"))
			continue
		}

		score, found := store.getZscoreValue(key, member)
		if !found {
			_, _ = conn.Write([]byte("*-1\r\n"))
			continue
		}

		coordinates := decode(uint64(score))
		_, _ = conn.Write([]byte("*2\r\n"))
		writeBulkString(conn, strconv.FormatFloat(coordinates.Longitude, 'f', -1, 64))
		writeBulkString(conn, strconv.FormatFloat(coordinates.Latitude, 'f', -1, 64))
	}
}

func handleGEODIST(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 4 {
		writeErr(conn, "wrong number of arguments for 'GEODIST' command")
		return
	}

	key, _ := asString(arr[1])
	member1, _ := asString(arr[2])
	member2, _ := asString(arr[3])

	score1, found1 := store.getZscoreValue(key, member1)
	score2, found2 := store.getZscoreValue(key, member2)
	if !found1 || !found2 {
		writeNullBulk(conn)
		return
	}

	coord1 := decode(uint64(score1))
	coord2 := decode(uint64(score2))
	distance := haversineDistanceMeters(coord1.Latitude, coord1.Longitude, coord2.Latitude, coord2.Longitude)

	writeBulkString(conn, strconv.FormatFloat(distance, 'f', -1, 64))
}

func handleGEOSEARCH(conn net.Conn, arr []any, store *Store) {
	if len(arr) < 8 {
		writeErr(conn, "wrong number of arguments for 'GEOSEARCH' command")
		return
	}

	key, _ := asString(arr[1])
	if strings.ToUpper(asStringOrEmpty(arr[2])) != "FROMLONLAT" {
		writeErr(conn, "unsupported GEOSEARCH mode")
		return
	}

	longitude, err1 := strconv.ParseFloat(asStringOrEmpty(arr[3]), 64)
	latitude, err2 := strconv.ParseFloat(asStringOrEmpty(arr[4]), 64)
	if err1 != nil || err2 != nil {
		writeErr(conn, "invalid longitude or latitude")
		return
	}

	if strings.ToUpper(asStringOrEmpty(arr[5])) != "BYRADIUS" {
		writeErr(conn, "unsupported GEOSEARCH option")
		return
	}

	radius, err := strconv.ParseFloat(asStringOrEmpty(arr[6]), 64)
	if err != nil {
		writeErr(conn, "invalid radius")
		return
	}

	unit := strings.ToLower(asStringOrEmpty(arr[7]))
	factor := 1.0
	switch unit {
	case "km":
		factor = 1000.0
	case "mi":
		factor = 1609.344
	case "ft":
		factor = 0.3048
	case "m":
		factor = 1.0
	default:
		factor = 1.0
	}

	maxDistance := radius * factor

	value, found := store.Get(key)
	if !found {
		writeArrayResponse(conn, []string{})
		return
	}

	zset, ok := value.(map[string]float64)
	if !ok {
		writeArrayResponse(conn, []string{})
		return
	}

	matches := make([]string, 0)
	for member, score := range zset {
		coords := decode(uint64(score))
		distance := haversineDistanceMeters(coords.Latitude, coords.Longitude, latitude, longitude)
		if distance <= maxDistance {
			matches = append(matches, member)
		}
	}

	writeArrayResponse(conn, matches)
}

// Implementation for INFO command
func handleINFO(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	slog.Debug("INFO command received", "remote", conn.RemoteAddr())
	if len(arr) < 1 {
		writeErr(conn, "wrong number of arguments for 'INFO' command")
		return
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", config.role))
	sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", config.masterReplID))
	sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", config.masterReplOffset))

	infoString := sb.String()
	writeBulkString(conn, infoString)
}

func handleACL(conn net.Conn, arr []any, store *Store, config ServerConfig) {
	if len(arr) < 2 {
		writeErr(conn, "wrong number of arguments for 'ACL' command")
		return
	}

	subcommand, _ := asString(arr[1])
	switch strings.ToUpper(subcommand) {
	case "WHOAMI":
		writeBulkString(conn, "default")
	default:
		writeErr(conn, "unknown ACL subcommand")
	}
}

func haversineDistanceMeters(lat1, lon1, lat2, lon2 float64) float64 {
	latitude1 := lat1 * (math.Pi / 180)
	longitude1 := lon1 * (math.Pi / 180)
	latitude2 := lat2 * (math.Pi / 180)
	longitude2 := lon2 * (math.Pi / 180)

	deltaLat := latitude2 - latitude1
	deltaLon := longitude2 - longitude1

	a := math.Sin(deltaLat/2)*math.Sin(deltaLat/2) + math.Cos(latitude1)*math.Cos(latitude2)*math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
	if a > 1 {
		a = 1
	}
	if a < 0 {
		a = 0
	}

	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return 6372797.560856 * c
}

func asStringOrEmpty(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
