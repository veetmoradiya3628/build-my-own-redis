package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"
)

// parsing code for RDB file

type RDBParser struct {
	r *bufio.Reader
}

func (p *RDBParser) readByte() (byte, error) {
	return p.r.ReadByte()
}

// readLength reads size-encoded values. It returns length, isSpecial (for strings), and error.
func (p *RDBParser) readLength() (uint32, bool, error) {
	b, err := p.readByte()
	if err != nil {
		return 0, false, err
	}

	enc := b >> 6
	switch enc {
	case 0: // 00: The next 6 bits represent the length
		return uint32(b & 0x3F), false, nil
	case 1: // 01: The next 14 bits represent the length
		nextB, err := p.readByte()
		if err != nil {
			return 0, false, err
		}
		return (uint32(b&0x3F) << 8) | uint32(nextB), false, nil
	case 2: // 10: Discard remaining 6 bits. The next 4 bytes represent the length
		var val uint32
		err := binary.Read(p.r, binary.BigEndian, &val)
		return val, false, err
	case 3: // 11: The next object is a special string encoding
		return uint32(b & 0x3F), true, nil
	}
	return 0, false, fmt.Errorf("invalid length encoding")
}

// readString parses string-encoded values, including integers encoded as strings
func (p *RDBParser) readString() (string, error) {
	length, isSpecial, err := p.readLength()
	if err != nil {
		return "", err
	}

	if isSpecial {
		switch length {
		case 0: // 8-bit integer
			b, err := p.readByte()
			return strconv.Itoa(int(int8(b))), err
		case 1: // 16-bit integer
			buf := make([]byte, 2)
			if _, err := io.ReadFull(p.r, buf); err != nil {
				return "", err
			}
			val := int16(binary.LittleEndian.Uint16(buf))
			return strconv.Itoa(int(val)), nil
		case 2: // 32-bit integer
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.r, buf); err != nil {
				return "", err
			}
			val := int32(binary.LittleEndian.Uint32(buf))
			return strconv.Itoa(int(val)), nil
		default:
			return "", fmt.Errorf("unsupported special string encoding: %d", length)
		}
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(p.r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// Update the Parse function signature and implementation
// Parse reads the RDB file format and returns a map of key->value and a map of key->expiry.
// This is a simplified parser focused on the subset needed by the exercise.
func (p *RDBParser) Parse() (map[string]any, map[string]time.Time, error) {
	store := make(map[string]any)
	expiry := make(map[string]time.Time) // New expiry map

	// Skip 9-byte header ("REDIS0011")
	header := make([]byte, 9)
	if _, err := io.ReadFull(p.r, header); err != nil {
		return nil, nil, err
	}

	for {
		b, err := p.readByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}

		switch b {
		case 0xFA: // Metadata subsection
			if _, err := p.readString(); err != nil {
				return nil, nil, err
			}
			if _, err := p.readString(); err != nil {
				return nil, nil, err
			}
		case 0xFE: // Database subsection
			if _, _, err := p.readLength(); err != nil {
				return nil, nil, err
			}
		case 0xFB: // Hash table sizes
			if _, _, err := p.readLength(); err != nil {
				return nil, nil, err
			} // Total keys
			if _, _, err := p.readLength(); err != nil {
				return nil, nil, err
			} // Expire keys
		case 0xFC: // Millisecond expire timestamp
			buf := make([]byte, 8)
			if _, err := io.ReadFull(p.r, buf); err != nil {
				return nil, nil, err
			}
			expiryMs := int64(binary.LittleEndian.Uint64(buf))

			if _, err := p.readByte(); err != nil {
				return nil, nil, err
			} // value type

			key, err := p.readString()
			if err != nil {
				return nil, nil, err
			}

			val, err := p.readString()
			if err != nil {
				return nil, nil, err
			}
			store[key] = val
			expiry[key] = time.UnixMilli(expiryMs) // Store timestamp

		case 0xFD: // Second expire timestamp
			buf := make([]byte, 4)
			if _, err := io.ReadFull(p.r, buf); err != nil {
				return nil, nil, err
			}
			expirySec := int64(binary.LittleEndian.Uint32(buf))

			if _, err := p.readByte(); err != nil {
				return nil, nil, err
			} // value type

			key, err := p.readString()
			if err != nil {
				return nil, nil, err
			}

			val, err := p.readString()
			if err != nil {
				return nil, nil, err
			}
			store[key] = val
			expiry[key] = time.Unix(expirySec, 0) // Store timestamp

		case 0xFF: // End of file
			return store, expiry, nil

		default: // Value type flag
			key, err := p.readString()
			if err != nil {
				return nil, nil, err
			}

			val, err := p.readString()
			if err != nil {
				return nil, nil, err
			}
			store[key] = val
		}
	}
	return store, expiry, nil
}

// Update LoadRDB to return both maps
func LoadRDB(filepath string) (map[string]any, map[string]time.Time) {
	f, err := os.Open(filepath)
	if err != nil {
		slog.Error("failed to open RDB file", "path", filepath, "err", err)
		return make(map[string]any), make(map[string]time.Time)
	}
	defer f.Close()

	parser := &RDBParser{r: bufio.NewReader(f)}
	store, expiry, err := parser.Parse()

	if err != nil {
		slog.Error("error parsing RDB", "err", err)
		if store == nil {
			store = make(map[string]any)
			expiry = make(map[string]time.Time)
		}
	}

	return store, expiry
}
