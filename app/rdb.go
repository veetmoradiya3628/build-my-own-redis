package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
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

// Parse extracts all key-value pairs from the RDB database section
func (p *RDBParser) Parse() (map[string]any, error) {
	// Initialize the map using 'any'
	store := make(map[string]any)

	// Skip 9-byte header ("REDIS0011")
	header := make([]byte, 9)
	if _, err := io.ReadFull(p.r, header); err != nil {
		return nil, err
	}

	for {
		b, err := p.readByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		switch b {
		case 0xFA: // Metadata subsection
			if _, err := p.readString(); err != nil {
				return nil, err
			}
			if _, err := p.readString(); err != nil {
				return nil, err
			}
		case 0xFE: // Database subsection
			if _, _, err := p.readLength(); err != nil {
				return nil, err
			}
		case 0xFB: // Hash table sizes
			if _, _, err := p.readLength(); err != nil {
				return nil, err
			} // Total keys
			if _, _, err := p.readLength(); err != nil {
				return nil, err
			} // Expire keys
		case 0xFC: // Millisecond expire timestamp
			if _, err := io.ReadFull(p.r, make([]byte, 8)); err != nil {
				return nil, err
			}
			if _, err := p.readByte(); err != nil {
				return nil, err
			} // value type

			key, err := p.readString()
			if err != nil {
				return nil, err
			}

			// Capture the value
			val, err := p.readString()
			if err != nil {
				return nil, err
			}
			store[key] = val

		case 0xFD: // Second expire timestamp
			if _, err := io.ReadFull(p.r, make([]byte, 4)); err != nil {
				return nil, err
			}
			if _, err := p.readByte(); err != nil {
				return nil, err
			} // value type

			key, err := p.readString()
			if err != nil {
				return nil, err
			}

			// Capture the value
			val, err := p.readString()
			if err != nil {
				return nil, err
			}
			store[key] = val

		case 0xFF: // End of file
			return store, nil

		default: // Value type flag (start of a key-value pair without expiry)
			// 'b' is the value type flag (e.g., 0x00 for string)
			key, err := p.readString()
			if err != nil {
				return nil, err
			}

			// Capture the value
			val, err := p.readString()
			if err != nil {
				return nil, err
			}
			store[key] = val
		}
	}
	return store, nil
}

// LoadRDB parses the RDB file and returns it as a map[string]any
func LoadRDB(filepath string) map[string]any {
	f, err := os.Open(filepath)
	if err != nil {
		// File doesn't exist, return an empty map
		return make(map[string]any)
	}
	defer f.Close()

	parser := &RDBParser{r: bufio.NewReader(f)}
	store, err := parser.Parse()

	if err != nil {
		fmt.Printf("Error parsing RDB: %v\n", err)
		if store == nil {
			store = make(map[string]any)
		}
	}

	return store
}
