package tsm1

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/golang/snappy"
)

const (
	float64EntryType  = 1
	integerEntryType  = 2
	booleanEntryType  = 3
	stringEntryType   = 4
	unsignedEntryType = 5
)

type WalEntryType byte

const (
	WriteWALEntryType       WalEntryType = 0x01
	DeleteWALEntryType      WalEntryType = 0x02
	DeleteRangeWALEntryType WalEntryType = 0x03
)

var ErrWALCorrupt = fmt.Errorf("corrupted WAL entry")

type WALEntry interface {
	UnmarshalBinary(b []byte) error
}

type WriteWALEntry struct {
	Values map[string][]Value
}

func (w *WriteWALEntry) UnmarshalBinary(b []byte) error {
	if w.Values == nil {
		w.Values = make(map[string][]Value)
	}
	var i int
	for i < len(b) {
		typ := b[i]
		i++

		if i+2 > len(b) {
			return ErrWALCorrupt
		}
		length := int(binary.BigEndian.Uint16(b[i : i+2]))
		i += 2
		if i+length > len(b) {
			return ErrWALCorrupt
		}
		key := string(b[i : i+length])
		i += length

		if i+4 > len(b) {
			return ErrWALCorrupt
		}
		nvals := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4
		if nvals <= 0 || nvals > len(b) {
			return ErrWALCorrupt
		}

		switch typ {
		case float64EntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				unixNano := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				value := math.Float64frombits(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, NewFloatValue(unixNano, value))
			}
			w.Values[key] = values
		case integerEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				unixNano := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				value := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				values = append(values, NewIntegerValue(unixNano, value))
			}
			w.Values[key] = values
		case unsignedEntryType:
			if i+16*nvals > len(b) {
				return ErrWALCorrupt
			}
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				unixNano := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				value := binary.BigEndian.Uint64(b[i : i+8])
				i += 8
				values = append(values, NewUnsignedValue(unixNano, value))
			}
			w.Values[key] = values
		case booleanEntryType:
			if i+9*nvals > len(b) {
				return ErrWALCorrupt
			}
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				unixNano := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				value := b[i] == 1
				i++
				values = append(values, NewBooleanValue(unixNano, value))
			}
			w.Values[key] = values
		case stringEntryType:
			values := make([]Value, 0, nvals)
			for j := 0; j < nvals; j++ {
				if i+12 > len(b) {
					return ErrWALCorrupt
				}
				unixNano := int64(binary.BigEndian.Uint64(b[i : i+8]))
				i += 8
				length := int(binary.BigEndian.Uint32(b[i : i+4]))
				i += 4
				if i+length > len(b) {
					return ErrWALCorrupt
				}
				value := string(b[i : i+length])
				i += length
				values = append(values, NewStringValue(unixNano, value))
			}
			w.Values[key] = values
		default:
			return fmt.Errorf("unsupported WAL value type %#v", typ)
		}
	}
	return nil
}

type DeleteWALEntry struct {
	Keys [][]byte
}

func (w *DeleteWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	buf := make([]byte, len(b))
	copy(buf, b)
	w.Keys = bytes.Split(buf, []byte("\n"))
	return nil
}

type DeleteRangeWALEntry struct {
	Keys     [][]byte
	Min, Max int64
}

func (w *DeleteRangeWALEntry) UnmarshalBinary(b []byte) error {
	if len(b) < 16 {
		return ErrWALCorrupt
	}
	w.Min = int64(binary.BigEndian.Uint64(b[:8]))
	w.Max = int64(binary.BigEndian.Uint64(b[8:16]))
	i := 16
	for i < len(b) {
		if i+4 > len(b) {
			return ErrWALCorrupt
		}
		sz := int(binary.BigEndian.Uint32(b[i : i+4]))
		i += 4
		if i+sz > len(b) {
			return ErrWALCorrupt
		}
		buf := make([]byte, sz)
		copy(buf, b[i:i+sz])
		w.Keys = append(w.Keys, buf)
		i += sz
	}
	return nil
}

type WALSegmentReader struct {
	rc    io.ReadCloser
	r     *bufio.Reader
	entry WALEntry
	n     int64
	err   error
}

func NewWALSegmentReader(r io.ReadCloser) *WALSegmentReader {
	return &WALSegmentReader{
		rc: r,
		r:  bufio.NewReader(r),
	}
}

func (r *WALSegmentReader) Reset(rc io.ReadCloser) {
	r.rc = rc
	r.r.Reset(rc)
	r.entry = nil
	r.n = 0
	r.err = nil
}

func (r *WALSegmentReader) Next() bool {
	var nReadOK int
	var lv [5]byte
	n, err := io.ReadFull(r.r, lv[:])
	if err == io.EOF {
		return false
	}
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	entryType := lv[0]
	length := binary.BigEndian.Uint32(lv[1:5])

	b := *(getBuf(int(length)))
	defer putBuf(&b)

	n, err = io.ReadFull(r.r, b[:length])
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	decodedLen, err := snappy.DecodedLen(b[:length])
	if err != nil {
		r.err = err
		return true
	}
	decodedBuf := *(getBuf(decodedLen))
	defer putBuf(&decodedBuf)

	data, err := snappy.Decode(decodedBuf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	switch WalEntryType(entryType) {
	case WriteWALEntryType:
		r.entry = &WriteWALEntry{Values: make(map[string][]Value)}
	case DeleteWALEntryType:
		r.entry = &DeleteWALEntry{}
	case DeleteRangeWALEntryType:
		r.entry = &DeleteRangeWALEntry{}
	default:
		r.err = fmt.Errorf("unknown wal entry type: %v", entryType)
		return true
	}
	r.err = r.entry.UnmarshalBinary(data)
	if r.err == nil {
		r.n += int64(nReadOK)
	}
	return true
}

func (r *WALSegmentReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}

func (r *WALSegmentReader) Count() int64 {
	return r.n
}

func (r *WALSegmentReader) Close() error {
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}

