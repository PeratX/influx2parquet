package tsm1

import (
	"bytes"
	"errors"
	"math"
	"regexp"
	"strconv"
)

const (
	DefaultMaxPointsPerBlock = 1000
	EOF                      = int64(math.MinInt64)
	keyFieldSeparator        = "#!~#"
	CompactionTempExtension  = "tmp"
)

var (
	ErrFieldTypeConflict = errors.New("field type conflict")
	ErrUnknownFieldType  = errors.New("unknown field type")

	keyFieldSeparatorBytes = []byte(keyFieldSeparator)

	tsmFileNamePattern = regexp.MustCompile(`^(\d+)-(\d+)\.tsm$`)
)

type FileStoreObserver interface {
	FileFinishing(path string) error
	FileUnlinking(path string) error
}

type FileStat struct {
	Path             string
	HasTombstone     bool
	Size             uint32
	LastModified     int64
	MinTime, MaxTime int64
	MinKey, MaxKey   []byte
	Generation       int
	Sequence         int
}

func (f FileStat) ToExtFileStat() ExtFileStat {
	return ExtFileStat{FileStat: f}
}

type ExtFileStat struct {
	FileStat
	FirstBlockCount int
}

type TombstoneStat struct {
	TombstoneExists bool
	Path            string
	LastModified    int64
	Size            uint32
}

type noFileStoreObserver struct{}

func (noFileStoreObserver) FileFinishing(string) error { return nil }
func (noFileStoreObserver) FileUnlinking(string) error { return nil }

type ParseFileNameFunc func(name string) (generation, sequence int, err error)

func DefaultParseFileName(name string) (generation, sequence int, err error) {
	matches := tsmFileNamePattern.FindStringSubmatch(name)
	if matches == nil {
		return 0, 0, errors.New("invalid tsm file name")
	}
	generation, err = strconv.Atoi(matches[1])
	if err != nil {
		return 0, 0, err
	}
	sequence, err = strconv.Atoi(matches[2])
	if err != nil {
		return 0, 0, err
	}
	return generation, sequence, nil
}

func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

func SeriesFieldKeyBytes(seriesKey, field string) []byte {
	b := make([]byte, len(seriesKey)+len(keyFieldSeparator)+len(field))
	i := copy(b, seriesKey)
	i += copy(b[i:], keyFieldSeparatorBytes)
	copy(b[i:], field)
	return b
}

func SeriesAndFieldFromCompositeKey(key []byte) ([]byte, []byte) {
	sep := bytes.Index(key, keyFieldSeparatorBytes)
	if sep == -1 {
		return key, nil
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}
