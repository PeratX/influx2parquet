package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		value int64
		want  string
	}{
		{value: 999, want: "999 B"},
		{value: 1024, want: "1.00 KB"},
		{value: 1024 * 1024, want: "1.00 MB"},
		{value: 1024 * 1024 * 1024, want: "1.00 GB"},
		{value: 1790 * 1024 * 1024 * 1024, want: "1.75 TB"},
	}

	for _, tt := range tests {
		if got := humanBytes(tt.value); got != tt.want {
			t.Fatalf("humanBytes(%d) = %q, want %q", tt.value, got, tt.want)
		}
	}
}

func TestHumanProgress(t *testing.T) {
	tests := []struct {
		current int64
		total   int64
		want    string
	}{
		{current: 1024, total: 2048, want: "1.00 KB/2.00 KB"},
		{current: 1790 * 1024 * 1024 * 1024, total: 2 * 1024 * 1024 * 1024 * 1024, want: "1.75 TB/2.00 TB"},
		{current: 999, total: 0, want: "999 B"},
	}

	for _, tt := range tests {
		if got := humanProgress(tt.current, tt.total); got != tt.want {
			t.Fatalf("humanProgress(%d, %d) = %q, want %q", tt.current, tt.total, got, tt.want)
		}
	}
}

func TestResolveWorkLayoutDefaults(t *testing.T) {
	cfg := config{OutputDir: "/tmp/out"}
	layout, err := resolveWorkLayout(cfg)
	if err != nil {
		t.Fatalf("resolveWorkLayout() error = %v", err)
	}
	if got, want := layout.MetaRoot, filepath.Join("/tmp/out", measurement); got != want {
		t.Fatalf("MetaRoot = %q, want %q", got, want)
	}
	if got, want := layout.RawRoot, filepath.Join(layout.MetaRoot, rawDirName); got != want {
		t.Fatalf("RawRoot = %q, want %q", got, want)
	}
	if got, want := layout.MergedRoot, filepath.Join(layout.MetaRoot, mergedDirName); got != want {
		t.Fatalf("MergedRoot = %q, want %q", got, want)
	}
	if got, want := layout.BuildRoot, filepath.Join(layout.MetaRoot, buildDirName); got != want {
		t.Fatalf("BuildRoot = %q, want %q", got, want)
	}
	if got, want := layout.FinalRoot, layout.MetaRoot; got != want {
		t.Fatalf("FinalRoot = %q, want %q", got, want)
	}
}

func TestResolveWorkLayoutSplitRoots(t *testing.T) {
	cfg := config{
		OutputDir: "/tmp/meta",
		RawDir:    "/mnt/rawdisk",
		MergedDir: "/mnt/mergeddisk",
		BuildDir:  "/mnt/builddisk",
		FinalDir:  "/mnt/finaldisk",
	}
	layout, err := resolveWorkLayout(cfg)
	if err != nil {
		t.Fatalf("resolveWorkLayout() error = %v", err)
	}
	if got, want := layout.RawRoot, filepath.Join("/mnt/rawdisk", measurement, rawDirName); got != want {
		t.Fatalf("RawRoot = %q, want %q", got, want)
	}
	if got, want := layout.MergedRoot, filepath.Join("/mnt/mergeddisk", measurement, mergedDirName); got != want {
		t.Fatalf("MergedRoot = %q, want %q", got, want)
	}
	if got, want := layout.BuildRoot, filepath.Join("/mnt/builddisk", measurement, buildDirName); got != want {
		t.Fatalf("BuildRoot = %q, want %q", got, want)
	}
	if got, want := layout.FinalRoot, filepath.Join("/mnt/finaldisk", measurement); got != want {
		t.Fatalf("FinalRoot = %q, want %q", got, want)
	}
}

func TestMergeSingleFieldStagedCreatesDestDirAndRenames(t *testing.T) {
	tempDir := t.TempDir()
	paths := workLayout{
		MetaRoot:   filepath.Join(tempDir, "meta"),
		RawRoot:    filepath.Join(tempDir, "raw"),
		MergedRoot: filepath.Join(tempDir, "merged"),
		BuildRoot:  filepath.Join(tempDir, "build"),
		FinalRoot:  filepath.Join(tempDir, "final"),
	}
	for _, path := range []string{paths.MetaRoot, paths.RawRoot, paths.MergedRoot, paths.BuildRoot, paths.FinalRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", path, err)
		}
	}

	cfg := config{
		IntermediateCompressionLevel: 3,
		MergeFanIn:                   40,
		ProgressInterval:             24 * time.Hour,
	}

	const instID = "BTC-USDT-SWAP"
	const field = "action"
	sources := make([]sourceFieldFragment, 0, 41)
	for i := 0; i < 41; i++ {
		sourceID := fmt.Sprintf("src-%03d", i)
		baseDir := filepath.Join(paths.RawRoot, sourceID)
		if err := os.MkdirAll(filepath.Join(baseDir, instID), 0o755); err != nil {
			t.Fatalf("mkdir raw dir: %v", err)
		}
		rawPath := rawFieldPath(baseDir, instID, field)
		writer, err := newSpoolWriter(rawPath, fieldKind(field), cfg.IntermediateCompressionLevel)
		if err != nil {
			t.Fatalf("newSpoolWriter(%s): %v", rawPath, err)
		}
		payload := []byte(fmt.Sprintf("value-%03d", i))
		if err := writer.WriteRecord(int64(1000+i), payload); err != nil {
			t.Fatalf("WriteRecord(%s): %v", rawPath, err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("Close(%s): %v", rawPath, err)
		}
		sources = append(sources, sourceFieldFragment{
			FileID:     sourceID,
			Path:       rawPath,
			Kind:       sourceKindTSM,
			ShardID:    i,
			Generation: 1,
			Sequence:   1,
		})
	}

	result, err := mergeSingleField(paths, cfg, instID, field, sources)
	if err != nil {
		t.Fatalf("mergeSingleField() error = %v", err)
	}
	if got, want := result.RowCount, int64(len(sources)); got != want {
		t.Fatalf("RowCount = %d, want %d", got, want)
	}

	destPath := mergedFieldPath(paths, instID, field)
	if _, err := os.Stat(destPath); err != nil {
		t.Fatalf("merged output missing at %s: %v", destPath, err)
	}
	if _, err := os.Stat(filepath.Join(paths.MergedRoot, ".merge_tmp")); err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat .merge_tmp: %v", err)
	}

	reader, err := newSpoolReader(destPath)
	if err != nil {
		t.Fatalf("newSpoolReader(%s): %v", destPath, err)
	}
	defer reader.Close()

	count := 0
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("ReadRecord(): %v", err)
		}
		if got, want := string(record.Payload), fmt.Sprintf("value-%03d", count); got != want {
			t.Fatalf("payload[%d] = %q, want %q", count, got, want)
		}
		count++
	}
	if count != len(sources) {
		t.Fatalf("record count = %d, want %d", count, len(sources))
	}
}

func TestAssignRowFieldJSONSchema(t *testing.T) {
	row := parquetRow{}

	if err := assignRowField(&row, "action", []byte("snapshot")); err != nil {
		t.Fatalf("assign action: %v", err)
	}
	if row.Action == nil || *row.Action != "snapshot" {
		t.Fatalf("action = %v, want snapshot", row.Action)
	}

	asks := []byte("[[\"1.25\",\"2.5\",\"0\",\"7\"],[\"1.20\",\"2\",\"1\",\"8\"]]")
	if err := assignRowField(&row, "asks", asks); err != nil {
		t.Fatalf("assign asks: %v", err)
	}
	if row.Asks == nil || *row.Asks != string(asks) {
		t.Fatalf("asks = %v, want %q", row.Asks, asks)
	}

	bids := []byte("[[\"1.10\",\"3\",\"0\",\"2\"]]")
	if err := assignRowField(&row, "bids", bids); err != nil {
		t.Fatalf("assign bids: %v", err)
	}
	if row.Bids == nil || *row.Bids != string(bids) {
		t.Fatalf("bids = %v, want %q", row.Bids, bids)
	}

	if err := assignRowField(&row, "ts", []byte("1700000000000")); err != nil {
		t.Fatalf("assign ts: %v", err)
	}
	if row.TS == nil || *row.TS != "1700000000000" {
		t.Fatalf("ts = %v, want 1700000000000", row.TS)
	}

	var checksum [8]byte
	binary.LittleEndian.PutUint64(checksum[:], uint64(123456789))
	if err := assignRowField(&row, "checksum", checksum[:]); err != nil {
		t.Fatalf("assign checksum: %v", err)
	}
	if row.Checksum == nil || *row.Checksum != 123456789 {
		t.Fatalf("checksum = %v, want 123456789", row.Checksum)
	}
}

func TestAssignRowFieldRejectsBadChecksumPayload(t *testing.T) {
	row := parquetRow{}
	if err := assignRowField(&row, "checksum", []byte{1, 2, 3}); err == nil {
		t.Fatalf("assignRowField(checksum) error = nil, want non-nil")
	}
}
