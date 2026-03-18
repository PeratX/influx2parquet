package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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

func TestAssignRowFieldCompactEncodings(t *testing.T) {
	row := parquetRow{}
	scales := compactBookScales{PxScale: 4, SzScale: 3}

	if err := assignRowField(&row, "action", []byte("snapshot"), scales); err != nil {
		t.Fatalf("assign action: %v", err)
	}
	if row.Action == nil || *row.Action != 1 {
		t.Fatalf("action = %v, want 1", row.Action)
	}

	asks := []byte("[[\"1.25\",\"2.5\",\"0\",\"7\"],[\"1.20\",\"2\",\"1\",\"8\"]]")
	if err := assignRowField(&row, "asks", asks, scales); err != nil {
		t.Fatalf("assign asks: %v", err)
	}
	if row.AsksRaw != nil {
		t.Fatalf("asks raw fallback unexpectedly set: %q", row.AsksRaw)
	}
	book, err := unpackCompactBookPayload(row.Asks)
	if err != nil {
		t.Fatalf("unpack asks: %v", err)
	}
	if got, want := book.Px, []int64{12500, 12000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asks px = %v, want %v", got, want)
	}
	if got, want := book.Sz, []int64{2500, 2000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asks sz = %v, want %v", got, want)
	}
	if got, want := book.Liq, []int64{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asks liq = %v, want %v", got, want)
	}
	if got, want := book.Orders, []int64{7, 8}; !reflect.DeepEqual(got, want) {
		t.Fatalf("asks orders = %v, want %v", got, want)
	}

	if err := assignRowField(&row, "ts", []byte("1700000000000"), scales); err != nil {
		t.Fatalf("assign ts: %v", err)
	}
	if row.TS == nil || *row.TS != 1700000000000 {
		t.Fatalf("ts = %v, want 1700000000000", row.TS)
	}

	var checksum [8]byte
	binary.LittleEndian.PutUint64(checksum[:], uint64(123456789))
	if err := assignRowField(&row, "checksum", checksum[:], scales); err != nil {
		t.Fatalf("assign checksum: %v", err)
	}
	if row.Checksum == nil || *row.Checksum != 123456789 {
		t.Fatalf("checksum = %v, want 123456789", row.Checksum)
	}
}

func TestDetectCompactBookScalesUsesSampleRowsAndSpareDigits(t *testing.T) {
	rows := []pendingBuildRow{
		{
			TimeNS:      1,
			AsksPayload: []byte("[[\"65000.1\",\"12.5\",\"0\",\"1\"]]"),
			HasAsks:     true,
		},
		{
			TimeNS:      2,
			BidsPayload: []byte("[[\"0.12345\",\"0.0001000\",\"0\",\"3\"],[\"0.1\",\"2.5000\",\"1\",\"4\"]]"),
			HasBids:     true,
		},
	}

	if got, want := detectCompactBookScales(rows), (compactBookScales{PxScale: 7, SzScale: 6}); got != want {
		t.Fatalf("detectCompactBookScales() = %+v, want %+v", got, want)
	}
}

func TestCompactBookMetadataIncludesDetectedScale(t *testing.T) {
	metadata := compactBookMetadata("BTC-USDT-SWAP", compactBookScales{PxScale: 7, SzScale: 6})
	if got, want := metadata["okex_depth.asks_px_scale"], "7"; got != want {
		t.Fatalf("asks px scale metadata = %q, want %q", got, want)
	}
	if got, want := metadata["okex_depth.bids_sz_scale"], "6"; got != want {
		t.Fatalf("bids sz scale metadata = %q, want %q", got, want)
	}
	if got, want := metadata["okex_depth.scale_sample_rows"], "10"; got != want {
		t.Fatalf("scale sample rows metadata = %q, want %q", got, want)
	}
	if got, want := metadata["okex_depth.scale_spare_digits"], "2"; got != want {
		t.Fatalf("scale spare digits metadata = %q, want %q", got, want)
	}
	if got, want := metadata["okex_depth.book_encoding"], "packed_i64le_v1"; got != want {
		t.Fatalf("book encoding metadata = %q, want %q", got, want)
	}
}

func TestAssignRowFieldBookFallbackRaw(t *testing.T) {
	row := parquetRow{}
	payload := []byte("[[\"1\",\"2\"]]")
	if err := assignRowField(&row, "bids", payload, compactBookScales{PxScale: 4, SzScale: 4}); err != nil {
		t.Fatalf("assign bids: %v", err)
	}
	if got := string(row.BidsRaw); got != string(payload) {
		t.Fatalf("bids raw = %q, want %q", got, payload)
	}
	if row.Bids != nil {
		t.Fatalf("packed bids column should stay nil on raw fallback")
	}
}

func TestWalkCompactBookLevelsParsesWhitespace(t *testing.T) {
	payload := []byte(" [ [ \"65000.1\" , \"12.5\" , \"0\" , \"1\" ] , [ \"64999.9\" , \"0.0001000\" , \"1\" , \"4\" ] ] ")
	var got [][]string
	err := walkCompactBookLevels(payload, func(pxText, szText, liqText, ordersText []byte) error {
		got = append(got, []string{
			string(pxText),
			string(szText),
			string(liqText),
			string(ordersText),
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walkCompactBookLevels() error = %v", err)
	}
	want := [][]string{
		{"65000.1", "12.5", "0", "1"},
		{"64999.9", "0.0001000", "1", "4"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkCompactBookLevels() = %v, want %v", got, want)
	}
}

func TestWalkCompactBookLevelsEmptyBook(t *testing.T) {
	calls := 0
	err := walkCompactBookLevels([]byte(" [  ] "), func(_, _, _, _ []byte) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("walkCompactBookLevels() error = %v", err)
	}
	if calls != 0 {
		t.Fatalf("walkCompactBookLevels() callback calls = %d, want 0", calls)
	}
}

func TestWalkCompactBookLevelsRejectsMalformedPayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name:    "missing-field",
			payload: "[[\"1\",\"2\",\"0\"]]",
			wantErr: "expected ',' after liq",
		},
		{
			name:    "extra-field",
			payload: "[[\"1\",\"2\",\"0\",\"1\",\"extra\"]]",
			wantErr: "expected ']' after level",
		},
		{
			name:    "escaped-string",
			payload: "[[\"1\\u0031\",\"2\",\"0\",\"1\"]]",
			wantErr: "escaped string unsupported",
		},
		{
			name:    "trailing-data",
			payload: "[[\"1\",\"2\",\"0\",\"1\"]]x",
			wantErr: "trailing data",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := walkCompactBookLevels([]byte(tt.payload), func(_, _, _, _ []byte) error {
				return nil
			})
			if err == nil {
				t.Fatalf("walkCompactBookLevels() error = nil, want substring %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("walkCompactBookLevels() error = %q, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestPackCompactBookPayloadEmptyBook(t *testing.T) {
	packed, err := packCompactBookPayload([]byte(" [ ] "), compactBookScales{PxScale: 4, SzScale: 4})
	if err != nil {
		t.Fatalf("packCompactBookPayload() error = %v", err)
	}
	book, err := unpackCompactBookPayload(packed)
	if err != nil {
		t.Fatalf("unpackCompactBookPayload() error = %v", err)
	}
	if len(book.Px) != 0 || len(book.Sz) != 0 || len(book.Liq) != 0 || len(book.Orders) != 0 {
		t.Fatalf("unpackCompactBookPayload() = %+v, want empty slices", book)
	}
}

func TestDetectCompactBookSideScaleTrimsTrailingZeros(t *testing.T) {
	payload := []byte("[[\"1.2300\",\"0.0100\",\"0\",\"1\"],[\"2\",\"3.000\",\"1\",\"2\"]]")
	got, err := detectCompactBookSideScale(payload)
	if err != nil {
		t.Fatalf("detectCompactBookSideScale() error = %v", err)
	}
	want := compactBookScales{PxScale: 2, SzScale: 2}
	if got != want {
		t.Fatalf("detectCompactBookSideScale() = %+v, want %+v", got, want)
	}
}

func TestPackCompactBookPayloadRoundTrip(t *testing.T) {
	payload := []byte("[[\"65000.1234\",\"12.5000\",\"0\",\"1\"],[\"64999.9\",\"0.0001000\",\"1\",\"4\"]]")
	packed, err := packCompactBookPayload(payload, compactBookScales{PxScale: 7, SzScale: 6})
	if err != nil {
		t.Fatalf("packCompactBookPayload() error = %v", err)
	}
	if got, want := packed[0], byte(compactBookPackedVersion); got != want {
		t.Fatalf("packed version = %d, want %d", got, want)
	}
	book, err := unpackCompactBookPayload(packed)
	if err != nil {
		t.Fatalf("unpackCompactBookPayload() error = %v", err)
	}
	if got, want := book.Px, []int64{650001234000, 649999000000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("px = %v, want %v", got, want)
	}
	if got, want := book.Sz, []int64{12500000, 100}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sz = %v, want %v", got, want)
	}
	if got, want := book.Liq, []int64{0, 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("liq = %v, want %v", got, want)
	}
	if got, want := book.Orders, []int64{1, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("orders = %v, want %v", got, want)
	}
}

func TestUnpackCompactBookPayloadRejectsMalformed(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr string
	}{
		{name: "too-short", payload: []byte{1, 0, 0}, wantErr: "too short"},
		{name: "bad-version", payload: []byte{2, 0, 0, 0, 0}, wantErr: "unsupported packed book version"},
		{name: "bad-size", payload: []byte{1, 1, 0, 0, 0}, wantErr: "size mismatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := unpackCompactBookPayload(tt.payload)
			if err == nil {
				t.Fatalf("unpackCompactBookPayload() error = nil, want substring %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("unpackCompactBookPayload() error = %q, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseScaledDecimalBytes(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		scale int32
		want  int64
	}{
		{name: "whole", text: "12", scale: 3, want: 12000},
		{name: "trimmed-fraction", text: "1.2300", scale: 4, want: 12300},
		{name: "negative", text: "-0.5", scale: 3, want: -500},
		{name: "leading-dot", text: ".25", scale: 4, want: 2500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseScaledDecimalBytes([]byte(tt.text), tt.scale)
			if err != nil {
				t.Fatalf("parseScaledDecimalBytes() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseScaledDecimalBytes() = %d, want %d", got, tt.want)
			}
		})
	}
}

func BenchmarkPackCompactBookPayload(b *testing.B) {
	payload := []byte("[[\"65000.1234\",\"12.5000\",\"0\",\"1\"],[\"64999.9\",\"0.0001000\",\"1\",\"4\"],[\"64999.8\",\"0.0100\",\"0\",\"2\"]]")
	scales := compactBookScales{PxScale: 7, SzScale: 6}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := packCompactBookPayload(payload, scales); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnpackCompactBookPayload(b *testing.B) {
	packed, err := packCompactBookPayload(
		[]byte("[[\"65000.1234\",\"12.5000\",\"0\",\"1\"],[\"64999.9\",\"0.0001000\",\"1\",\"4\"],[\"64999.8\",\"0.0100\",\"0\",\"2\"]]"),
		compactBookScales{PxScale: 7, SzScale: 6},
	)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := unpackCompactBookPayload(packed); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDetectCompactBookSideScale(b *testing.B) {
	payload := []byte("[[\"65000.1234\",\"12.5000\",\"0\",\"1\"],[\"64999.9\",\"0.0001000\",\"1\",\"4\"],[\"64999.8\",\"0.0100\",\"0\",\"2\"]]")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := detectCompactBookSideScale(payload); err != nil {
			b.Fatal(err)
		}
	}
}
