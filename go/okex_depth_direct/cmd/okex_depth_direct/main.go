package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"influx2parquet/internal/influxlite/tsm1"

	"github.com/influxdata/influxdb/models"
	"github.com/klauspost/compress/zstd"
	parquet "github.com/parquet-go/parquet-go"
	parquetzstd "github.com/parquet-go/parquet-go/compress/zstd"
	"golang.org/x/sync/errgroup"
)

const (
	measurement            = "okex_depth"
	defaultDatabase        = "OptionData"
	defaultRetention       = "autogen"
	defaultStart           = "2022-05-26T08:50:23.185000000Z"
	defaultEnd             = "2023-11-21T04:31:03.229056001Z"
	goPipelineVersion      = 1
	stateFileName          = "_pipeline_state_go.json"
	scanIndexFileName      = "_tsm_index.json"
	rawDirName             = "_raw_go"
	mergedDirName          = "_merged_go"
	buildDirName           = "_build_go"
	spoolExt               = ".bin.zst"
	spoolMagic             = "OKDSP01\n"
	spoolKindString   byte = 1
	spoolKindInt64    byte = 2
	sourceKindTSM          = "tsm"
	sourceKindWAL          = "wal"
	// Keep zstd spool streams on a bounded-memory profile. We trade some peak
	// throughput for predictable RAM usage when a source opens many field
	// writers concurrently or when merge/build opens many readers at once.
	spoolZstdWindowSize       = 1 << 20
	spoolZstdDecoderMaxWindow = 128 << 20
	spoolZstdDecoderMaxMemory = 256 << 20
	buildMaxRowsPerRowGroup   = 65536
	parquetPageBufferSize     = 1 << 20
	parquetWriteBufferSize    = 1 << 20
)

var (
	fieldNames    = []string{"action", "asks", "bids", "checksum", "ts"}
	stringFields  = map[string]struct{}{"action": {}, "asks": {}, "bids": {}, "ts": {}}
	targetFields  = map[string]struct{}{"action": {}, "asks": {}, "bids": {}, "checksum": {}, "ts": {}}
	errNoScan     = errors.New("existing scan result not found")
	errBadSpool   = errors.New("invalid spool file")
	errNoInstID   = errors.New("instId tag missing")
	errSkipSource = errors.New("source has no selected fields")
)

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	*s = append(*s, value)
	return nil
}

type config struct {
	Database                     string
	Retention                    string
	DataDir                      string
	WALDir                       string
	OutputDir                    string
	RawDir                       string
	MergedDir                    string
	BuildDir                     string
	FinalDir                     string
	Start                        string
	End                          string
	StartNS                      int64
	EndNS                        int64
	InstIDs                      []string
	RowsPerFile                  int
	Compression                  string
	CompressionLevel             int
	IntermediateCompressionLevel int
	Workers                      int
	BuildWorkers                 int
	MergeFanIn                   int
	MaxOpenSpoolWriters          int
	ProgressInterval             time.Duration
	Overwrite                    bool
	StopAfter                    string
}

type workLayout struct {
	MetaRoot   string
	RawRoot    string
	MergedRoot string
	BuildRoot  string
	FinalRoot  string
}

type influxTree struct {
	InputRoot     string
	DatabaseRoot  string
	RetentionRoot string
	Layout        string
}

type scanIndex struct {
	Measurement     string         `json:"measurement"`
	Database        string         `json:"database"`
	Retention       string         `json:"retention"`
	DataDir         string         `json:"data_dir"`
	DataLayout      string         `json:"data_layout"`
	DatabaseRoot    string         `json:"database_root"`
	RetentionRoot   string         `json:"retention_root"`
	CreatedAt       string         `json:"created_at"`
	ScanComplete    bool           `json:"scan_complete"`
	TotalTSMFiles   int            `json:"total_tsm_files"`
	TotalTSMBytes   int64          `json:"total_tsm_bytes"`
	MatchedTSMFiles int            `json:"matched_tsm_files"`
	MatchedTSMBytes int64          `json:"matched_tsm_bytes"`
	InstIDs         []string       `json:"inst_ids"`
	Files           []scanFileInfo `json:"files"`
}

type scanFileInfo struct {
	FileID     string   `json:"file_id"`
	RelPath    string   `json:"rel_path"`
	ShardID    int      `json:"shard_id"`
	SizeBytes  int64    `json:"size_bytes"`
	Generation int      `json:"generation"`
	Sequence   int      `json:"sequence"`
	InstIDs    []string `json:"inst_ids"`
}

type walFileInfo struct {
	RelPath   string `json:"rel_path"`
	AbsPath   string `json:"abs_path"`
	SegmentID int    `json:"segment_id"`
	SizeBytes int64  `json:"size_bytes"`
}

type sourceRef struct {
	ID         string        `json:"id"`
	Kind       string        `json:"kind"`
	RelPath    string        `json:"rel_path,omitempty"`
	AbsPath    string        `json:"abs_path,omitempty"`
	RelPaths   []string      `json:"rel_paths,omitempty"`
	AbsPaths   []string      `json:"abs_paths,omitempty"`
	WALFiles   []walFileInfo `json:"wal_files,omitempty"`
	ShardID    int           `json:"shard_id"`
	SizeBytes  int64         `json:"size_bytes"`
	Generation int           `json:"generation"`
	Sequence   int           `json:"sequence"`
	InstIDs    []string      `json:"inst_ids,omitempty"`
}

type pipelineMeta struct {
	PipelineVersion         int      `json:"pipeline_version"`
	PipelineName            string   `json:"pipeline_name"`
	Measurement             string   `json:"measurement"`
	Database                string   `json:"database"`
	Retention               string   `json:"retention"`
	DataDir                 string   `json:"data_dir"`
	WALDir                  string   `json:"wal_dir"`
	DataLayout              string   `json:"data_layout"`
	WALLayout               string   `json:"wal_layout"`
	DatabaseRoot            string   `json:"database_root"`
	RetentionRoot           string   `json:"retention_root"`
	WALRetentionRoot        string   `json:"wal_retention_root"`
	Start                   string   `json:"start"`
	End                     string   `json:"end"`
	StartNS                 int64    `json:"start_ns"`
	EndNS                   int64    `json:"end_ns"`
	InstIDs                 []string `json:"inst_ids"`
	IntermediateFormat      string   `json:"intermediate_format"`
	IntermediateCompression string   `json:"intermediate_compression"`
	MetaRoot                string   `json:"meta_root"`
	RawRoot                 string   `json:"raw_root"`
	MergedRoot              string   `json:"merged_root"`
	BuildRoot               string   `json:"build_root"`
	FinalRoot               string   `json:"final_root"`
	ScanIndexPath           string   `json:"scan_index_path"`
}

type fileState struct {
	Source        sourceRef `json:"source"`
	ExportStatus  string    `json:"export_status"`
	ExportRecords int64     `json:"export_records"`
	ExportBytes   int64     `json:"export_bytes"`
	MinTimeNS     *int64    `json:"min_time_ns"`
	MaxTimeNS     *int64    `json:"max_time_ns"`
}

type mergeFieldState struct {
	Status      string   `json:"status"`
	RowCount    int64    `json:"row_count"`
	SourceCount int      `json:"source_count"`
	SourceFiles []string `json:"source_files"`
	DestPath    string   `json:"dest_path"`
}

type buildState struct {
	Status           string                          `json:"status"`
	Rows             int64                           `json:"rows"`
	PartCount        int                             `json:"part_count"`
	BytesWritten     int64                           `json:"bytes_written"`
	MinTimeNS        *int64                          `json:"min_time_ns"`
	MaxTimeNS        *int64                          `json:"max_time_ns"`
	MergedSignature  map[string]mergedFieldSignature `json:"merged_signature"`
	RowsPerFile      int                             `json:"rows_per_file"`
	Compression      string                          `json:"compression"`
	CompressionLevel int                             `json:"compression_level"`
}

type pipelineState struct {
	Meta      pipelineMeta                          `json:"meta"`
	CreatedAt string                                `json:"created_at"`
	UpdatedAt string                                `json:"updated_at"`
	Files     map[string]*fileState                 `json:"files"`
	Merge     map[string]map[string]mergeFieldState `json:"merge"`
	Build     map[string]buildState                 `json:"build"`
}

type exportManifest struct {
	SourceID          string                      `json:"source_id"`
	Kind              string                      `json:"kind"`
	RawDir            string                      `json:"raw_dir"`
	Records           int64                       `json:"records"`
	BytesRead         int64                       `json:"bytes_read"`
	FieldRecordCounts map[string]map[string]int64 `json:"field_record_counts"`
	MinTimeNS         *int64                      `json:"min_time_ns"`
	MaxTimeNS         *int64                      `json:"max_time_ns"`
	ElapsedSeconds    float64                     `json:"elapsed_seconds"`
}

type mergeResult struct {
	InstID      string
	FieldName   string
	DestPath    string
	RowCount    int64
	SourceCount int
	SourceFiles []string
	Elapsed     time.Duration
}

type mergedFieldSignature struct {
	RowCount    int64    `json:"row_count"`
	SourceFiles []string `json:"source_files"`
}

type buildResult struct {
	InstID          string
	StageDataset    string
	Rows            int64
	PartCount       int
	BytesWritten    int64
	MinTimeNS       *int64
	MaxTimeNS       *int64
	MergedSignature map[string]mergedFieldSignature
	Elapsed         time.Duration
}

type sourceFieldFragment struct {
	FileID     string
	Path       string
	Kind       string
	ShardID    int
	Generation int
	Sequence   int
}

type spoolRecord struct {
	TimestampNS int64
	Payload     []byte
}

type spoolWriter struct {
	file *os.File
	zw   *zstd.Encoder
	bw   *bufio.Writer
	kind byte
}

type spoolReader struct {
	file *os.File
	zr   *zstd.Decoder
	br   *bufio.Reader
	kind byte
}

type collapsedSpoolReader struct {
	reader  *spoolReader
	current *spoolRecord
	pending *spoolRecord
}

type exportProgress struct {
	Source      string
	Records     int64
	BytesRead   int64
	TotalBytes  int64
	LastInstID  string
	LastTimeNS  int64
	HasLastTime bool
	StartedAt   time.Time
	LastPrinted time.Time
	Every       time.Duration
}

type mergeProgress struct {
	InstID       string
	FieldName    string
	Stage        int
	BatchIndex   int
	BatchTotal   int
	Rows         int64
	LogicalBytes int64
	SourceCount  int
	LastTimeNS   int64
	HasLastTime  bool
	StartedAt    time.Time
	LastPrinted  time.Time
	Every        time.Duration
}

type buildProgress struct {
	InstID       string
	Rows         int64
	PartCount    int
	BytesWritten int64
	LastTimeNS   int64
	HasLastTime  bool
	StartedAt    time.Time
	LastPrinted  time.Time
	Every        time.Duration
}

type parquetRow struct {
	Time     int64   `parquet:"time,timestamp(nanosecond:utc)"`
	Action   *string `parquet:"action,optional"`
	Asks     *string `parquet:"asks,optional"`
	Bids     *string `parquet:"bids,optional"`
	Checksum *int64  `parquet:"checksum,optional"`
	TS       *string `parquet:"ts,optional"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "[error] %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}

	paths, err := resolveWorkLayout(cfg)
	if err != nil {
		return err
	}
	if err := ensureRoot(paths, cfg.Overwrite); err != nil {
		return err
	}

	index, err := loadScanIndex(paths.MetaRoot)
	if err != nil {
		return err
	}
	if !index.ScanComplete {
		return fmt.Errorf("existing scan index is not complete: %s", scanIndexPath(paths.MetaRoot))
	}

	targetInstIDs, err := determineTargetInstIDs(index.InstIDs, cfg.InstIDs)
	if err != nil {
		return err
	}

	var (
		dataTree   influxTree
		walTree    influxTree
		tsmSources []sourceRef
		walSources []sourceRef
		sources    []sourceRef
		state      *pipelineState
	)
	if phaseNeedsSourceRoots(cfg.StopAfter) {
		dataTree, err = resolveInfluxTree(cfg.DataDir, cfg.Database, cfg.Retention)
		if err != nil {
			return err
		}
		walTree, err = resolveInfluxTree(cfg.WALDir, cfg.Database, cfg.Retention)
		if err != nil {
			return err
		}
		tsmSources, err = tsmSourcesFromScan(index, dataTree, targetInstIDs)
		if err != nil {
			return err
		}
		walSources, err = walSourcesForMatchedShards(walTree, tsmSources)
		if err != nil {
			return err
		}
		sources = append(tsmSources, walSources...)
		sort.Slice(sources, func(i, j int) bool { return sourceLess(sources[i], sources[j]) })

		state, err = ensureState(paths, cfg, dataTree, walTree, targetInstIDs, sources)
		if err != nil {
			return err
		}
	} else {
		state, err = prepareExistingStateForOfflinePhase(paths, cfg)
		if err != nil {
			return err
		}
		tsmSources, walSources = sourceCountsFromState(state, targetInstIDs)
	}

	fmt.Printf(
		"[plan] matched_tsm=%d matched_tsm_size=%s wal_sources=%d workers=%d build_workers=%d max_open_writers=%d root=%s\n",
		len(tsmSources),
		humanBytes(index.MatchedTSMBytes),
		len(walSources),
		max(1, cfg.Workers),
		max(1, cfg.BuildWorkers),
		max(1, cfg.MaxOpenSpoolWriters),
		paths.MetaRoot,
	)
	fmt.Printf(
		"[workspace] raw=%s merged=%s build=%s final=%s\n",
		paths.RawRoot,
		paths.MergedRoot,
		paths.BuildRoot,
		paths.FinalRoot,
	)
	fmt.Printf(
		"[scan] already-complete files=%d/%d matched_size=%s index=%s\n",
		index.MatchedTSMFiles,
		index.TotalTSMFiles,
		humanBytes(index.MatchedTSMBytes),
		scanIndexPath(paths.MetaRoot),
	)
	if cfg.StopAfter == "scan" {
		fmt.Println("[stop] after scan")
		return nil
	}

	if phaseNeedsSourceRoots(cfg.StopAfter) {
		fmt.Printf(
			"[target] inst_ids=%s matched_files=%d wal_sources=%d data_layout=%s wal_layout=%s\n",
			strings.Join(targetInstIDs, ","),
			len(tsmSources),
			len(walSources),
			dataTree.Layout,
			walTree.Layout,
		)
	} else {
		fmt.Printf(
			"[target] inst_ids=%s matched_files=%d wal_sources=%d offline_phase=%s\n",
			strings.Join(targetInstIDs, ","),
			len(tsmSources),
			len(walSources),
			phaseName(cfg.StopAfter),
		)
	}

	if phaseNeedsSourceRoots(cfg.StopAfter) {
		if err := runExportPhase(paths, cfg, state, targetInstIDs, sources); err != nil {
			return err
		}
		if cfg.StopAfter == "export" {
			fmt.Println("[stop] after export")
			return nil
		}
	}

	if cfg.StopAfter == "build" && !phaseNeedsSourceRoots(cfg.StopAfter) {
		if err := ensureMergeComplete(paths, state, targetInstIDs); err != nil {
			return err
		}
	} else {
		if err := runMergePhase(paths, cfg, state, targetInstIDs); err != nil {
			return err
		}
		if cfg.StopAfter == "merge" {
			fmt.Println("[stop] after merge")
			return nil
		}
	}

	if err := runBuildPhase(paths, cfg, state, targetInstIDs); err != nil {
		return err
	}

	fmt.Printf("[done] measurement=%s inst_ids=%d meta=%s final=%s\n", measurement, len(targetInstIDs), paths.MetaRoot, paths.FinalRoot)
	return nil
}

func parseConfig() (config, error) {
	var cfg config
	var instIDs stringList
	var err error

	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.StringVar(&cfg.Database, "database", defaultDatabase, "InfluxDB database name")
	fs.StringVar(&cfg.Retention, "retention", defaultRetention, "InfluxDB retention policy")
	fs.StringVar(&cfg.DataDir, "data-dir", "/opt/my_influx/data", "InfluxDB data root or database root; only used for scan/export")
	fs.StringVar(&cfg.WALDir, "wal-dir", "/opt/my_influx/wal", "InfluxDB wal root or database root; only used for scan/export")
	fs.StringVar(&cfg.OutputDir, "output-dir", "direct_exports", "Metadata root and default final output root")
	fs.StringVar(&cfg.RawDir, "raw-dir", "", "Optional base directory for export intermediates; defaults under --output-dir")
	fs.StringVar(&cfg.MergedDir, "merged-dir", "", "Optional base directory for merged intermediates; defaults under --output-dir")
	fs.StringVar(&cfg.BuildDir, "build-dir", "", "Optional base directory for build staging outputs; defaults under --output-dir")
	fs.StringVar(&cfg.FinalDir, "final-dir", "", "Optional base directory for final parquet datasets; defaults under --output-dir")
	fs.StringVar(&cfg.Start, "start", defaultStart, "Inclusive RFC3339 UTC start timestamp")
	fs.StringVar(&cfg.End, "end", defaultEnd, "Exclusive RFC3339 UTC end timestamp")
	fs.Var(&instIDs, "instid", "Optional instId filter; repeat to limit export")
	fs.IntVar(&cfg.RowsPerFile, "rows-per-file", 5000, "Target rows per parquet part")
	fs.StringVar(&cfg.Compression, "compression", "zstd", "Parquet compression codec")
	fs.IntVar(&cfg.CompressionLevel, "compression-level", 5, "Parquet compression level")
	fs.IntVar(&cfg.IntermediateCompressionLevel, "intermediate-compression-level", 3, "Intermediate zstd compression level")
	fs.IntVar(&cfg.Workers, "workers", 1, "Parallel workers for export and merge")
	fs.IntVar(&cfg.BuildWorkers, "build-workers", 2, "Parallel workers for build")
	fs.IntVar(&cfg.MergeFanIn, "merge-fan-in", 32, "Maximum source readers opened by one merge task")
	fs.IntVar(&cfg.MaxOpenSpoolWriters, "max-open-spool-writers", 4, "Maximum concurrently-open zstd spool writers per source export")
	progressSeconds := fs.Int("progress-interval", 5, "Progress print interval in seconds")
	fs.BoolVar(&cfg.Overwrite, "overwrite", false, "Reset Go export/merge/build state but keep existing scan result")
	fs.StringVar(&cfg.StopAfter, "stop-after", "", "Optional stop phase: scan, export, merge, build")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return cfg, err
	}

	cfg.InstIDs = dedupSortedStrings(instIDs)
	cfg.Workers = max(1, cfg.Workers)
	cfg.BuildWorkers = max(1, cfg.BuildWorkers)
	cfg.MergeFanIn = max(2, cfg.MergeFanIn)
	cfg.MaxOpenSpoolWriters = max(1, cfg.MaxOpenSpoolWriters)
	cfg.ProgressInterval = time.Duration(max(1, *progressSeconds)) * time.Second
	cfg.StartNS, err = parseRFC3339Nano(cfg.Start)
	if err != nil {
		return cfg, fmt.Errorf("invalid --start: %w", err)
	}
	cfg.EndNS, err = parseRFC3339Nano(cfg.End)
	if err != nil {
		return cfg, fmt.Errorf("invalid --end: %w", err)
	}
	if cfg.EndNS <= cfg.StartNS {
		return cfg, fmt.Errorf("--end must be later than --start")
	}
	switch cfg.StopAfter {
	case "", "scan", "export", "merge", "build":
	default:
		return cfg, fmt.Errorf("invalid --stop-after=%q", cfg.StopAfter)
	}
	return cfg, nil
}

func parseRFC3339Nano(text string) (int64, error) {
	t, err := time.Parse(time.RFC3339Nano, text)
	if err != nil {
		return 0, err
	}
	return t.UTC().UnixNano(), nil
}

func phaseNeedsSourceRoots(stopAfter string) bool {
	switch stopAfter {
	case "", "scan", "export":
		return true
	case "merge", "build":
		return false
	default:
		return true
	}
}

func phaseName(stopAfter string) string {
	if stopAfter == "" {
		return "full"
	}
	return stopAfter
}

func resolveWorkLayout(cfg config) (workLayout, error) {
	metaBase, err := filepath.Abs(cfg.OutputDir)
	if err != nil {
		return workLayout{}, err
	}
	layout := workLayout{
		MetaRoot: measurementRoot(metaBase),
	}

	if layout.RawRoot, err = resolvePhaseRoot(cfg.RawDir, layout.MetaRoot, rawDirName); err != nil {
		return workLayout{}, err
	}
	if layout.MergedRoot, err = resolvePhaseRoot(cfg.MergedDir, layout.MetaRoot, mergedDirName); err != nil {
		return workLayout{}, err
	}
	if layout.BuildRoot, err = resolvePhaseRoot(cfg.BuildDir, layout.MetaRoot, buildDirName); err != nil {
		return workLayout{}, err
	}
	if layout.FinalRoot, err = resolveFinalRoot(cfg.FinalDir, layout.MetaRoot); err != nil {
		return workLayout{}, err
	}
	return layout, nil
}

func resolvePhaseRoot(baseDir, defaultMetaRoot, leaf string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return filepath.Join(defaultMetaRoot, leaf), nil
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	return filepath.Join(measurementRoot(absBase), leaf), nil
}

func resolveFinalRoot(baseDir, defaultMetaRoot string) (string, error) {
	if strings.TrimSpace(baseDir) == "" {
		return defaultMetaRoot, nil
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", err
	}
	return measurementRoot(absBase), nil
}

func resolveInfluxTree(rootDir, database, retention string) (influxTree, error) {
	inputRoot, err := filepath.Abs(rootDir)
	if err != nil {
		return influxTree{}, err
	}
	dataRootRetention := filepath.Join(inputRoot, database, retention)
	databaseRootRetention := filepath.Join(inputRoot, retention)
	if isDir(dataRootRetention) {
		return influxTree{
			InputRoot:     inputRoot,
			DatabaseRoot:  filepath.Join(inputRoot, database),
			RetentionRoot: dataRootRetention,
			Layout:        "data-root",
		}, nil
	}
	if isDir(databaseRootRetention) {
		return influxTree{
			InputRoot:     inputRoot,
			DatabaseRoot:  inputRoot,
			RetentionRoot: databaseRootRetention,
			Layout:        "database-root",
		}, nil
	}
	return influxTree{}, fmt.Errorf(
		"could not resolve influx tree from %s; expected %s or %s",
		inputRoot,
		dataRootRetention,
		databaseRootRetention,
	)
}

func loadScanIndex(root string) (scanIndex, error) {
	var idx scanIndex
	path := scanIndexPath(root)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return idx, fmt.Errorf("%w: %s", errNoScan, path)
		}
		return idx, err
	}
	if err := json.Unmarshal(data, &idx); err != nil {
		return idx, err
	}
	if idx.Measurement != measurement {
		return idx, fmt.Errorf("scan index measurement mismatch: %s", idx.Measurement)
	}
	return idx, nil
}

func tsmSourcesFromScan(index scanIndex, dataTree influxTree, targetInstIDs []string) ([]sourceRef, error) {
	targetSet := make(map[string]struct{}, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		targetSet[instID] = struct{}{}
	}

	sources := make([]sourceRef, 0, len(index.Files))
	for _, file := range index.Files {
		if len(targetSet) > 0 && !intersects(file.InstIDs, targetSet) {
			continue
		}
		absPath := filepath.Join(dataTree.RetentionRoot, filepath.FromSlash(file.RelPath))
		info, err := os.Stat(absPath)
		if err != nil {
			return nil, fmt.Errorf("stat tsm %s: %w", absPath, err)
		}
		sources = append(sources, sourceRef{
			ID:         file.FileID,
			Kind:       sourceKindTSM,
			RelPath:    file.RelPath,
			AbsPath:    absPath,
			ShardID:    file.ShardID,
			SizeBytes:  info.Size(),
			Generation: file.Generation,
			Sequence:   file.Sequence,
			InstIDs:    append([]string(nil), file.InstIDs...),
		})
	}
	sort.Slice(sources, func(i, j int) bool { return sourceLess(sources[i], sources[j]) })
	return sources, nil
}

func walSourcesForMatchedShards(walTree influxTree, tsmSources []sourceRef) ([]sourceRef, error) {
	latestGeneration := map[int]int{}
	for _, src := range tsmSources {
		if src.Generation > latestGeneration[src.ShardID] {
			latestGeneration[src.ShardID] = src.Generation
		}
	}

	shards := make([]int, 0, len(latestGeneration))
	for shardID := range latestGeneration {
		shards = append(shards, shardID)
	}
	sort.Ints(shards)

	var sources []sourceRef
	for _, shardID := range shards {
		shardDir := filepath.Join(walTree.RetentionRoot, strconv.Itoa(shardID))
		entries, err := os.ReadDir(shardDir)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		var walFiles []walFileInfo
		totalSize := int64(0)
		maxSegment := 0
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wal") {
				continue
			}
			segmentID, ok := parseWALSegmentID(entry.Name())
			if !ok {
				continue
			}
			absPath := filepath.Join(shardDir, entry.Name())
			info, err := entry.Info()
			if err != nil {
				return nil, err
			}
			if info.Size() <= 0 {
				continue
			}
			totalSize += info.Size()
			if segmentID > maxSegment {
				maxSegment = segmentID
			}
			walFiles = append(walFiles, walFileInfo{
				RelPath:   filepath.ToSlash(filepath.Join(strconv.Itoa(shardID), entry.Name())),
				AbsPath:   absPath,
				SegmentID: segmentID,
				SizeBytes: info.Size(),
			})
		}
		if len(walFiles) == 0 {
			continue
		}
		sort.Slice(walFiles, func(i, j int) bool { return walFiles[i].SegmentID < walFiles[j].SegmentID })
		relPaths := make([]string, 0, len(walFiles))
		absPaths := make([]string, 0, len(walFiles))
		for _, walFile := range walFiles {
			relPaths = append(relPaths, walFile.RelPath)
			absPaths = append(absPaths, walFile.AbsPath)
		}
		sources = append(sources, sourceRef{
			ID:         fmt.Sprintf("wal-shard-%d__%06d", shardID, maxSegment),
			Kind:       sourceKindWAL,
			RelPaths:   relPaths,
			AbsPaths:   absPaths,
			WALFiles:   walFiles,
			ShardID:    shardID,
			SizeBytes:  totalSize,
			Generation: latestGeneration[shardID],
			Sequence:   maxSegment,
		})
	}
	sort.Slice(sources, func(i, j int) bool { return sourceLess(sources[i], sources[j]) })
	return sources, nil
}

func determineTargetInstIDs(discovered, requested []string) ([]string, error) {
	discovered = dedupSortedStrings(discovered)
	if len(requested) == 0 {
		return discovered, nil
	}
	found := make(map[string]struct{}, len(discovered))
	for _, instID := range discovered {
		found[instID] = struct{}{}
	}
	for _, instID := range requested {
		if _, ok := found[instID]; !ok {
			return nil, fmt.Errorf("requested instId not found in scan result: %s", instID)
		}
	}
	return dedupSortedStrings(requested), nil
}

func ensureRoot(paths workLayout, overwrite bool) error {
	if err := os.MkdirAll(paths.MetaRoot, 0o755); err != nil {
		return err
	}
	if overwrite {
		for _, path := range []string{paths.RawRoot, paths.MergedRoot, paths.BuildRoot, statePath(paths.MetaRoot)} {
			if err := os.RemoveAll(path); err != nil {
				return err
			}
		}
		entries, err := os.ReadDir(paths.FinalRoot)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
				if err := os.RemoveAll(filepath.Join(paths.FinalRoot, entry.Name())); err != nil {
					return err
				}
			}
		}
	}
	for _, path := range []string{paths.RawRoot, paths.MergedRoot, paths.BuildRoot, paths.FinalRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func ensureState(paths workLayout, cfg config, dataTree, walTree influxTree, targetInstIDs []string, sources []sourceRef) (*pipelineState, error) {
	meta := pipelineMeta{
		PipelineVersion:         goPipelineVersion,
		PipelineName:            "okex_depth_direct_go",
		Measurement:             measurement,
		Database:                cfg.Database,
		Retention:               cfg.Retention,
		DataDir:                 cfg.DataDir,
		WALDir:                  cfg.WALDir,
		DataLayout:              dataTree.Layout,
		WALLayout:               walTree.Layout,
		DatabaseRoot:            dataTree.DatabaseRoot,
		RetentionRoot:           dataTree.RetentionRoot,
		WALRetentionRoot:        walTree.RetentionRoot,
		Start:                   cfg.Start,
		End:                     cfg.End,
		StartNS:                 cfg.StartNS,
		EndNS:                   cfg.EndNS,
		InstIDs:                 append([]string(nil), targetInstIDs...),
		IntermediateFormat:      "okex_depth_spool_v1",
		IntermediateCompression: "zstd",
		MetaRoot:                paths.MetaRoot,
		RawRoot:                 paths.RawRoot,
		MergedRoot:              paths.MergedRoot,
		BuildRoot:               paths.BuildRoot,
		FinalRoot:               paths.FinalRoot,
		ScanIndexPath:           scanIndexPath(paths.MetaRoot),
	}
	path := statePath(paths.MetaRoot)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		files := make(map[string]*fileState, len(sources))
		for _, src := range sources {
			files[src.ID] = &fileState{
				Source:       src,
				ExportStatus: "pending",
			}
		}
		state := &pipelineState{
			Meta:      meta,
			CreatedAt: utcNowISO(),
			UpdatedAt: utcNowISO(),
			Files:     files,
			Merge:     map[string]map[string]mergeFieldState{},
			Build:     map[string]buildState{},
		}
		return state, saveState(paths.MetaRoot, state)
	} else if err != nil {
		return nil, err
	}

	state, err := loadState(paths.MetaRoot)
	if err != nil {
		return nil, err
	}
	if !equalMeta(state.Meta, meta) {
		if !equalMetaIgnoringWorkspaceRoots(state.Meta, meta) {
			return nil, fmt.Errorf("existing Go pipeline state does not match current request; rerun with --overwrite to rebuild export/merge/build state")
		}
		oldMeta := state.Meta
		state.Meta.RawRoot = meta.RawRoot
		state.Meta.MergedRoot = meta.MergedRoot
		state.Meta.BuildRoot = meta.BuildRoot
		state.Meta.FinalRoot = meta.FinalRoot
		state.Meta.ScanIndexPath = meta.ScanIndexPath
		state.UpdatedAt = utcNowISO()
		fmt.Printf(
			"[workspace-relocated] raw=%s -> %s merged=%s -> %s build=%s -> %s final=%s -> %s\n",
			oldMeta.RawRoot,
			state.Meta.RawRoot,
			oldMeta.MergedRoot,
			state.Meta.MergedRoot,
			oldMeta.BuildRoot,
			state.Meta.BuildRoot,
			oldMeta.FinalRoot,
			state.Meta.FinalRoot,
		)
		if err := saveState(paths.MetaRoot, state); err != nil {
			return nil, err
		}
	}
	if len(state.Files) != len(sources) {
		return nil, fmt.Errorf("existing Go pipeline state source count changed; rerun with --overwrite")
	}
	for _, src := range sources {
		entry, ok := state.Files[src.ID]
		if !ok {
			return nil, fmt.Errorf("existing Go pipeline state missing source %s; rerun with --overwrite", src.ID)
		}
		if !sameSource(entry.Source, src) {
			return nil, fmt.Errorf("source metadata changed for %s; rerun with --overwrite", src.ID)
		}
	}
	return state, nil
}

func prepareExistingStateForOfflinePhase(paths workLayout, cfg config) (*pipelineState, error) {
	state, err := loadState(paths.MetaRoot)
	if err != nil {
		return nil, err
	}
	if state.Meta.PipelineName != "okex_depth_direct_go" {
		return nil, fmt.Errorf("existing pipeline state is not a Go okex_depth pipeline: %s", state.Meta.PipelineName)
	}
	if state.Meta.Measurement != measurement {
		return nil, fmt.Errorf("pipeline state measurement mismatch: %s", state.Meta.Measurement)
	}
	if state.Meta.Database != cfg.Database {
		return nil, fmt.Errorf("existing pipeline state database mismatch: state=%s request=%s", state.Meta.Database, cfg.Database)
	}
	if state.Meta.Retention != cfg.Retention {
		return nil, fmt.Errorf("existing pipeline state retention mismatch: state=%s request=%s", state.Meta.Retention, cfg.Retention)
	}
	if state.Meta.StartNS != cfg.StartNS || state.Meta.EndNS != cfg.EndNS {
		return nil, fmt.Errorf("existing pipeline state time window mismatch; rerun with the original --start/--end or use a matching state directory")
	}
	oldMeta := state.Meta
	state.Meta.RawRoot = paths.RawRoot
	state.Meta.MergedRoot = paths.MergedRoot
	state.Meta.BuildRoot = paths.BuildRoot
	state.Meta.FinalRoot = paths.FinalRoot
	state.Meta.ScanIndexPath = scanIndexPath(paths.MetaRoot)
	if oldMeta.RawRoot != state.Meta.RawRoot ||
		oldMeta.MergedRoot != state.Meta.MergedRoot ||
		oldMeta.BuildRoot != state.Meta.BuildRoot ||
		oldMeta.FinalRoot != state.Meta.FinalRoot ||
		oldMeta.ScanIndexPath != state.Meta.ScanIndexPath {
		fmt.Printf(
			"[workspace-relocated] raw=%s -> %s merged=%s -> %s build=%s -> %s final=%s -> %s\n",
			oldMeta.RawRoot,
			state.Meta.RawRoot,
			oldMeta.MergedRoot,
			state.Meta.MergedRoot,
			oldMeta.BuildRoot,
			state.Meta.BuildRoot,
			oldMeta.FinalRoot,
			state.Meta.FinalRoot,
		)
		if err := saveState(paths.MetaRoot, state); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func sourceCountsFromState(state *pipelineState, targetInstIDs []string) ([]sourceRef, []sourceRef) {
	targetSet := make(map[string]struct{}, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		targetSet[instID] = struct{}{}
	}
	var tsmSources []sourceRef
	var walSources []sourceRef
	for _, entry := range state.Files {
		src := entry.Source
		if src.Kind == sourceKindTSM && len(targetSet) > 0 && !intersects(src.InstIDs, targetSet) {
			continue
		}
		switch src.Kind {
		case sourceKindTSM:
			tsmSources = append(tsmSources, src)
		case sourceKindWAL:
			walSources = append(walSources, src)
		}
	}
	sort.Slice(tsmSources, func(i, j int) bool { return sourceLess(tsmSources[i], tsmSources[j]) })
	sort.Slice(walSources, func(i, j int) bool { return sourceLess(walSources[i], walSources[j]) })
	return tsmSources, walSources
}

func runExportPhase(paths workLayout, cfg config, state *pipelineState, targetInstIDs []string, sources []sourceRef) error {
	targetSet := make(map[string]struct{}, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		targetSet[instID] = struct{}{}
	}

	selected := make([]sourceRef, 0, len(sources))
	pending := make([]sourceRef, 0, len(sources))
	for _, src := range sources {
		if src.Kind == sourceKindTSM && len(targetSet) > 0 && !intersects(src.InstIDs, targetSet) {
			continue
		}
		selected = append(selected, src)
		entry := state.Files[src.ID]
		if entry.ExportStatus == "complete" {
			if ok, err := exportArtifactComplete(paths, src.ID); err == nil && ok {
				continue
			}
		}
		pending = append(pending, src)
	}
	if len(pending) == 0 {
		fmt.Printf("[export] already-complete files=%d target_inst_ids=%s\n", len(selected), strings.Join(targetInstIDs, ","))
		return nil
	}

	var (
		mu        sync.Mutex
		completed = len(selected) - len(pending)
	)

	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(cfg.Workers)
	for _, src := range pending {
		src := src
		mu.Lock()
		state.Files[src.ID].ExportStatus = "running"
		if err := saveState(paths.MetaRoot, state); err != nil {
			mu.Unlock()
			return err
		}
		mu.Unlock()

		g.Go(func() error {
			result, err := exportSource(paths, cfg, src, targetSet)
			if err != nil {
				return fmt.Errorf("export %s: %w", src.ID, err)
			}
			mu.Lock()
			entry := state.Files[src.ID]
			entry.ExportStatus = "complete"
			entry.ExportRecords = result.Records
			entry.ExportBytes = result.BytesRead
			entry.MinTimeNS = result.MinTimeNS
			entry.MaxTimeNS = result.MaxTimeNS
			if err := saveState(paths.MetaRoot, state); err != nil {
				mu.Unlock()
				return err
			}
			completed++
			fmt.Printf(
				"[export-done] done=%d/%d src=%s kind=%s records=%d stream=%s elapsed=%s\n",
				completed,
				len(selected),
				displaySourcePath(src),
				src.Kind,
				result.Records,
				humanProgress(result.BytesRead, src.SizeBytes),
				humanDuration(time.Duration(result.ElapsedSeconds*float64(time.Second))),
			)
			mu.Unlock()
			releaseHeap()
			return nil
		})
	}
	return g.Wait()
}

func exportSource(paths workLayout, cfg config, src sourceRef, targetInstIDs map[string]struct{}) (exportManifest, error) {
	started := time.Now()
	rawDir := filepath.Join(paths.RawRoot, src.ID)
	if err := os.RemoveAll(rawDir); err != nil {
		return exportManifest{}, err
	}
	if err := os.MkdirAll(rawDir, 0o755); err != nil {
		return exportManifest{}, err
	}

	progress := exportProgress{
		Source:      displaySourcePath(src),
		TotalBytes:  src.SizeBytes,
		StartedAt:   started,
		LastPrinted: started,
		Every:       cfg.ProgressInterval,
	}

	type exportWriterState struct {
		path     string
		kind     byte
		writer   *spoolWriter
		lastUsed uint64
	}

	writerStates := map[string]*exportWriterState{}
	openWriterCount := 0
	var writerUseSeq uint64
	fieldCounts := map[string]map[string]int64{}
	var minTimeNS *int64
	var maxTimeNS *int64

	closeWriter := func(state *exportWriterState) error {
		if state == nil || state.writer == nil {
			return nil
		}
		err := state.writer.Close()
		state.writer = nil
		openWriterCount--
		return err
	}

	evictWriter := func() error {
		var victim *exportWriterState
		for _, state := range writerStates {
			if state.writer == nil {
				continue
			}
			if victim == nil || state.lastUsed < victim.lastUsed {
				victim = state
			}
		}
		if victim == nil {
			return nil
		}
		return closeWriter(victim)
	}

	ensureWriter := func(writerKey, path string, kind byte) (*spoolWriter, error) {
		state := writerStates[writerKey]
		if state == nil {
			state = &exportWriterState{path: path, kind: kind}
			writerStates[writerKey] = state
		}
		writerUseSeq++
		state.lastUsed = writerUseSeq
		if state.writer != nil {
			return state.writer, nil
		}
		if openWriterCount >= cfg.MaxOpenSpoolWriters {
			if err := evictWriter(); err != nil {
				return nil, err
			}
		}
		writer, err := openSpoolWriter(path, kind, cfg.IntermediateCompressionLevel, true)
		if err != nil {
			return nil, err
		}
		state.writer = writer
		openWriterCount++
		return writer, nil
	}

	writeValue := func(instID, field string, ts int64, payload []byte) error {
		writerKey := instID + "\x00" + field
		path := rawFieldPath(rawDir, instID, field)
		writer, err := ensureWriter(writerKey, path, fieldKind(field))
		if err != nil {
			return err
		}
		if err := writer.WriteRecord(ts, payload); err != nil {
			return err
		}
		perInst := fieldCounts[instID]
		if perInst == nil {
			perInst = map[string]int64{}
			fieldCounts[instID] = perInst
		}
		perInst[field]++
		progress.Records++
		progress.LastInstID = instID
		progress.LastTimeNS = ts
		progress.HasLastTime = true
		if minTimeNS == nil || ts < *minTimeNS {
			minTimeNS = ptrInt64(ts)
		}
		if maxTimeNS == nil || ts > *maxTimeNS {
			maxTimeNS = ptrInt64(ts)
		}
		progress.maybePrint()
		return nil
	}

	defer func() {
		for _, state := range writerStates {
			_ = closeWriter(state)
		}
	}()

	switch src.Kind {
	case sourceKindTSM:
		if err := exportTSMSource(src, cfg.StartNS, cfg.EndNS, targetInstIDs, &progress, writeValue); err != nil {
			return exportManifest{}, err
		}
	case sourceKindWAL:
		if err := exportWALSource(src, cfg.StartNS, cfg.EndNS, targetInstIDs, &progress, writeValue); err != nil {
			return exportManifest{}, err
		}
	default:
		return exportManifest{}, fmt.Errorf("unknown source kind %q", src.Kind)
	}

	for _, state := range writerStates {
		if err := closeWriter(state); err != nil {
			return exportManifest{}, err
		}
	}
	progress.maybePrintForce()

	manifest := exportManifest{
		SourceID:          src.ID,
		Kind:              src.Kind,
		RawDir:            rawDir,
		Records:           progress.Records,
		BytesRead:         progress.BytesRead,
		FieldRecordCounts: fieldCounts,
		MinTimeNS:         minTimeNS,
		MaxTimeNS:         maxTimeNS,
		ElapsedSeconds:    time.Since(started).Seconds(),
	}
	if err := saveJSON(rawManifestPath(paths, src.ID), manifest); err != nil {
		return exportManifest{}, err
	}
	return manifest, nil
}

func exportTSMSource(
	src sourceRef,
	startNS int64,
	endNS int64,
	targetInstIDs map[string]struct{},
	progress *exportProgress,
	writeValue func(instID, field string, ts int64, payload []byte) error,
) error {
	file, err := os.Open(src.AbsPath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader, err := tsm1.NewTSMReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()

	iter := reader.BlockIterator()
	var (
		lastKey     []byte
		tombstones  []tsm1.TimeRange
		valueBuffer []tsm1.Value
	)
	for iter.Next() {
		key, minTime, maxTime, _, _, buf, err := iter.Read()
		if err != nil {
			return err
		}
		progress.BytesRead += int64(len(buf))
		if maxTime < startNS || minTime >= endNS {
			progress.maybePrint()
			continue
		}

		seriesKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey(key)
		field := string(fieldKey)
		if !isTargetField(field) {
			progress.maybePrint()
			continue
		}
		name, tags := models.ParseKeyBytes(seriesKey)
		if string(name) != measurement {
			progress.maybePrint()
			continue
		}
		instID, err := instIDFromTags(tags)
		if err != nil {
			progress.maybePrint()
			continue
		}
		if len(targetInstIDs) > 0 {
			if _, ok := targetInstIDs[instID]; !ok {
				progress.maybePrint()
				continue
			}
		}

		if !bytes.Equal(lastKey, key) {
			lastKey = append(lastKey[:0], key...)
			tombstones = reader.TombstoneRange(key)
		}
		if blockFullyDeleted(minTime, maxTime, tombstones) {
			progress.maybePrint()
			continue
		}

		valueBuffer, err = tsm1.DecodeBlock(buf, valueBuffer[:0])
		if err != nil {
			return err
		}
		values := valueBuffer
		for _, tombstone := range tombstones {
			values = tsm1.Values(values).Exclude(tombstone.Min, tombstone.Max)
		}

		for _, value := range values {
			ts := value.UnixNano()
			if ts < startNS || ts >= endNS {
				continue
			}
			payload, err := encodeValue(field, value)
			if err != nil {
				return err
			}
			if err := writeValue(instID, field, ts, payload); err != nil {
				return err
			}
		}
	}
	return iter.Err()
}

func exportWALSource(
	src sourceRef,
	startNS int64,
	endNS int64,
	targetInstIDs map[string]struct{},
	progress *exportProgress,
	writeValue func(instID, field string, ts int64, payload []byte) error,
) error {
	paths := append([]string(nil), src.AbsPaths...)
	if len(paths) == 0 {
		for _, walFile := range src.WALFiles {
			paths = append(paths, walFile.AbsPath)
		}
	}

	valuesByKey := make(map[string][]walValueRecord)
	var writeSeq int64
	for _, path := range paths {
		if err := replayWALFile(path, valuesByKey, &writeSeq); err != nil {
			return err
		}
	}

	keys := make([]string, 0, len(valuesByKey))
	for key := range valuesByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		seriesKey, fieldKey := tsm1.SeriesAndFieldFromCompositeKey([]byte(key))
		field := string(fieldKey)
		if !isTargetField(field) {
			continue
		}
		name, tags := models.ParseKeyBytes(seriesKey)
		if string(name) != measurement {
			continue
		}
		instID, err := instIDFromTags(tags)
		if err != nil {
			continue
		}
		if len(targetInstIDs) > 0 {
			if _, ok := targetInstIDs[instID]; !ok {
				continue
			}
		}

		values := dedupeWALValues(valuesByKey[key])
		for _, value := range values {
			ts := value.UnixNano()
			if ts < startNS || ts >= endNS {
				continue
			}
			payload, err := encodeValue(field, value)
			if err != nil {
				return err
			}
			if err := writeValue(instID, field, ts, payload); err != nil {
				return err
			}
		}
	}
	return nil
}

type walValueRecord struct {
	Seq   int64
	Value tsm1.Value
}

func replayWALFile(path string, valuesByKey map[string][]walValueRecord, writeSeq *int64) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := tsm1.NewWALSegmentReader(file)
	defer reader.Close()

	for reader.Next() {
		entry, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				fmt.Printf("[warn] truncated wal ignored path=%s consumed=%s error=%v\n", path, humanBytes(reader.Count()), err)
				return nil
			}
			return fmt.Errorf("read wal %s: %w", path, err)
		}
		switch item := entry.(type) {
		case *tsm1.WriteWALEntry:
			replayWALWrites(valuesByKey, item.Values, writeSeq)
		case *tsm1.DeleteWALEntry:
			for _, key := range item.Keys {
				delete(valuesByKey, string(key))
			}
		case *tsm1.DeleteRangeWALEntry:
			for _, key := range item.Keys {
				existing := valuesByKey[string(key)]
				if len(existing) == 0 {
					continue
				}
				valuesByKey[string(key)] = excludeWALRange(existing, item.Min, item.Max)
				if len(valuesByKey[string(key)]) == 0 {
					delete(valuesByKey, string(key))
				}
			}
		default:
			return fmt.Errorf("unsupported WAL entry type %T", entry)
		}
	}
	return nil
}

func replayWALWrites(valuesByKey map[string][]walValueRecord, writes map[string][]tsm1.Value, writeSeq *int64) {
	for key, values := range writes {
		if len(values) == 0 {
			continue
		}
		dst := valuesByKey[key]
		for _, value := range values {
			(*writeSeq)++
			dst = append(dst, walValueRecord{Seq: *writeSeq, Value: value})
		}
		valuesByKey[key] = dst
	}
}

func excludeWALRange(values []walValueRecord, min, max int64) []walValueRecord {
	dst := values[:0]
	for _, item := range values {
		ts := item.Value.UnixNano()
		if ts >= min && ts <= max {
			continue
		}
		dst = append(dst, item)
	}
	return dst
}

func dedupeWALValues(records []walValueRecord) []tsm1.Value {
	if len(records) == 0 {
		return nil
	}
	sort.SliceStable(records, func(i, j int) bool {
		ti := records[i].Value.UnixNano()
		tj := records[j].Value.UnixNano()
		if ti != tj {
			return ti < tj
		}
		return records[i].Seq < records[j].Seq
	})

	result := make([]tsm1.Value, 0, len(records))
	for i := 0; i < len(records); {
		j := i + 1
		best := records[i]
		ts := records[i].Value.UnixNano()
		for j < len(records) && records[j].Value.UnixNano() == ts {
			best = records[j]
			j++
		}
		result = append(result, best.Value)
		i = j
	}
	return result
}

func runMergePhase(paths workLayout, cfg config, state *pipelineState, targetInstIDs []string) error {
	sources, err := gatherMergeSources(paths, state, targetInstIDs)
	if err != nil {
		return err
	}

	total := len(targetInstIDs) * len(fieldNames)
	tasks := make([]struct {
		InstID string
		Field  string
		Src    []sourceFieldFragment
	}, 0, total)
	completed := 0
	for _, instID := range targetInstIDs {
		if state.Merge[instID] == nil {
			state.Merge[instID] = map[string]mergeFieldState{}
		}
		for _, field := range fieldNames {
			sourceList := sources[instID][field]
			sourceIDs := make([]string, 0, len(sourceList))
			for _, item := range sourceList {
				sourceIDs = append(sourceIDs, item.FileID)
			}
			existing := state.Merge[instID][field]
			destPath := mergedFieldPath(paths, instID, field)
			if existing.Status == "complete" && sameStrings(existing.SourceFiles, sourceIDs) {
				if len(sourceIDs) == 0 || fileExists(destPath) {
					completed++
					continue
				}
			}
			if existing.Status == "running" {
				existing.Status = "pending"
				state.Merge[instID][field] = existing
			}
			tasks = append(tasks, struct {
				InstID string
				Field  string
				Src    []sourceFieldFragment
			}{InstID: instID, Field: field, Src: sourceList})
		}
	}
	if err := saveState(paths.MetaRoot, state); err != nil {
		return err
	}
	if len(tasks) == 0 {
		fmt.Println("[merge] already-complete")
		return nil
	}

	var mu sync.Mutex
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(cfg.Workers)
	for _, task := range tasks {
		task := task
		g.Go(func() error {
			mu.Lock()
			fieldState := state.Merge[task.InstID][task.Field]
			fieldState.Status = "running"
			state.Merge[task.InstID][task.Field] = fieldState
			if err := saveState(paths.MetaRoot, state); err != nil {
				mu.Unlock()
				return err
			}
			mu.Unlock()

			result, err := mergeSingleField(paths, cfg, task.InstID, task.Field, task.Src)
			if err != nil {
				return fmt.Errorf("merge %s/%s: %w", task.InstID, task.Field, err)
			}
			mu.Lock()
			state.Merge[task.InstID][task.Field] = mergeFieldState{
				Status:      "complete",
				RowCount:    result.RowCount,
				SourceCount: result.SourceCount,
				SourceFiles: append([]string(nil), result.SourceFiles...),
				DestPath:    result.DestPath,
			}
			if err := saveState(paths.MetaRoot, state); err != nil {
				mu.Unlock()
				return err
			}
			completed++
			fmt.Printf(
				"[merge-field] done=%d/%d instId=%s field=%s rows=%d sources=%d elapsed=%s\n",
				completed,
				total,
				task.InstID,
				task.Field,
				result.RowCount,
				result.SourceCount,
				humanDuration(result.Elapsed),
			)
			mu.Unlock()
			releaseHeap()
			return nil
		})
	}
	return g.Wait()
}

func ensureMergeComplete(paths workLayout, state *pipelineState, targetInstIDs []string) error {
	for _, instID := range targetInstIDs {
		fields := state.Merge[instID]
		if fields == nil {
			return fmt.Errorf("merge state missing for %s", instID)
		}
		for _, field := range fieldNames {
			entry, ok := fields[field]
			if !ok {
				return fmt.Errorf("merge state missing for %s/%s", instID, field)
			}
			if entry.Status != "complete" {
				return fmt.Errorf("merge incomplete for %s/%s: status=%s", instID, field, entry.Status)
			}
			destPath := entry.DestPath
			if destPath == "" {
				destPath = mergedFieldPath(paths, instID, field)
			}
			if entry.SourceCount > 0 && !fileExists(destPath) {
				return fmt.Errorf("merged file missing for %s/%s: %s", instID, field, destPath)
			}
		}
	}
	return nil
}

func gatherMergeSources(paths workLayout, state *pipelineState, targetInstIDs []string) (map[string]map[string][]sourceFieldFragment, error) {
	targetSet := make(map[string]struct{}, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		targetSet[instID] = struct{}{}
	}
	result := make(map[string]map[string][]sourceFieldFragment, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		fields := make(map[string][]sourceFieldFragment, len(fieldNames))
		for _, field := range fieldNames {
			fields[field] = nil
		}
		result[instID] = fields
	}

	for sourceID, entry := range state.Files {
		if entry.ExportStatus != "complete" {
			continue
		}
		manifest, err := loadManifest(paths, sourceID)
		if err != nil {
			return nil, err
		}
		for instID, fields := range manifest.FieldRecordCounts {
			if len(targetSet) > 0 {
				if _, ok := targetSet[instID]; !ok {
					continue
				}
			}
			for field, count := range fields {
				if count <= 0 {
					continue
				}
				path := rawFieldPath(manifest.RawDir, instID, field)
				if !fileExists(path) {
					return nil, fmt.Errorf("missing raw fragment for %s: %s", sourceID, path)
				}
				result[instID][field] = append(result[instID][field], sourceFieldFragment{
					FileID:     sourceID,
					Path:       path,
					Kind:       entry.Source.Kind,
					ShardID:    entry.Source.ShardID,
					Generation: entry.Source.Generation,
					Sequence:   entry.Source.Sequence,
				})
			}
		}
	}

	for instID := range result {
		for field := range result[instID] {
			sort.Slice(result[instID][field], func(i, j int) bool {
				return mergeSourceLess(result[instID][field][i], result[instID][field][j])
			})
		}
	}
	return result, nil
}

type orderedSpoolInput struct {
	Path string
}

type mergeStageFile struct {
	Path string
	Rows int64
}

func mergeSingleField(paths workLayout, cfg config, instID, field string, sources []sourceFieldFragment) (mergeResult, error) {
	started := time.Now()
	destPath := mergedFieldPath(paths, instID, field)
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return mergeResult{}, err
	}
	if err := os.RemoveAll(destPath); err != nil {
		return mergeResult{}, err
	}
	if len(sources) == 0 {
		return mergeResult{
			InstID:      instID,
			FieldName:   field,
			DestPath:    destPath,
			RowCount:    0,
			SourceCount: 0,
			SourceFiles: nil,
			Elapsed:     time.Since(started),
		}, nil
	}

	sourceFiles := make([]string, 0, len(sources))
	for _, src := range sources {
		sourceFiles = append(sourceFiles, src.FileID)
	}

	progress := mergeProgress{
		InstID:      instID,
		FieldName:   field,
		SourceCount: len(sources),
		StartedAt:   started,
		LastPrinted: started,
		Every:       cfg.ProgressInterval,
	}

	if len(sources) <= cfg.MergeFanIn {
		inputs := make([]orderedSpoolInput, 0, len(sources))
		for _, src := range sources {
			inputs = append(inputs, orderedSpoolInput{Path: src.Path})
		}
		rowCount, err := mergeOrderedSpoolInputs(destPath, fieldKind(field), cfg.IntermediateCompressionLevel, inputs, &progress)
		if err != nil {
			return mergeResult{}, err
		}
		return mergeResult{
			InstID:      instID,
			FieldName:   field,
			DestPath:    destPath,
			RowCount:    rowCount,
			SourceCount: len(sources),
			SourceFiles: sourceFiles,
			Elapsed:     time.Since(started),
		}, nil
	}

	tempRoot := filepath.Join(paths.MergedRoot, ".merge_tmp")
	if err := os.MkdirAll(tempRoot, 0o755); err != nil {
		return mergeResult{}, err
	}
	tempDir, err := os.MkdirTemp(tempRoot, fmt.Sprintf("%s_%s_", safeTempName(instID), safeTempName(field)))
	if err != nil {
		return mergeResult{}, err
	}
	defer os.RemoveAll(tempDir)

	stageFiles := make([]mergeStageFile, 0, (len(sources)+cfg.MergeFanIn-1)/cfg.MergeFanIn)
	stage0BatchTotal := (len(sources) + cfg.MergeFanIn - 1) / cfg.MergeFanIn
	stageChunk := 0
	for i := 0; i < len(sources); i += cfg.MergeFanIn {
		j := minInt(i+cfg.MergeFanIn, len(sources))
		inputs := make([]orderedSpoolInput, 0, j-i)
		for _, src := range sources[i:j] {
			inputs = append(inputs, orderedSpoolInput{Path: src.Path})
		}
		stagePath := filepath.Join(tempDir, fmt.Sprintf("stage0-%04d%s", stageChunk, spoolExt))
		batchStarted := time.Now()
		fmt.Printf(
			"[merge-batch] instId=%s field=%s stage=0 batch=%d/%d inputs=%d src_range=%d:%d dest=%s started=%s\n",
			instID,
			field,
			stageChunk+1,
			stage0BatchTotal,
			len(inputs),
			i,
			j,
			stagePath,
			batchStarted.UTC().Format(time.RFC3339),
		)
		batchProgress := mergeProgress{
			InstID:      instID,
			FieldName:   field,
			Stage:       0,
			BatchIndex:  stageChunk + 1,
			BatchTotal:  stage0BatchTotal,
			SourceCount: len(inputs),
			StartedAt:   batchStarted,
			LastPrinted: batchStarted,
			Every:       cfg.ProgressInterval,
		}
		stageRows, err := mergeOrderedSpoolInputs(stagePath, fieldKind(field), cfg.IntermediateCompressionLevel, inputs, &batchProgress)
		if err != nil {
			return mergeResult{}, err
		}
		stageInfo, statErr := os.Stat(stagePath)
		stageSize := int64(0)
		if statErr == nil {
			stageSize = stageInfo.Size()
		}
		fmt.Printf(
			"[merge-batch] instId=%s field=%s stage=0 batch=%d/%d rows=%d rows_rate=%s out=%s rate=%s elapsed=%s\n",
			instID,
			field,
			stageChunk+1,
			stage0BatchTotal,
			stageRows,
			humanRowsRate(stageRows, time.Since(batchStarted)),
			humanBytes(stageSize),
			humanBytesRate(stageSize, time.Since(batchStarted)),
			humanDuration(time.Since(batchStarted)),
		)
		stageFiles = append(stageFiles, mergeStageFile{Path: stagePath, Rows: stageRows})
		stageChunk++
	}
	fmt.Printf(
		"[merge-stage] instId=%s field=%s stage=0 batches=%d fan_in=%d sources=%d elapsed=%s\n",
		instID,
		field,
		len(stageFiles),
		cfg.MergeFanIn,
		len(sources),
		humanDuration(time.Since(started)),
	)

	stage := 1
	for len(stageFiles) > 1 {
		nextStage := make([]mergeStageFile, 0, (len(stageFiles)+cfg.MergeFanIn-1)/cfg.MergeFanIn)
		stageChunk = 0
		for i := 0; i < len(stageFiles); i += cfg.MergeFanIn {
			j := minInt(i+cfg.MergeFanIn, len(stageFiles))
			inputs := make([]orderedSpoolInput, 0, j-i)
			batchStarted := time.Now()
			stageBatchTotal := (len(stageFiles) + cfg.MergeFanIn - 1) / cfg.MergeFanIn
			progressPtr := (*mergeProgress)(nil)
			if len(stageFiles) <= cfg.MergeFanIn {
				progress.Rows = 0
				progress.LogicalBytes = 0
				progress.HasLastTime = false
				progress.Stage = stage
				progress.BatchIndex = stageChunk + 1
				progress.BatchTotal = stageBatchTotal
				progress.SourceCount = len(inputs)
				progress.StartedAt = batchStarted
				progress.LastPrinted = batchStarted
				progressPtr = &progress
			}
			for _, stageFile := range stageFiles[i:j] {
				inputs = append(inputs, orderedSpoolInput{Path: stageFile.Path})
			}
			stagePath := filepath.Join(tempDir, fmt.Sprintf("stage%d-%04d%s", stage, stageChunk, spoolExt))
			fmt.Printf(
				"[merge-batch] instId=%s field=%s stage=%d batch=%d/%d inputs=%d dest=%s started=%s\n",
				instID,
				field,
				stage,
				stageChunk+1,
				stageBatchTotal,
				len(inputs),
				stagePath,
				batchStarted.UTC().Format(time.RFC3339),
			)
			stageRows, err := mergeOrderedSpoolInputs(stagePath, fieldKind(field), cfg.IntermediateCompressionLevel, inputs, progressPtr)
			if err != nil {
				return mergeResult{}, err
			}
			stageInfo, statErr := os.Stat(stagePath)
			stageSize := int64(0)
			if statErr == nil {
				stageSize = stageInfo.Size()
			}
			fmt.Printf(
				"[merge-batch] instId=%s field=%s stage=%d batch=%d/%d rows=%d rows_rate=%s out=%s rate=%s elapsed=%s\n",
				instID,
				field,
				stage,
				stageChunk+1,
				stageBatchTotal,
				stageRows,
				humanRowsRate(stageRows, time.Since(batchStarted)),
				humanBytes(stageSize),
				humanBytesRate(stageSize, time.Since(batchStarted)),
				humanDuration(time.Since(batchStarted)),
			)
			for _, stageFile := range stageFiles[i:j] {
				_ = os.Remove(stageFile.Path)
			}
			nextStage = append(nextStage, mergeStageFile{Path: stagePath, Rows: stageRows})
			stageChunk++
		}
		stageFiles = nextStage
		fmt.Printf(
			"[merge-stage] instId=%s field=%s stage=%d batches=%d fan_in=%d elapsed=%s\n",
			instID,
			field,
			stage,
			len(stageFiles),
			cfg.MergeFanIn,
			humanDuration(time.Since(started)),
		)
		stage++
	}

	if err := os.Rename(stageFiles[0].Path, destPath); err != nil {
		return mergeResult{}, err
	}
	return mergeResult{
		InstID:      instID,
		FieldName:   field,
		DestPath:    destPath,
		RowCount:    stageFiles[0].Rows,
		SourceCount: len(sources),
		SourceFiles: sourceFiles,
		Elapsed:     time.Since(started),
	}, nil
}

func mergeOrderedSpoolInputs(destPath string, kind byte, compressionLevel int, inputs []orderedSpoolInput, progress *mergeProgress) (int64, error) {
	writer, err := newSpoolWriter(destPath, kind, compressionLevel)
	if err != nil {
		return 0, err
	}
	defer writer.Close()

	readers := make([]*collapsedSpoolReader, 0, len(inputs))
	for _, input := range inputs {
		reader, err := newCollapsedSpoolReader(input.Path)
		if err != nil {
			return 0, err
		}
		defer reader.Close()
		readers = append(readers, reader)
	}

	type heapItem struct {
		Timestamp int64
		Rank      int
		Record    *spoolRecord
		Index     int
	}
	h := make([]heapItem, 0, len(readers))
	for idx, reader := range readers {
		if record := reader.Current(); record != nil {
			h = append(h, heapItem{Timestamp: record.TimestampNS, Rank: idx, Record: record, Index: idx})
		}
	}
	sort.Slice(h, func(i, j int) bool {
		if h[i].Timestamp != h[j].Timestamp {
			return h[i].Timestamp < h[j].Timestamp
		}
		return h[i].Rank < h[j].Rank
	})

	popMin := func() heapItem {
		item := h[0]
		h = h[1:]
		return item
	}
	pushItem := func(item heapItem) {
		idx := sort.Search(len(h), func(i int) bool {
			if h[i].Timestamp != item.Timestamp {
				return h[i].Timestamp > item.Timestamp
			}
			return h[i].Rank >= item.Rank
		})
		h = append(h, heapItem{})
		copy(h[idx+1:], h[idx:])
		h[idx] = item
	}

	var rowCount int64
	for len(h) > 0 {
		item := popMin()
		same := []heapItem{item}
		for len(h) > 0 && h[0].Timestamp == item.Timestamp {
			same = append(same, popMin())
		}
		chosen := same[0]
		for _, candidate := range same[1:] {
			if candidate.Rank > chosen.Rank {
				chosen = candidate
			}
		}
		if err := writer.WriteRecord(chosen.Record.TimestampNS, chosen.Record.Payload); err != nil {
			return 0, err
		}
		rowCount++
		if progress != nil {
			progress.Rows = rowCount
			progress.LogicalBytes += spoolRecordEncodedSize(chosen.Record.Payload)
			progress.LastTimeNS = chosen.Record.TimestampNS
			progress.HasLastTime = true
			progress.maybePrint()
		}
		for _, candidate := range same {
			nextRecord, err := readers[candidate.Index].Advance()
			if err != nil {
				return 0, err
			}
			if nextRecord != nil {
				pushItem(heapItem{
					Timestamp: nextRecord.TimestampNS,
					Rank:      candidate.Index,
					Record:    nextRecord,
					Index:     candidate.Index,
				})
			}
		}
	}

	if err := writer.Close(); err != nil {
		return 0, err
	}
	return rowCount, nil
}

func runBuildPhase(paths workLayout, cfg config, state *pipelineState, targetInstIDs []string) error {
	total := len(targetInstIDs)
	completed := 0
	tasks := make([]string, 0, len(targetInstIDs))
	for _, instID := range targetInstIDs {
		signature := mergedSignatureForInst(state, instID)
		finalDataset := finalDatasetPath(paths, instID)
		entry := state.Build[instID]
		if entry.Status == "complete" &&
			entry.RowsPerFile == cfg.RowsPerFile &&
			strings.EqualFold(entry.Compression, cfg.Compression) &&
			entry.CompressionLevel == cfg.CompressionLevel &&
			equalMergedSignature(entry.MergedSignature, signature) &&
			isDir(finalDataset) {
			completed++
			continue
		}
		tasks = append(tasks, instID)
	}
	if len(tasks) == 0 {
		fmt.Println("[build] already-complete")
		return nil
	}

	var mu sync.Mutex
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(cfg.BuildWorkers)
	for _, instID := range tasks {
		instID := instID
		mu.Lock()
		entry := state.Build[instID]
		entry.Status = "running"
		state.Build[instID] = entry
		if err := saveState(paths.MetaRoot, state); err != nil {
			mu.Unlock()
			return err
		}
		mu.Unlock()

		g.Go(func() error {
			result, err := buildDataset(paths, cfg, state, instID)
			if err != nil {
				return fmt.Errorf("build %s: %w", instID, err)
			}
			mu.Lock()
			state.Build[instID] = buildState{
				Status:           "complete",
				Rows:             result.Rows,
				PartCount:        result.PartCount,
				BytesWritten:     result.BytesWritten,
				MinTimeNS:        result.MinTimeNS,
				MaxTimeNS:        result.MaxTimeNS,
				MergedSignature:  result.MergedSignature,
				RowsPerFile:      cfg.RowsPerFile,
				Compression:      cfg.Compression,
				CompressionLevel: cfg.CompressionLevel,
			}
			if err := saveState(paths.MetaRoot, state); err != nil {
				mu.Unlock()
				return err
			}
			completed++
			fmt.Printf(
				"[build-dataset] done=%d/%d instId=%s rows=%d parts=%d size=%s elapsed=%s\n",
				completed,
				total,
				instID,
				result.Rows,
				result.PartCount,
				humanBytes(result.BytesWritten),
				humanDuration(result.Elapsed),
			)
			mu.Unlock()
			releaseHeap()
			return nil
		})
	}
	return g.Wait()
}

func buildDataset(paths workLayout, cfg config, state *pipelineState, instID string) (buildResult, error) {
	started := time.Now()
	stageDataset := stageDatasetPath(paths, instID)
	if err := os.RemoveAll(stageDataset); err != nil {
		return buildResult{}, err
	}
	if err := os.MkdirAll(stageDataset, 0o755); err != nil {
		return buildResult{}, err
	}

	existing := map[string]string{}
	for _, field := range fieldNames {
		path := mergedFieldPath(paths, instID, field)
		if fileExists(path) {
			existing[field] = path
		}
	}
	signature := mergedSignatureForInst(state, instID)
	if len(existing) == 0 {
		finalDataset := finalDatasetPath(paths, instID)
		_ = os.RemoveAll(finalDataset)
		if err := os.Rename(stageDataset, finalDataset); err != nil {
			return buildResult{}, err
		}
		return buildResult{
			InstID:          instID,
			StageDataset:    finalDataset,
			Rows:            0,
			PartCount:       0,
			BytesWritten:    0,
			MergedSignature: signature,
			Elapsed:         time.Since(started),
		}, nil
	}

	readers := map[string]*collapsedSpoolReader{}
	current := map[string]*spoolRecord{}
	for field, path := range existing {
		reader, err := newCollapsedSpoolReader(path)
		if err != nil {
			return buildResult{}, err
		}
		defer reader.Close()
		readers[field] = reader
		current[field] = reader.Current()
	}

	partIndex := 0
	var (
		buffer       []parquetRow
		rowsWritten  int64
		bytesWritten int64
		minTimeNS    *int64
		maxTimeNS    *int64
	)
	progress := buildProgress{
		InstID:      instID,
		StartedAt:   started,
		LastPrinted: started,
		Every:       cfg.ProgressInterval,
	}

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		partPath := filepath.Join(stageDataset, fmt.Sprintf("part-%05d.parquet", partIndex))
		if err := writeParquetPart(partPath, buffer, cfg.Compression, cfg.CompressionLevel); err != nil {
			return err
		}
		info, err := os.Stat(partPath)
		if err != nil {
			return err
		}
		bytesWritten += info.Size()
		partIndex++
		progress.PartCount = partIndex
		progress.BytesWritten = bytesWritten
		buffer = buffer[:0]
		return nil
	}

	for hasCurrent(current) {
		nextTimestamp := nextTimestamp(current)
		row := parquetRow{
			Time: nextTimestamp,
		}
		for field, record := range current {
			if record == nil || record.TimestampNS != nextTimestamp {
				continue
			}
			if err := assignRowField(&row, field, record.Payload); err != nil {
				return buildResult{}, err
			}
			nextRecord, err := readers[field].Advance()
			if err != nil {
				return buildResult{}, err
			}
			current[field] = nextRecord
		}
		buffer = append(buffer, row)
		rowsWritten++
		progress.Rows = rowsWritten
		progress.LastTimeNS = nextTimestamp
		progress.HasLastTime = true
		if minTimeNS == nil || nextTimestamp < *minTimeNS {
			minTimeNS = ptrInt64(nextTimestamp)
		}
		if maxTimeNS == nil || nextTimestamp > *maxTimeNS {
			maxTimeNS = ptrInt64(nextTimestamp)
		}
		if len(buffer) >= cfg.RowsPerFile {
			if err := flush(); err != nil {
				return buildResult{}, err
			}
		}
		progress.maybePrint()
	}
	if err := flush(); err != nil {
		return buildResult{}, err
	}

	finalDataset := finalDatasetPath(paths, instID)
	if err := os.RemoveAll(finalDataset); err != nil {
		return buildResult{}, err
	}
	if err := os.Rename(stageDataset, finalDataset); err != nil {
		return buildResult{}, err
	}
	if err := writeDatasetSummary(finalDataset, instID, rowsWritten, partIndex, bytesWritten, minTimeNS, maxTimeNS, cfg); err != nil {
		return buildResult{}, err
	}

	return buildResult{
		InstID:          instID,
		StageDataset:    finalDataset,
		Rows:            rowsWritten,
		PartCount:       partIndex,
		BytesWritten:    bytesWritten,
		MinTimeNS:       minTimeNS,
		MaxTimeNS:       maxTimeNS,
		MergedSignature: signature,
		Elapsed:         time.Since(started),
	}, nil
}

func writeDatasetSummary(datasetPath, instID string, rows int64, partCount int, bytesWritten int64, minTimeNS, maxTimeNS *int64, cfg config) error {
	summary := map[string]any{
		"measurement":        measurement,
		"inst_id":            instID,
		"record_count":       rows,
		"part_count":         partCount,
		"dataset_size_bytes": bytesWritten,
		"dataset_size_human": humanBytes(bytesWritten),
		"compression":        cfg.Compression,
		"compression_level":  cfg.CompressionLevel,
		"rows_per_file":      cfg.RowsPerFile,
		"min_time_ns":        minTimeNS,
		"max_time_ns":        maxTimeNS,
		"min_time":           formatNS(minTimeNS),
		"max_time":           formatNS(maxTimeNS),
	}
	return saveJSON(filepath.Join(datasetPath, "summary.json"), summary)
}

func writeParquetPart(path string, rows []parquetRow, compression string, compressionLevel int) error {
	options, err := parquetWriterOptions(compression, compressionLevel)
	if err != nil {
		return err
	}
	return parquet.WriteFile(path, rows, options...)
}

func parquetWriterOptions(codecName string, level int) ([]parquet.WriterOption, error) {
	baseOptions := []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(buildMaxRowsPerRowGroup),
		parquet.PageBufferSize(parquetPageBufferSize),
		parquet.WriteBufferSize(parquetWriteBufferSize),
		parquet.DataPageStatistics(false),
	}
	switch strings.ToLower(codecName) {
	case "", "zstd":
		codec := &parquetzstd.Codec{Level: parquetzstd.Level(zstd.EncoderLevelFromZstd(level))}
		return append(baseOptions, parquet.Compression(codec)), nil
	case "snappy":
		return append(baseOptions, parquet.Compression(&parquet.Snappy)), nil
	case "none", "uncompressed":
		return baseOptions, nil
	default:
		return nil, fmt.Errorf("unsupported parquet compression codec: %s", codecName)
	}
}

func loadState(root string) (*pipelineState, error) {
	var state pipelineState
	data, err := os.ReadFile(statePath(root))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Files == nil {
		state.Files = map[string]*fileState{}
	}
	if state.Merge == nil {
		state.Merge = map[string]map[string]mergeFieldState{}
	}
	if state.Build == nil {
		state.Build = map[string]buildState{}
	}
	if state.Meta.MetaRoot == "" {
		state.Meta.MetaRoot = root
	}
	if state.Meta.RawRoot == "" {
		state.Meta.RawRoot = filepath.Join(root, rawDirName)
	}
	if state.Meta.MergedRoot == "" {
		state.Meta.MergedRoot = filepath.Join(root, mergedDirName)
	}
	if state.Meta.BuildRoot == "" {
		state.Meta.BuildRoot = filepath.Join(root, buildDirName)
	}
	if state.Meta.FinalRoot == "" {
		state.Meta.FinalRoot = root
	}
	if state.Meta.ScanIndexPath == "" {
		state.Meta.ScanIndexPath = scanIndexPath(root)
	}
	return &state, nil
}

func saveState(root string, state *pipelineState) error {
	state.UpdatedAt = utcNowISO()
	return saveJSON(statePath(root), state)
}

func loadManifest(paths workLayout, sourceID string) (exportManifest, error) {
	var manifest exportManifest
	data, err := os.ReadFile(rawManifestPath(paths, sourceID))
	if err != nil {
		return manifest, err
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func saveJSON(path string, payload any) error {
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func exportArtifactComplete(paths workLayout, sourceID string) (bool, error) {
	manifest, err := loadManifest(paths, sourceID)
	if err != nil {
		return false, err
	}
	if !isDir(manifest.RawDir) {
		return false, nil
	}
	for instID, fields := range manifest.FieldRecordCounts {
		for field, count := range fields {
			if count <= 0 {
				continue
			}
			if !fileExists(rawFieldPath(manifest.RawDir, instID, field)) {
				return false, nil
			}
		}
	}
	return true, nil
}

func newSpoolWriter(path string, kind byte, compressionLevel int) (*spoolWriter, error) {
	return openSpoolWriter(path, kind, compressionLevel, false)
}

func openSpoolWriter(path string, kind byte, compressionLevel int, appendMode bool) (*spoolWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	writeHeader := true
	flags := os.O_WRONLY | os.O_CREATE
	if appendMode {
		flags |= os.O_APPEND
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			writeHeader = false
		}
	} else {
		flags |= os.O_TRUNC
	}
	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, err
	}
	level := zstd.EncoderLevelFromZstd(compressionLevel)
	zw, err := zstd.NewWriter(
		file,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true),
		zstd.WithWindowSize(spoolZstdWindowSize),
	)
	if err != nil {
		file.Close()
		return nil, err
	}
	bw := bufio.NewWriterSize(zw, 1<<20)
	writer := &spoolWriter{file: file, zw: zw, bw: bw, kind: kind}
	if writeHeader {
		if _, err := bw.WriteString(spoolMagic); err != nil {
			writer.Close()
			return nil, err
		}
		if err := bw.WriteByte(kind); err != nil {
			writer.Close()
			return nil, err
		}
	}
	return writer, nil
}

func (w *spoolWriter) WriteRecord(timestampNS int64, payload []byte) error {
	if err := binary.Write(w.bw, binary.LittleEndian, timestampNS); err != nil {
		return err
	}
	var lenbuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenbuf[:], uint64(len(payload)))
	if _, err := w.bw.Write(lenbuf[:n]); err != nil {
		return err
	}
	_, err := w.bw.Write(payload)
	return err
}

func (w *spoolWriter) Close() error {
	if w == nil || w.file == nil {
		return nil
	}
	var firstErr error
	if err := w.bw.Flush(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.zw.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	w.file = nil
	w.zw = nil
	w.bw = nil
	return firstErr
}

func newSpoolReader(path string) (*spoolReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	zr, err := zstd.NewReader(
		file,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderLowmem(true),
		zstd.WithDecoderMaxWindow(spoolZstdDecoderMaxWindow),
		zstd.WithDecoderMaxMemory(spoolZstdDecoderMaxMemory),
	)
	if err != nil {
		file.Close()
		return nil, err
	}
	br := bufio.NewReaderSize(zr, 1<<20)
	header := make([]byte, len(spoolMagic))
	if _, err := io.ReadFull(br, header); err != nil {
		zr.Close()
		file.Close()
		return nil, err
	}
	if string(header) != spoolMagic {
		zr.Close()
		file.Close()
		return nil, fmt.Errorf("%w: %s", errBadSpool, path)
	}
	kind, err := br.ReadByte()
	if err != nil {
		zr.Close()
		file.Close()
		return nil, err
	}
	return &spoolReader{file: file, zr: zr, br: br, kind: kind}, nil
}

func (r *spoolReader) ReadRecord() (*spoolRecord, error) {
	var timestampNS int64
	if err := binary.Read(r.br, binary.LittleEndian, &timestampNS); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, err
	}
	size, err := binary.ReadUvarint(r.br)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(r.br, payload); err != nil {
		return nil, err
	}
	return &spoolRecord{TimestampNS: timestampNS, Payload: payload}, nil
}

func (r *spoolReader) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	var firstErr error
	if r.zr != nil {
		r.zr.Close()
	}
	if err := r.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	r.file = nil
	return firstErr
}

func newCollapsedSpoolReader(path string) (*collapsedSpoolReader, error) {
	reader, err := newSpoolReader(path)
	if err != nil {
		return nil, err
	}
	cr := &collapsedSpoolReader{reader: reader}
	first, err := cr.readCollapsed()
	if err != nil {
		reader.Close()
		return nil, err
	}
	cr.current = first
	return cr, nil
}

func (r *collapsedSpoolReader) readCollapsed() (*spoolRecord, error) {
	if r.pending == nil {
		record, err := r.reader.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, nil
			}
			return nil, err
		}
		r.pending = record
	}
	current := r.pending
	for {
		next, err := r.reader.ReadRecord()
		if err != nil {
			if errors.Is(err, io.EOF) {
				r.pending = nil
				return current, nil
			}
			return nil, err
		}
		if next.TimestampNS != current.TimestampNS {
			r.pending = next
			return current, nil
		}
		current = next
	}
}

func (r *collapsedSpoolReader) Current() *spoolRecord {
	return r.current
}

func (r *collapsedSpoolReader) Advance() (*spoolRecord, error) {
	next, err := r.readCollapsed()
	if err != nil {
		return nil, err
	}
	r.current = next
	return next, nil
}

func (r *collapsedSpoolReader) Close() error {
	if r == nil {
		return nil
	}
	return r.reader.Close()
}

func releaseHeap() {
	runtime.GC()
	debug.FreeOSMemory()
}

func encodeValue(field string, value tsm1.Value) ([]byte, error) {
	if field == "checksum" {
		number, ok := value.Value().(int64)
		if !ok {
			return nil, fmt.Errorf("field %s expected int64, got %T", field, value.Value())
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], uint64(number))
		return buf[:], nil
	}
	text, ok := value.Value().(string)
	if !ok {
		return nil, fmt.Errorf("field %s expected string, got %T", field, value.Value())
	}
	return []byte(text), nil
}

func assignRowField(row *parquetRow, field string, payload []byte) error {
	switch field {
	case "action":
		value := string(payload)
		row.Action = &value
	case "asks":
		value := string(payload)
		row.Asks = &value
	case "bids":
		value := string(payload)
		row.Bids = &value
	case "ts":
		value := string(payload)
		row.TS = &value
	case "checksum":
		if len(payload) != 8 {
			return fmt.Errorf("invalid checksum payload length: %d", len(payload))
		}
		value := int64(binary.LittleEndian.Uint64(payload))
		row.Checksum = &value
	default:
		return fmt.Errorf("unknown field %q", field)
	}
	return nil
}

func mergedSignatureForInst(state *pipelineState, instID string) map[string]mergedFieldSignature {
	signature := map[string]mergedFieldSignature{}
	for field, entry := range state.Merge[instID] {
		if entry.Status != "complete" {
			continue
		}
		signature[field] = mergedFieldSignature{
			RowCount:    entry.RowCount,
			SourceFiles: append([]string(nil), entry.SourceFiles...),
		}
	}
	return signature
}

func sameSource(a, b sourceRef) bool {
	if a.ID != b.ID || a.Kind != b.Kind || a.RelPath != b.RelPath || a.AbsPath != b.AbsPath ||
		a.ShardID != b.ShardID || a.SizeBytes != b.SizeBytes || a.Generation != b.Generation || a.Sequence != b.Sequence {
		return false
	}
	if !sameStrings(a.RelPaths, b.RelPaths) || !sameStrings(a.AbsPaths, b.AbsPaths) || !sameStrings(a.InstIDs, b.InstIDs) {
		return false
	}
	if len(a.WALFiles) != len(b.WALFiles) {
		return false
	}
	for i := range a.WALFiles {
		if a.WALFiles[i] != b.WALFiles[i] {
			return false
		}
	}
	return true
}

func equalMeta(a, b pipelineMeta) bool {
	return a.PipelineVersion == b.PipelineVersion &&
		a.PipelineName == b.PipelineName &&
		a.Measurement == b.Measurement &&
		a.Database == b.Database &&
		a.Retention == b.Retention &&
		a.DataDir == b.DataDir &&
		a.WALDir == b.WALDir &&
		a.DataLayout == b.DataLayout &&
		a.WALLayout == b.WALLayout &&
		a.DatabaseRoot == b.DatabaseRoot &&
		a.RetentionRoot == b.RetentionRoot &&
		a.WALRetentionRoot == b.WALRetentionRoot &&
		a.Start == b.Start &&
		a.End == b.End &&
		a.StartNS == b.StartNS &&
		a.EndNS == b.EndNS &&
		a.IntermediateFormat == b.IntermediateFormat &&
		a.IntermediateCompression == b.IntermediateCompression &&
		a.MetaRoot == b.MetaRoot &&
		a.RawRoot == b.RawRoot &&
		a.MergedRoot == b.MergedRoot &&
		a.BuildRoot == b.BuildRoot &&
		a.FinalRoot == b.FinalRoot &&
		a.ScanIndexPath == b.ScanIndexPath &&
		sameStrings(a.InstIDs, b.InstIDs)
}

func equalMetaIgnoringWorkspaceRoots(a, b pipelineMeta) bool {
	return a.PipelineVersion == b.PipelineVersion &&
		a.PipelineName == b.PipelineName &&
		a.Measurement == b.Measurement &&
		a.Database == b.Database &&
		a.Retention == b.Retention &&
		a.DataDir == b.DataDir &&
		a.WALDir == b.WALDir &&
		a.DataLayout == b.DataLayout &&
		a.WALLayout == b.WALLayout &&
		a.DatabaseRoot == b.DatabaseRoot &&
		a.RetentionRoot == b.RetentionRoot &&
		a.WALRetentionRoot == b.WALRetentionRoot &&
		a.Start == b.Start &&
		a.End == b.End &&
		a.StartNS == b.StartNS &&
		a.EndNS == b.EndNS &&
		a.IntermediateFormat == b.IntermediateFormat &&
		a.IntermediateCompression == b.IntermediateCompression &&
		a.MetaRoot == b.MetaRoot &&
		sameStrings(a.InstIDs, b.InstIDs)
}

func equalMergedSignature(a, b map[string]mergedFieldSignature) bool {
	if len(a) != len(b) {
		return false
	}
	for key, left := range a {
		right, ok := b[key]
		if !ok || left.RowCount != right.RowCount || !sameStrings(left.SourceFiles, right.SourceFiles) {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sourceLess(a, b sourceRef) bool {
	if a.ShardID != b.ShardID {
		return a.ShardID < b.ShardID
	}
	if a.Kind != b.Kind {
		return a.Kind == sourceKindTSM
	}
	if a.Kind == sourceKindTSM {
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
	} else if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	return a.ID < b.ID
}

func mergeSourceLess(a, b sourceFieldFragment) bool {
	if a.ShardID != b.ShardID {
		return a.ShardID < b.ShardID
	}
	if a.Kind != b.Kind {
		return a.Kind == sourceKindTSM
	}
	if a.Kind == sourceKindTSM {
		if a.Generation != b.Generation {
			return a.Generation < b.Generation
		}
		if a.Sequence != b.Sequence {
			return a.Sequence < b.Sequence
		}
	} else if a.Sequence != b.Sequence {
		return a.Sequence < b.Sequence
	}
	return a.FileID < b.FileID
}

func statePath(metaRoot string) string      { return filepath.Join(metaRoot, stateFileName) }
func scanIndexPath(metaRoot string) string  { return filepath.Join(metaRoot, scanIndexFileName) }
func measurementRoot(baseDir string) string { return filepath.Join(baseDir, measurement) }
func rawManifestPath(paths workLayout, sourceID string) string {
	return filepath.Join(paths.RawRoot, sourceID, "_manifest.json")
}
func rawFieldPath(baseDir, instID, field string) string {
	return filepath.Join(baseDir, instID, field+spoolExt)
}
func mergedFieldPath(paths workLayout, instID, field string) string {
	return filepath.Join(paths.MergedRoot, instID, field+spoolExt)
}
func stageDatasetPath(paths workLayout, instID string) string {
	return filepath.Join(paths.BuildRoot, instID+".parquet")
}
func finalDatasetPath(paths workLayout, instID string) string {
	return filepath.Join(paths.FinalRoot, instID+".parquet")
}

func displaySourcePath(src sourceRef) string {
	if src.Kind == sourceKindTSM {
		return src.RelPath
	}
	if len(src.RelPaths) > 0 {
		return strings.Join(src.RelPaths, ",")
	}
	return src.ID
}

func fieldKind(field string) byte {
	if field == "checksum" {
		return spoolKindInt64
	}
	return spoolKindString
}

func isTargetField(field string) bool {
	_, ok := targetFields[field]
	return ok
}

func instIDFromTags(tags models.Tags) (string, error) {
	for _, tag := range tags {
		if string(tag.Key) == "instId" {
			return string(tag.Value), nil
		}
	}
	return "", errNoInstID
}

func blockFullyDeleted(minTime, maxTime int64, tombstones []tsm1.TimeRange) bool {
	for _, tombstone := range tombstones {
		if tombstone.Min <= minTime && tombstone.Max >= maxTime {
			return true
		}
	}
	return false
}

func parseWALSegmentID(name string) (int, bool) {
	if !strings.HasPrefix(name, "_") || !strings.HasSuffix(name, ".wal") {
		return 0, false
	}
	number := strings.TrimSuffix(strings.TrimPrefix(name, "_"), ".wal")
	value, err := strconv.Atoi(number)
	if err != nil {
		return 0, false
	}
	return value, true
}

func dedupSortedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := append([]string(nil), values...)
	sort.Strings(out)
	dst := out[:0]
	var last string
	for i, value := range out {
		if i == 0 || value != last {
			dst = append(dst, value)
			last = value
		}
	}
	return dst
}

func intersects(items []string, set map[string]struct{}) bool {
	for _, item := range items {
		if _, ok := set[item]; ok {
			return true
		}
	}
	return false
}

func utcNowISO() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func humanBytes(value int64) string {
	if value < 1024 {
		return fmt.Sprintf("%d B", value)
	}
	units := []string{"KB", "MB", "GB", "TB", "PB"}
	size := float64(value)
	unit := -1
	for size >= 1024 && unit < len(units)-1 {
		size /= 1024
		unit++
	}
	if size >= 100 {
		return fmt.Sprintf("%.0f %s", size, units[unit])
	}
	if size >= 10 {
		return fmt.Sprintf("%.1f %s", size, units[unit])
	}
	return fmt.Sprintf("%.2f %s", size, units[unit])
}

func humanProgress(current, total int64) string {
	if total <= 0 {
		return humanBytes(current)
	}
	return fmt.Sprintf("%s/%s", humanBytes(current), humanBytes(total))
}

func humanBytesRate(value int64, elapsed time.Duration) string {
	if elapsed <= 0 {
		return "0 B/s"
	}
	perSecond := int64(float64(value) / elapsed.Seconds())
	return humanBytes(perSecond) + "/s"
}

func humanRowsRate(rows int64, elapsed time.Duration) string {
	if elapsed <= 0 {
		return "0/s"
	}
	rate := float64(rows) / elapsed.Seconds()
	switch {
	case rate >= 1_000_000:
		return fmt.Sprintf("%.2fM/s", rate/1_000_000)
	case rate >= 1_000:
		return fmt.Sprintf("%.1fk/s", rate/1_000)
	default:
		return fmt.Sprintf("%.0f/s", rate)
	}
}

func spoolRecordEncodedSize(payload []byte) int64 {
	return int64(8 + uvarintLen(uint64(len(payload))) + len(payload))
}

func uvarintLen(value uint64) int {
	length := 1
	for value >= 0x80 {
		value >>= 7
		length++
	}
	return length
}

func humanDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	hours := int(d / time.Hour)
	minutes := int((d % time.Hour) / time.Minute)
	seconds := int((d % time.Minute) / time.Second)
	if hours > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", hours, minutes, seconds)
	}
	return fmt.Sprintf("%dm%02ds", minutes, seconds)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var tempNameSanitizer = strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")

func safeTempName(value string) string {
	if value == "" {
		return "empty"
	}
	return tempNameSanitizer.Replace(value)
}

func formatNS(value *int64) any {
	if value == nil {
		return nil
	}
	return time.Unix(0, *value).UTC().Format(time.RFC3339Nano)
}

func ptrInt64(value int64) *int64 {
	v := value
	return &v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func hasCurrent(current map[string]*spoolRecord) bool {
	for _, record := range current {
		if record != nil {
			return true
		}
	}
	return false
}

func nextTimestamp(current map[string]*spoolRecord) int64 {
	first := true
	var ts int64
	for _, record := range current {
		if record == nil {
			continue
		}
		if first {
			ts = record.TimestampNS
			first = false
			continue
		}
		ts = minInt64(ts, record.TimestampNS)
	}
	return ts
}

func (p *exportProgress) maybePrint() {
	if time.Since(p.LastPrinted) < p.Every {
		return
	}
	p.LastPrinted = time.Now()
	lastTime := "-"
	if p.HasLastTime {
		lastTime = time.Unix(0, p.LastTimeNS).UTC().Format(time.RFC3339Nano)
	}
	fmt.Printf(
		"[export-file] src=%s stream=%s records=%d last_inst=%s last_time=%s elapsed=%s\n",
		p.Source,
		humanProgress(p.BytesRead, p.TotalBytes),
		p.Records,
		emptyDash(p.LastInstID),
		lastTime,
		humanDuration(time.Since(p.StartedAt)),
	)
}

func (p *exportProgress) maybePrintForce() {
	p.LastPrinted = time.Now()
	lastTime := "-"
	if p.HasLastTime {
		lastTime = time.Unix(0, p.LastTimeNS).UTC().Format(time.RFC3339Nano)
	}
	fmt.Printf(
		"[export-file] src=%s stream=%s records=%d last_inst=%s last_time=%s elapsed=%s\n",
		p.Source,
		humanProgress(p.BytesRead, p.TotalBytes),
		p.Records,
		emptyDash(p.LastInstID),
		lastTime,
		humanDuration(time.Since(p.StartedAt)),
	)
}

func (p *mergeProgress) maybePrint() {
	if time.Since(p.LastPrinted) < p.Every {
		return
	}
	p.LastPrinted = time.Now()
	elapsed := time.Since(p.StartedAt)
	lastTime := "-"
	if p.HasLastTime {
		lastTime = time.Unix(0, p.LastTimeNS).UTC().Format(time.RFC3339Nano)
	}
	if p.BatchTotal > 0 {
		fmt.Printf(
			"[merge-progress] instId=%s field=%s stage=%d batch=%d/%d rows=%d rows_rate=%s out=%s rate=%s sources=%d last_time=%s elapsed=%s\n",
			p.InstID,
			p.FieldName,
			p.Stage,
			p.BatchIndex,
			p.BatchTotal,
			p.Rows,
			humanRowsRate(p.Rows, elapsed),
			humanBytes(p.LogicalBytes),
			humanBytesRate(p.LogicalBytes, elapsed),
			p.SourceCount,
			lastTime,
			humanDuration(elapsed),
		)
		return
	}
	fmt.Printf(
		"[merge-progress] instId=%s field=%s rows=%d rows_rate=%s out=%s rate=%s sources=%d last_time=%s elapsed=%s\n",
		p.InstID,
		p.FieldName,
		p.Rows,
		humanRowsRate(p.Rows, elapsed),
		humanBytes(p.LogicalBytes),
		humanBytesRate(p.LogicalBytes, elapsed),
		p.SourceCount,
		lastTime,
		humanDuration(elapsed),
	)
}

func (p *buildProgress) maybePrint() {
	if time.Since(p.LastPrinted) < p.Every {
		return
	}
	p.LastPrinted = time.Now()
	elapsed := time.Since(p.StartedAt)
	lastTime := "-"
	if p.HasLastTime {
		lastTime = time.Unix(0, p.LastTimeNS).UTC().Format(time.RFC3339Nano)
	}
	fmt.Printf(
		"[build-progress] instId=%s rows=%d rows_rate=%s parts=%d size=%s rate=%s last_time=%s elapsed=%s\n",
		p.InstID,
		p.Rows,
		humanRowsRate(p.Rows, elapsed),
		p.PartCount,
		humanBytes(p.BytesWritten),
		humanBytesRate(p.BytesWritten, elapsed),
		lastTime,
		humanDuration(elapsed),
	)
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
