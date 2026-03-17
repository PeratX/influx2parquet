package main

import (
	"path/filepath"
	"testing"
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
