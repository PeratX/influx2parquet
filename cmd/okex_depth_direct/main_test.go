package main

import "testing"

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
