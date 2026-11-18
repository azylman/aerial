package main

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestCheckoutRevisionFromArchive(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		logs    []string
		invalid bool
		want    string
		wantErr bool
	}{
		{"checkout revision", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"repeated checkout", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false},
		{"conflicting jobs", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "time [command]/usr/bin/git log -1 --format=%H\ntime bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, false, "", true},
		{"conflicting checkouts", []string{"time [command]/usr/bin/git log -1 --format=%H\ntime aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\ntime [command]/usr/bin/git log -1 --format=%H\ntime bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, false, "", true},
		{"missing checkout", []string{"a random SHA aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, false, "", true},
		{"invalid archive", nil, true, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			writer := zip.NewWriter(&buf)
			if _, err := writer.Create("logs/"); err != nil {
				t.Fatal(err)
			}
			for i, log := range tc.logs {
				entry, err := writer.Create(string(rune('a'+i)) + ".txt")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = entry.Write([]byte(log)); err != nil {
					t.Fatal(err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			raw := buf.Bytes()
			if tc.invalid {
				raw = []byte("invalid")
			}
			got, err := checkoutRevisionFromArchive(raw)
			if (err != nil) != tc.wantErr || (!tc.wantErr && got != tc.want) {
				t.Fatalf("revision=%q error=%v", got, err)
			}
		})
	}
}
