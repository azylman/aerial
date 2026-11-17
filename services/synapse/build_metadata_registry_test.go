package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func metadataFixture(t *testing.T) BuildMetadata {
	t.Helper()
	raw, err := os.ReadFile("testdata/build-metadata-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := ParseBuildMetadata(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRecordPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, repo, kind     string
		nilPool, nilReceiver bool
		dbErr                error
		wantErr, want        bool
	}{
		{name: "updated", repo: "owner/repo", want: true},
		{name: "final", repo: "owner/repo", kind: "final", want: true},
		{name: "transport result rejected", repo: "owner/repo", kind: "result", wantErr: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "nil receiver", repo: "owner/repo", nilReceiver: true},
		{name: "unchanged", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := metadataFixture(t)
			if tc.kind != "" {
				m.Kind = tc.kind
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, true)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			if tc.nilReceiver {
				r = nil
			}
			got, err := r.RecordPRBuildMetadata(context.Background(), tc.repo, 1, m)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func TestLoadPRBuildMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, raw, repo string
		nilPool         bool
		dbErr           error
		wantErr, want   bool
	}{
		{name: "valid", repo: "owner/repo", want: true},
		{name: "wrong repo", repo: "owner/other", wantErr: true},
		{name: "invalid JSON", repo: "owner/repo", raw: "{}", wantErr: true},
		{name: "nil pool", repo: "owner/repo", nilPool: true},
		{name: "absent", repo: "owner/repo", dbErr: pgx.ErrNoRows},
		{name: "database error", repo: "owner/repo", dbErr: errors.New("offline"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile("testdata/build-metadata-v1.json")
			if err != nil {
				t.Fatal(err)
			}
			if tc.raw != "" {
				raw = []byte(tc.raw)
			}
			r := NewPostgresPRRegistry(&mockSynapsePgxPool{queryRowFn: func(context.Context, string, ...any) pgx.Row {
				return mockSynapsePgxRow{scanFn: func(dest ...any) error {
					if tc.dbErr != nil {
						return tc.dbErr
					}
					return scanValues(dest, raw)
				}}
			}})
			if tc.nilPool {
				r.pool = nil
			}
			m, got, err := r.LoadPRBuildMetadata(context.Background(), tc.repo, 1)
			if got != tc.want || (err != nil) != tc.wantErr {
				t.Fatalf("got=%v err=%v", got, err)
			}
			if got && m.RunID != 1 {
				t.Fatal("wrong run identity")
			}
		})
	}
}
