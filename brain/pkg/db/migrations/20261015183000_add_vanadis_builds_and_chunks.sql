-- Migration: 20261015183000_add_vanadis_builds_and_chunks.sql
-- Create vanadis_builds and vanadis_chunks tables and indexes

CREATE TABLE IF NOT EXISTS vanadis_builds (
    id TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    tier INTEGER NOT NULL DEFAULT 2,
    objective TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_vanadis_builds_repo_status ON vanadis_builds (repo, status);
CREATE INDEX IF NOT EXISTS idx_vanadis_builds_thread_id ON vanadis_builds (thread_id);

CREATE TABLE IF NOT EXISTS vanadis_chunks (
    id TEXT PRIMARY KEY,
    build_id TEXT NOT NULL REFERENCES vanadis_builds(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    title TEXT NOT NULL,
    instructions TEXT NOT NULL,
    target_files TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    pr_number INTEGER,
    diff_stat TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_vanadis_chunks_build_index UNIQUE (build_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS idx_vanadis_chunks_build_status ON vanadis_chunks (build_id, status);
CREATE INDEX IF NOT EXISTS idx_vanadis_chunks_pr_number ON vanadis_chunks (pr_number) WHERE pr_number IS NOT NULL;
