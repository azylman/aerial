-- Migration: 20261004183000_add_production_code_coverage.sql
-- Create production_code_coverage table and indexes for tracking runtime binary execution

CREATE TABLE IF NOT EXISTS production_code_coverage (
    service TEXT NOT NULL,
    file TEXT NOT NULL,
    function_name TEXT NOT NULL,
    coverage_percent REAL NOT NULL DEFAULT 0.0,
    last_executed_at TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (service, file, function_name)
);

CREATE INDEX IF NOT EXISTS idx_production_coverage_last_executed ON production_code_coverage(last_executed_at);
CREATE INDEX IF NOT EXISTS idx_production_coverage_service ON production_code_coverage(service);
