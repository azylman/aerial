-- Modify "sessions" table
ALTER TABLE "sessions" ADD COLUMN "lease_generation" bigint NOT NULL DEFAULT 0;
