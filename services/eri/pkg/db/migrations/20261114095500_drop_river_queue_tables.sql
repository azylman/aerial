-- Drop river queue tables and types
DROP TABLE IF EXISTS "river_job" CASCADE;
DROP TABLE IF EXISTS "river_queue" CASCADE;
DROP TABLE IF EXISTS "river_leader" CASCADE;
DROP TABLE IF EXISTS "river_notification" CASCADE;
DROP TABLE IF EXISTS "river_client" CASCADE;
DROP TABLE IF EXISTS "river_client_queue" CASCADE;
DROP TABLE IF EXISTS "river_migration" CASCADE;
DROP FUNCTION IF EXISTS "river_job_state_in_bitmask" CASCADE;
DROP TYPE IF EXISTS "river_job_state" CASCADE;
