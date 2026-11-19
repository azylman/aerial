-- Transcript role ownership migration (hand-written, idempotent).
-- Mirrors the CNPG-managed transcripts/memory_writer group roles for environments
-- replaying migrations without CNPG (dev, hermetic fixtures). Production roles are
-- provisioned by CNPG (c02); every privileged statement below degrades to NOTICE
-- when the executor lacks rights or the role is already managed externally, so this
-- migration never aborts a replay chain run by an unprivileged user.
-- Transfers ownership of the two stack-scoped transcript tables
-- (session_summaries, transcript_steps) to transcripts and grants memory_writer
-- DML on them plus schema-wide sequence USAGE/SELECT for serial-backed writes.

DO $$
BEGIN
    CREATE ROLE transcripts WITH NOLOGIN;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'role transcripts already exists, skipping';
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to create role transcripts, skipping';
END
$$;

DO $$
BEGIN
    CREATE ROLE memory_writer WITH NOLOGIN;
EXCEPTION
    WHEN duplicate_object THEN
        RAISE NOTICE 'role memory_writer already exists, skipping';
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to create role memory_writer, skipping';
END
$$;

DO $$
BEGIN
    ALTER TABLE public.session_summaries OWNER TO transcripts;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to transfer session_summaries ownership, skipping';
    WHEN undefined_object THEN
        RAISE NOTICE 'transcripts role or session_summaries table missing, skipping ownership transfer';
END
$$;

DO $$
BEGIN
    ALTER TABLE public.transcript_steps OWNER TO transcripts;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to transfer transcript_steps ownership, skipping';
    WHEN undefined_object THEN
        RAISE NOTICE 'transcripts role or transcript_steps table missing, skipping ownership transfer';
END
$$;

DO $$
BEGIN
    GRANT SELECT, INSERT, UPDATE, DELETE ON public.session_summaries TO memory_writer;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to grant DML on session_summaries, skipping';
    WHEN undefined_object THEN
        RAISE NOTICE 'memory_writer role or session_summaries table missing, skipping DML grant';
END
$$;

DO $$
BEGIN
    GRANT SELECT, INSERT, UPDATE, DELETE ON public.transcript_steps TO memory_writer;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to grant DML on transcript_steps, skipping';
    WHEN undefined_object THEN
        RAISE NOTICE 'memory_writer role or transcript_steps table missing, skipping DML grant';
END
$$;

DO $$
BEGIN
    GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO memory_writer;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'insufficient privilege to grant sequence usage, skipping';
    WHEN undefined_object THEN
        RAISE NOTICE 'memory_writer role missing, skipping sequence grant';
END
$$;
