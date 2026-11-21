-- Modify "pr_registry" table
ALTER TABLE "public"."pr_registry" ADD COLUMN "vanadis_chunk_id" text NOT NULL DEFAULT '', ADD COLUMN "bot_id" text NOT NULL DEFAULT '', ADD COLUMN "ci_fail_count" integer NOT NULL DEFAULT 0, ADD COLUMN "deploy_failed_published" boolean NOT NULL DEFAULT false, ADD COLUMN "deploy_success_pending" boolean NOT NULL DEFAULT false, ADD COLUMN "banner" text NOT NULL DEFAULT '', ADD COLUMN "widget_state" text NOT NULL DEFAULT '', ADD COLUMN "build_version" integer NULL, ADD COLUMN "build_kind" text NULL, ADD COLUMN "build_repository" text NULL, ADD COLUMN "build_workflow_path" text NULL, ADD COLUMN "build_run_id" bigint NULL, ADD COLUMN "build_run_number" bigint NULL, ADD COLUMN "build_run_attempt" integer NULL, ADD COLUMN "build_event" text NULL, ADD COLUMN "build_source_sha" text NULL, ADD COLUMN "build_execution_sha" text NULL, ADD COLUMN "build_pr_number" integer NULL, ADD COLUMN "build_service" text NULL, ADD COLUMN "build_image_name" text NULL, ADD COLUMN "build_state" text NULL, ADD COLUMN "build_pushed" boolean NULL, ADD COLUMN "build_source_attempt" integer NULL, ADD COLUMN "build_image_refs" text[] NULL, ADD COLUMN "build_digest" text NULL;
-- Create "pr_registry_build_results" table
CREATE TABLE "public"."pr_registry_build_results" (
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "service" text NOT NULL,
  "image_name" text NOT NULL,
  "position" integer NOT NULL,
  "state" text NOT NULL DEFAULT '',
  "pushed" boolean NOT NULL DEFAULT false,
  "source_attempt" integer NOT NULL DEFAULT 0,
  "image_refs" text[] NOT NULL DEFAULT '{}',
  "digest" text NOT NULL DEFAULT '',
  PRIMARY KEY ("repo", "pr_number", "service", "image_name"),
  CONSTRAINT "fk_pr_registry_build_results_pr" FOREIGN KEY ("repo", "pr_number") REFERENCES "public"."pr_registry" ("repo", "pr_number") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "pr_registry_build_selections" table
CREATE TABLE "public"."pr_registry_build_selections" (
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "service" text NOT NULL,
  "image_name" text NOT NULL,
  "position" integer NOT NULL,
  PRIMARY KEY ("repo", "pr_number", "service", "image_name"),
  CONSTRAINT "fk_pr_registry_build_selections_pr" FOREIGN KEY ("repo", "pr_number") REFERENCES "public"."pr_registry" ("repo", "pr_number") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "pr_registry_checks" table
CREATE TABLE "public"."pr_registry_checks" (
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "check_name" text NOT NULL,
  "head_sha" text NOT NULL,
  "state" text NOT NULL,
  PRIMARY KEY ("repo", "pr_number", "check_name"),
  CONSTRAINT "fk_pr_registry_checks_pr" FOREIGN KEY ("repo", "pr_number") REFERENCES "public"."pr_registry" ("repo", "pr_number") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "pr_registry_jobs" table
CREATE TABLE "public"."pr_registry_jobs" (
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "job_name" text NOT NULL,
  "expected" boolean NOT NULL DEFAULT false,
  "started" boolean NOT NULL DEFAULT false,
  "deployed" boolean NOT NULL DEFAULT false,
  "state" text NOT NULL DEFAULT '',
  PRIMARY KEY ("repo", "pr_number", "job_name"),
  CONSTRAINT "fk_pr_registry_jobs_pr" FOREIGN KEY ("repo", "pr_number") REFERENCES "public"."pr_registry" ("repo", "pr_number") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Create "pr_registry_kustomizations" table
CREATE TABLE "public"."pr_registry_kustomizations" (
  "repo" text NOT NULL,
  "pr_number" integer NOT NULL,
  "name" text NOT NULL,
  "namespace" text NOT NULL DEFAULT '',
  "requested_at" timestamptz NULL,
  "deployed" boolean NOT NULL DEFAULT false,
  PRIMARY KEY ("repo", "pr_number", "name"),
  CONSTRAINT "fk_pr_registry_kustomizations_pr" FOREIGN KEY ("repo", "pr_number") REFERENCES "public"."pr_registry" ("repo", "pr_number") ON UPDATE NO ACTION ON DELETE CASCADE
);
-- Backfill typed columns and child tables from metadata. Every statement is
-- idempotent (UPDATE ... WHERE the typed value differs, upserts that skip
-- unchanged rows, deletes of entries metadata no longer holds), so a later
-- chunk can re-run this block verbatim as a catch-up sync.
UPDATE "public"."pr_registry" AS r SET
  "agent" = v.agent,
  "vanadis_chunk_id" = v.vanadis_chunk_id,
  "bot_id" = v.bot_id,
  "ci_fail_count" = v.ci_fail_count,
  "deploy_failed_published" = v.deploy_failed_published,
  "deploy_success_pending" = v.deploy_success_pending,
  "banner" = v.banner,
  "widget_state" = v.widget_state,
  "build_version" = v.build_version,
  "build_kind" = v.build_kind,
  "build_repository" = v.build_repository,
  "build_workflow_path" = v.build_workflow_path,
  "build_run_id" = v.build_run_id,
  "build_run_number" = v.build_run_number,
  "build_run_attempt" = v.build_run_attempt,
  "build_event" = v.build_event,
  "build_source_sha" = v.build_source_sha,
  "build_execution_sha" = v.build_execution_sha,
  "build_pr_number" = v.build_pr_number,
  "build_service" = v.build_service,
  "build_image_name" = v.build_image_name,
  "build_state" = v.build_state,
  "build_pushed" = v.build_pushed,
  "build_source_attempt" = v.build_source_attempt,
  "build_image_refs" = v.build_image_refs,
  "build_digest" = v.build_digest
FROM (
  SELECT p.id,
    COALESCE(NULLIF(p.metadata->>'agent', ''), NULLIF(p.metadata->>'bot_id', ''), p.agent) AS agent,
    COALESCE(p.metadata->>'vanadis_chunk_id', '') AS vanadis_chunk_id,
    COALESCE(p.metadata->>'bot_id', '') AS bot_id,
    COALESCE((p.metadata->>'ci_fail_count')::integer, 0) AS ci_fail_count,
    COALESCE((p.metadata->>'deploy_failed_published')::boolean, false) AS deploy_failed_published,
    COALESCE((p.metadata->>'deploy_success_pending')::boolean, false) AS deploy_success_pending,
    COALESCE(p.metadata->>'banner', '') AS banner,
    COALESCE(p.metadata->>'widget_state', '') AS widget_state,
    (b.m->>'version')::integer AS build_version,
    b.m->>'kind' AS build_kind,
    b.m->>'repository' AS build_repository,
    b.m->>'workflow_path' AS build_workflow_path,
    (b.m->>'run_id')::bigint AS build_run_id,
    (b.m->>'run_number')::bigint AS build_run_number,
    (b.m->>'run_attempt')::integer AS build_run_attempt,
    b.m->>'event' AS build_event,
    b.m->>'source_sha' AS build_source_sha,
    b.m->>'execution_sha' AS build_execution_sha,
    (b.m->>'pr_number')::integer AS build_pr_number,
    b.m->>'service' AS build_service,
    b.m->>'image_name' AS build_image_name,
    b.m->>'state' AS build_state,
    (b.m->>'pushed')::boolean AS build_pushed,
    (b.m->>'source_attempt')::integer AS build_source_attempt,
    CASE WHEN jsonb_typeof(b.m->'image_refs') = 'array'
      THEN ARRAY(SELECT jsonb_array_elements_text(b.m->'image_refs')) END AS build_image_refs,
    b.m->>'digest' AS build_digest
  FROM "public"."pr_registry" AS p
  LEFT JOIN LATERAL (
    SELECT p.metadata->'build_metadata' AS m WHERE jsonb_typeof(p.metadata->'build_metadata') = 'object'
  ) AS b ON true
) AS v
WHERE r.id = v.id
  AND (r."agent", r."vanadis_chunk_id", r."bot_id", r."ci_fail_count", r."deploy_failed_published",
       r."deploy_success_pending", r."banner", r."widget_state", r."build_version", r."build_kind",
       r."build_repository", r."build_workflow_path", r."build_run_id", r."build_run_number",
       r."build_run_attempt", r."build_event", r."build_source_sha", r."build_execution_sha",
       r."build_pr_number", r."build_service", r."build_image_name", r."build_state", r."build_pushed",
       r."build_source_attempt", r."build_image_refs", r."build_digest")
  IS DISTINCT FROM
      (v.agent, v.vanadis_chunk_id, v.bot_id, v.ci_fail_count, v.deploy_failed_published,
       v.deploy_success_pending, v.banner, v.widget_state, v.build_version, v.build_kind,
       v.build_repository, v.build_workflow_path, v.build_run_id, v.build_run_number,
       v.build_run_attempt, v.build_event, v.build_source_sha, v.build_execution_sha,
       v.build_pr_number, v.build_service, v.build_image_name, v.build_state, v.build_pushed,
       v.build_source_attempt, v.build_image_refs, v.build_digest);
-- Backfill "pr_registry_checks" from metadata.ci_checks (check name -> state)
INSERT INTO "public"."pr_registry_checks" ("repo", "pr_number", "check_name", "head_sha", "state")
SELECT p.repo, p.pr_number, c.key, p.head_sha, c.value
FROM "public"."pr_registry" AS p, jsonb_each_text(p.metadata->'ci_checks') AS c
WHERE jsonb_typeof(p.metadata->'ci_checks') = 'object'
ON CONFLICT ("repo", "pr_number", "check_name") DO UPDATE
  SET "head_sha" = EXCLUDED."head_sha", "state" = EXCLUDED."state"
  WHERE ("public"."pr_registry_checks"."head_sha", "public"."pr_registry_checks"."state")
    IS DISTINCT FROM (EXCLUDED."head_sha", EXCLUDED."state");
DELETE FROM "public"."pr_registry_checks" AS c
USING "public"."pr_registry" AS p
WHERE p.repo = c.repo AND p.pr_number = c.pr_number
  AND NOT (jsonb_typeof(p.metadata->'ci_checks') = 'object' AND p.metadata->'ci_checks' ? c.check_name);
-- Backfill "pr_registry_jobs" from metadata.jobs, started_jobs, deployed_jobs and job_states
WITH job_meta AS (
  SELECT p.repo, p.pr_number,
    CASE WHEN jsonb_typeof(p.metadata->'jobs') = 'array' THEN p.metadata->'jobs' ELSE '[]'::jsonb END AS jobs,
    CASE WHEN jsonb_typeof(p.metadata->'started_jobs') = 'array' THEN p.metadata->'started_jobs' ELSE '[]'::jsonb END AS started,
    CASE WHEN jsonb_typeof(p.metadata->'deployed_jobs') = 'array' THEN p.metadata->'deployed_jobs' ELSE '[]'::jsonb END AS deployed,
    CASE WHEN jsonb_typeof(p.metadata->'job_states') = 'object' THEN p.metadata->'job_states' ELSE '{}'::jsonb END AS states
  FROM "public"."pr_registry" AS p
), job_names AS (
  SELECT repo, pr_number, jobs, started, deployed, states, n.job_name
  FROM job_meta, LATERAL (
    SELECT jsonb_array_elements_text(jobs) UNION
    SELECT jsonb_array_elements_text(started) UNION
    SELECT jsonb_array_elements_text(deployed) UNION
    SELECT jsonb_object_keys(states)
  ) AS n(job_name)
)
INSERT INTO "public"."pr_registry_jobs" ("repo", "pr_number", "job_name", "expected", "started", "deployed", "state")
SELECT repo, pr_number, job_name, jobs ? job_name, started ? job_name, deployed ? job_name,
  COALESCE(states->>job_name, '')
FROM job_names
ON CONFLICT ("repo", "pr_number", "job_name") DO UPDATE
  SET "expected" = EXCLUDED."expected", "started" = EXCLUDED."started",
      "deployed" = EXCLUDED."deployed", "state" = EXCLUDED."state"
  WHERE ("public"."pr_registry_jobs"."expected", "public"."pr_registry_jobs"."started",
         "public"."pr_registry_jobs"."deployed", "public"."pr_registry_jobs"."state")
    IS DISTINCT FROM (EXCLUDED."expected", EXCLUDED."started", EXCLUDED."deployed", EXCLUDED."state");
DELETE FROM "public"."pr_registry_jobs" AS j
USING "public"."pr_registry" AS p
WHERE p.repo = j.repo AND p.pr_number = j.pr_number
  AND NOT (
    (jsonb_typeof(p.metadata->'jobs') = 'array' AND p.metadata->'jobs' ? j.job_name)
    OR (jsonb_typeof(p.metadata->'started_jobs') = 'array' AND p.metadata->'started_jobs' ? j.job_name)
    OR (jsonb_typeof(p.metadata->'deployed_jobs') = 'array' AND p.metadata->'deployed_jobs' ? j.job_name)
    OR (jsonb_typeof(p.metadata->'job_states') = 'object' AND p.metadata->'job_states' ? j.job_name));
-- Backfill "pr_registry_kustomizations" from metadata.kustomization_requests and deployed_kustomizations
WITH kust_meta AS (
  SELECT p.repo, p.pr_number,
    CASE WHEN jsonb_typeof(p.metadata->'kustomization_requests') = 'object' THEN p.metadata->'kustomization_requests' ELSE '{}'::jsonb END AS requests,
    CASE WHEN jsonb_typeof(p.metadata->'deployed_kustomizations') = 'array' THEN p.metadata->'deployed_kustomizations' ELSE '[]'::jsonb END AS deployed
  FROM "public"."pr_registry" AS p
), kust_names AS (
  SELECT repo, pr_number, requests, deployed, n.name
  FROM kust_meta, LATERAL (
    SELECT jsonb_object_keys(requests) UNION
    SELECT jsonb_array_elements_text(deployed)
  ) AS n(name)
)
INSERT INTO "public"."pr_registry_kustomizations" ("repo", "pr_number", "name", "namespace", "requested_at", "deployed")
SELECT repo, pr_number, name, COALESCE(requests->name->>'namespace', ''),
  NULLIF(requests->name->>'requested_at', '')::timestamptz, deployed ? name
FROM kust_names
ON CONFLICT ("repo", "pr_number", "name") DO UPDATE
  SET "namespace" = EXCLUDED."namespace", "requested_at" = EXCLUDED."requested_at",
      "deployed" = EXCLUDED."deployed"
  WHERE ("public"."pr_registry_kustomizations"."namespace", "public"."pr_registry_kustomizations"."requested_at",
         "public"."pr_registry_kustomizations"."deployed")
    IS DISTINCT FROM (EXCLUDED."namespace", EXCLUDED."requested_at", EXCLUDED."deployed");
DELETE FROM "public"."pr_registry_kustomizations" AS k
USING "public"."pr_registry" AS p
WHERE p.repo = k.repo AND p.pr_number = k.pr_number
  AND NOT (
    (jsonb_typeof(p.metadata->'kustomization_requests') = 'object' AND p.metadata->'kustomization_requests' ? k.name)
    OR (jsonb_typeof(p.metadata->'deployed_kustomizations') = 'array' AND p.metadata->'deployed_kustomizations' ? k.name));
-- Backfill "pr_registry_build_selections" from metadata.build_metadata.selected
INSERT INTO "public"."pr_registry_build_selections" ("repo", "pr_number", "service", "image_name", "position")
SELECT DISTINCT ON (p.repo, p.pr_number, COALESCE(s.e->>'service', ''), COALESCE(s.e->>'image_name', ''))
  p.repo, p.pr_number, COALESCE(s.e->>'service', ''), COALESCE(s.e->>'image_name', ''), s.ord::integer
FROM "public"."pr_registry" AS p,
  jsonb_array_elements(p.metadata->'build_metadata'->'selected') WITH ORDINALITY AS s(e, ord)
WHERE jsonb_typeof(p.metadata->'build_metadata'->'selected') = 'array'
ORDER BY p.repo, p.pr_number, COALESCE(s.e->>'service', ''), COALESCE(s.e->>'image_name', ''), s.ord
ON CONFLICT ("repo", "pr_number", "service", "image_name") DO UPDATE
  SET "position" = EXCLUDED."position"
  WHERE "public"."pr_registry_build_selections"."position" IS DISTINCT FROM EXCLUDED."position";
DELETE FROM "public"."pr_registry_build_selections" AS bs
USING "public"."pr_registry" AS p
WHERE p.repo = bs.repo AND p.pr_number = bs.pr_number
  AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(
      CASE WHEN jsonb_typeof(p.metadata->'build_metadata'->'selected') = 'array'
        THEN p.metadata->'build_metadata'->'selected' ELSE '[]'::jsonb END) AS e
    WHERE COALESCE(e->>'service', '') = bs.service AND COALESCE(e->>'image_name', '') = bs.image_name);
-- Backfill "pr_registry_build_results" from metadata.build_metadata.results
INSERT INTO "public"."pr_registry_build_results"
  ("repo", "pr_number", "service", "image_name", "position", "state", "pushed", "source_attempt", "image_refs", "digest")
SELECT DISTINCT ON (p.repo, p.pr_number, COALESCE(r.e->>'service', ''), COALESCE(r.e->>'image_name', ''))
  p.repo, p.pr_number, COALESCE(r.e->>'service', ''), COALESCE(r.e->>'image_name', ''), r.ord::integer,
  COALESCE(r.e->>'state', ''), COALESCE((r.e->>'pushed')::boolean, false),
  COALESCE((r.e->>'source_attempt')::integer, 0),
  CASE WHEN jsonb_typeof(r.e->'image_refs') = 'array'
    THEN ARRAY(SELECT jsonb_array_elements_text(r.e->'image_refs')) ELSE '{}'::text[] END,
  COALESCE(r.e->>'digest', '')
FROM "public"."pr_registry" AS p,
  jsonb_array_elements(p.metadata->'build_metadata'->'results') WITH ORDINALITY AS r(e, ord)
WHERE jsonb_typeof(p.metadata->'build_metadata'->'results') = 'array'
ORDER BY p.repo, p.pr_number, COALESCE(r.e->>'service', ''), COALESCE(r.e->>'image_name', ''), r.ord
ON CONFLICT ("repo", "pr_number", "service", "image_name") DO UPDATE
  SET "position" = EXCLUDED."position", "state" = EXCLUDED."state", "pushed" = EXCLUDED."pushed",
      "source_attempt" = EXCLUDED."source_attempt", "image_refs" = EXCLUDED."image_refs",
      "digest" = EXCLUDED."digest"
  WHERE ("public"."pr_registry_build_results"."position", "public"."pr_registry_build_results"."state",
         "public"."pr_registry_build_results"."pushed", "public"."pr_registry_build_results"."source_attempt",
         "public"."pr_registry_build_results"."image_refs", "public"."pr_registry_build_results"."digest")
    IS DISTINCT FROM (EXCLUDED."position", EXCLUDED."state", EXCLUDED."pushed", EXCLUDED."source_attempt",
         EXCLUDED."image_refs", EXCLUDED."digest");
DELETE FROM "public"."pr_registry_build_results" AS br
USING "public"."pr_registry" AS p
WHERE p.repo = br.repo AND p.pr_number = br.pr_number
  AND NOT EXISTS (
    SELECT 1 FROM jsonb_array_elements(
      CASE WHEN jsonb_typeof(p.metadata->'build_metadata'->'results') = 'array'
        THEN p.metadata->'build_metadata'->'results' ELSE '[]'::jsonb END) AS e
    WHERE COALESCE(e->>'service', '') = br.service AND COALESCE(e->>'image_name', '') = br.image_name);
