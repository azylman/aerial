-- Pre-migration: migrate historical silent messages into dispatches and purge ephemeral debug threads
INSERT INTO "public"."dispatches" (
    "id", "source", "session_id", "conversation_id", "prompt", "summary",
    "effort", "model", "status", "retry_count", "restart_count",
    "error_message", "response_text", "created_at", "updated_at"
)
SELECT
    "id",
    CASE
        WHEN "thread_id" LIKE 'heartbeat-%' THEN 'heartbeat'
        WHEN "content" LIKE '[VANADIS BUILD CHUNK EXECUTION]%' THEN 'vanadis'
        ELSE 'webhook'
    END AS "source",
    CASE
        WHEN "thread_id" LIKE 'heartbeat-%' THEN "thread_id"
        ELSE ''
    END AS "session_id",
    COALESCE("conversation_id", ''),
    "content",
    COALESCE("summary", ''),
    COALESCE("effort", ''),
    COALESCE("model", ''),
    "status",
    "retry_count",
    "restart_count",
    "error_message",
    "response_text",
    "created_at",
    "updated_at"
FROM "public"."messages"
WHERE "silent" = true
ON CONFLICT ("id") DO NOTHING;

DELETE FROM "public"."messages" WHERE "silent" = true OR "thread_id" IN ('current', 't-stale-failopen');

-- Drop legacy columns on messages
ALTER TABLE "public"."messages" DROP COLUMN "silent", DROP COLUMN "ephemeral";
