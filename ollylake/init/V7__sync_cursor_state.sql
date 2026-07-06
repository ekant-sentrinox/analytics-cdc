-- =============================================================================
-- V7 — Local cursor state for SCCAL change-stream polling.
--
-- The CDC job consumes two offset-paged SCCAL endpoints:
--   GET /internal/sccal/api/v1/cursors  — global registry: which tenants advanced
--   GET /internal/sccal/api/v1/changes  — per-tenant entity_change stream
--
-- These tables persist where the consumer got to, so a restarted job resumes
-- instead of re-reading. Both are written inside the same transaction as the
-- data merge they belong to, so an idle poll writes no DuckLake snapshot —
-- with one exception: when an idle poll finds a page-or-more backlog of
-- out-of-scope registry rows, the registry offset is advanced on its own to
-- keep the backlog from being re-paged forever.
-- =============================================================================

-- Per-(customer, tenant) position in that tenant's /changes stream: the
-- startOffset (entity_change.id) to request next. A pair with no row here has
-- never been bootstrapped — the job full-snapshots the stream-fed tables and
-- seeds the row from the registry head.
CREATE TABLE IF NOT EXISTS ollylake.main.sccal_change_cursor (
                                                                 customer_id        BIGINT NOT NULL,
                                                                 tenant_id          BIGINT NOT NULL,
                                                                 next_change_offset BIGINT NOT NULL
);

-- Singleton (zero or one row): the startOffset (registry seq) for the next
-- /cursors discovery call. Absent row = start from 1.
CREATE TABLE IF NOT EXISTS ollylake.main.sccal_registry_cursor (
                                                                   next_seq_offset BIGINT NOT NULL
);
