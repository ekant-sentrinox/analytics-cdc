-- =============================================================================
-- V7 — Local cursor state for SCCAL polling.
--
-- The CDC job consumes the SCCAL endpoints:
--   GET /internal/sccal/api/v1/cursors            — global registry: which tenants advanced
--   GET /internal/sccal/api/v1/changes            — per-tenant entity_change stream
--   GET /internal/sccal/api/v1/{objectType}/list  — global catalogue snapshot + updatedSince delta
--
-- These tables persist where the consumer got to, so a restarted job resumes
-- instead of re-reading. All are written inside the same transaction as the
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

-- Per-objectType incremental cursor for the catalogue /list pull (the tables
-- it feeds are in V6): the `updatedSince` to send on the next delta pull, as an
-- ISO-8601 UTC instant. The /list payload carries no updated_at, so the
-- consumer advances this to (request start - a fixed overlap lap) after a
-- non-empty pull — the strict `updated_at > since` bound plus the idempotent
-- merge make the overlap harmless. A type with no row here has never pulled
-- data and bootstraps with a full live snapshot (no updatedSince).
CREATE TABLE IF NOT EXISTS ollylake.main.sccal_catalogue_cursor (
                                                                    object_type        VARCHAR NOT NULL,
                                                                    next_updated_since VARCHAR NOT NULL
);

-- Singleton (zero or one row): the last-applied global-catalog schema revision
-- (MAX(installed_rank) of the source's global Flyway history), read from the
-- `globalCatalog` gate embedded in every /cursors page. Catalogue /list pulls
-- are skipped while the gate value matches — catalog rows only change when a
-- global migration lands. Absent row = never pulled.
CREATE TABLE IF NOT EXISTS ollylake.main.sccal_catalogue_revision (
                                                                      schema_revision BIGINT NOT NULL
);
