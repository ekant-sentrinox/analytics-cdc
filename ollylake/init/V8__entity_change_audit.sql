-- =============================================================================
-- V8 — Audit log of consumed entity-change entries.
--
-- Every /changes entry the CDC job pulls is appended here verbatim (ALL entity
-- types, including ones that feed no reference table, e.g. ACTIVATION) before
-- being merged into the dimension tables — an immutable, queryable history of
-- "who changed what, when, in which activation commit".
--
-- Payloads are stored as JSON text in VARCHAR columns (DuckLake/Parquet has no
-- native JSON type); shred them at query time with DuckDB's json functions.
--
-- The log only contains what the stream still had when this consumer first
-- polled: history from before SCCAL's change capture, and ranges skipped by a
-- bootstrap / 410-Gone snapshot resync, are not replayable and thus absent.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ollylake.main.entity_change_audit (
                                                                 change_id     BIGINT  NOT NULL,  -- entity_change.id (unique per customer schema)
                                                                 customer_id   BIGINT  NOT NULL,
                                                                 tenant_id     BIGINT  NOT NULL,
                                                                 entity_type   VARCHAR NOT NULL,  -- USER, ACTIVATION, ...
                                                                 entity_id     BIGINT,            -- id of the changed entity (ACTIVATION: the commit id)
                                                                 action        VARCHAR NOT NULL,  -- CREATE / UPDATE / DELETE / ACTIVATE
                                                                 commit_id     BIGINT,            -- activation commit the change belongs to
                                                                 changed_at    TIMESTAMPTZ,       -- audit time stamped by SCCAL
                                                                 sccal_payload VARCHAR,           -- minimal CDC shape (JSON text)
                                                                 human_payload VARCHAR            -- full response DTO (JSON text; secret-free upstream)
);
ALTER TABLE ollylake.main.entity_change_audit
    SET PARTITIONED BY (customer_id, day(changed_at));
