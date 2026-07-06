-- =============================================================================
-- V6 — Reference / dimension tables.
--
-- Lookup tables for the IDs stored in ai_txn. Each table holds only the
-- stable identity columns (id + name). Richer attributes live in the
-- source-of-truth systems; these rows exist solely so analytics queries can
-- join a readable name onto a numeric ID without hitting an external service.
--
-- customer_id scopes a row to its owning customer, so the per-customer row
-- filter (the JWT x-dd-access grant) applies to these dimensions the same way
-- it does to the fact tables. It is present on the per-customer dimensions —
-- which are also PARTITIONED BY (customer_id) so per-customer reads prune to a
-- single partition — and absent from the GLOBAL lookups (provider, mcp_server),
-- which every customer may read. See analytics-search ACCESS_TOKEN_ISSUER_SPEC.md.
--
-- NOTE: "user" and "group" are SQL reserved words and must always be quoted.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ollylake.main.tenant (
                                                    tenant_id   BIGINT  NOT NULL,
                                                    customer_id BIGINT  NOT NULL,
                                                    name        VARCHAR NOT NULL,
                                                    is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.tenant SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main.workspace (
                                                       workspace_id    BIGINT  NOT NULL,
                                                       customer_id     BIGINT  NOT NULL,
                                                       name            VARCHAR NOT NULL,
                                                       is_deleted      BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.workspace SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main."user" (
                                                    user_id     BIGINT  NOT NULL,
                                                    customer_id BIGINT  NOT NULL,
                                                    name        VARCHAR NOT NULL,
                                                    is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main."user" SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main."group" (
                                                     group_id    BIGINT  NOT NULL,
                                                     customer_id BIGINT  NOT NULL,
                                                     name        VARCHAR NOT NULL,
                                                     is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main."group" SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main.user_group_mapping (
                                                                user_id     BIGINT  NOT NULL,
                                                                group_id    BIGINT  NOT NULL,
                                                                customer_id BIGINT  NOT NULL,
                                                                is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.user_group_mapping SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main.vkey (

                                                  vkey_id     BIGINT  NOT NULL,
                                                  customer_id BIGINT  NOT NULL,
                                                  name        VARCHAR NOT NULL,
                                                  is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.vkey SET PARTITIONED BY (customer_id);

-- Global lookup — every customer may read it. No customer_id, not partitioned.
-- provider_id is INTEGER in ai_txn — keep consistent.
CREATE TABLE IF NOT EXISTS ollylake.main.provider (
                                                      provider_id     INTEGER NOT NULL,
                                                      name            VARCHAR NOT NULL,
                                                      is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

-- Global lookup — every customer may read it. No customer_id, not partitioned.
CREATE TABLE IF NOT EXISTS ollylake.main.mcp_server (
                                                        mcp_server_id   BIGINT  NOT NULL,
                                                        name            VARCHAR NOT NULL,
                                                        is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS ollylake.main.mcp_access_rule (
                                                             mcp_access_rule_id  BIGINT  NOT NULL,
                                                             customer_id         BIGINT  NOT NULL,
                                                             name                VARCHAR NOT NULL,
                                                             is_deleted          BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.mcp_access_rule SET PARTITIONED BY (customer_id);

CREATE TABLE IF NOT EXISTS ollylake.main.llm_access_rule (
                                                             llm_access_rule_id  BIGINT  NOT NULL,
                                                             customer_id         BIGINT  NOT NULL,
                                                             name                VARCHAR NOT NULL,
                                                             is_deleted          BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.llm_access_rule SET PARTITIONED BY (customer_id);

-- budget_rule_ids BIGINT[] in ai_txn references rows in this table.
CREATE TABLE IF NOT EXISTS ollylake.main.budget_rule (
                                                         budget_rule_id  BIGINT  NOT NULL,
                                                         customer_id     BIGINT  NOT NULL,
                                                         name            VARCHAR NOT NULL,
                                                         is_deleted      BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.budget_rule SET PARTITIONED BY (customer_id);