-- =============================================================================
-- V6 — Reference / dimension tables.
--
-- Lookup tables for the IDs stored in ai_txn. Each table holds only the
-- stable identity columns (id + name). Richer attributes live in the
-- source-of-truth systems; these rows exist solely so analytics queries can
-- join a readable name onto a numeric ID without hitting an external service.
--
-- NOTE: "user" and "group" are SQL reserved words and must always be quoted.
-- =============================================================================

CREATE TABLE IF NOT EXISTS ollylake.main.tenant (
                                                    tenant_id   BIGINT  NOT NULL,
                                                    name        VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.workspace (
                                                       workspace_id    BIGINT  NOT NULL,
                                                       name            VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main."user" (
                                                    user_id     BIGINT  NOT NULL,
                                                    name        VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main."group" (
                                                     group_id    BIGINT  NOT NULL,
                                                     name        VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.user_group_mapping (
                                                                user_id     BIGINT  NOT NULL,
                                                                group_id    BIGINT  NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.vkey (
                                                  vkey_id     BIGINT  NOT NULL,
                                                  name        VARCHAR NOT NULL
);

-- provider_id is INTEGER in ai_txn — keep consistent.
CREATE TABLE IF NOT EXISTS ollylake.main.provider (
                                                      provider_id     INTEGER NOT NULL,
                                                      name            VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.mcp_server (
                                                        mcp_server_id   BIGINT  NOT NULL,
                                                        name            VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.mcp_access_rule (
                                                             mcp_access_rule_id  BIGINT  NOT NULL,
                                                             name                VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS ollylake.main.llm_access_rule (
                                                             llm_access_rule_id  BIGINT  NOT NULL,
                                                             name                VARCHAR NOT NULL
);

-- budget_rule_ids BIGINT[] in ai_txn references rows in this table.
CREATE TABLE IF NOT EXISTS ollylake.main.budget_rule (
                                                         budget_rule_id  BIGINT  NOT NULL,
                                                         name            VARCHAR NOT NULL
);