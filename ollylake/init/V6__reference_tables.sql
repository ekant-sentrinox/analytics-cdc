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


CREATE TABLE IF NOT EXISTS ollylake.main.vkey (

                                                  vkey_id     BIGINT  NOT NULL,
                                                  customer_id BIGINT  NOT NULL,
                                                  name        VARCHAR NOT NULL,
                                                  is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.vkey SET PARTITIONED BY (customer_id);

-- Global lookups — every customer may read them. No customer_id, not
-- partitioned. Fed from the catalogue /list pull (snapshot + updatedSince
-- deltas — the global catalog tier is not in the /changes stream; see the
-- admin layer's global-catalog-sync.md). Ids are the source cluster's
-- Snowflake ids copied verbatim so they join to fact data — hence BIGINT
-- (ai_txn's INTEGER provider_id still joins via implicit widening).
CREATE TABLE IF NOT EXISTS ollylake.main.provider (
                                                      provider_id     BIGINT  NOT NULL,
                                                      name            VARCHAR NOT NULL,
                                                      is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

-- Global lookup fed from the catalogue pull, like provider. provider_id links
-- a model to its provider (a join key, kept alongside the name).
CREATE TABLE IF NOT EXISTS ollylake.main.model (
                                                   model_id        BIGINT  NOT NULL,
                                                   name            VARCHAR NOT NULL,
                                                   provider_id     BIGINT  NOT NULL,
                                                   is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

-- Global lookup fed from the catalogue pull, like provider.
CREATE TABLE IF NOT EXISTS ollylake.main.mcp_server (
                                                        mcp_server_id   BIGINT  NOT NULL,
                                                        name            VARCHAR NOT NULL,
                                                        is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

-- Global lookup fed from the catalogue pull. mcp_server_id links a tool to
-- its server (a join key, kept alongside the name).
CREATE TABLE IF NOT EXISTS ollylake.main.mcp_tool (
                                                      mcp_tool_id     BIGINT  NOT NULL,
                                                      name            VARCHAR NOT NULL,
                                                      mcp_server_id   BIGINT  NOT NULL,
                                                      is_deleted      BOOLEAN NOT NULL DEFAULT 0
);

-- Per-customer, fed from the catalogue pull's per-pair tenant/list snapshot
-- (the /changes stream carries no TENANT entity). Never tombstoned — the
-- endpoint always answers isDeleted:false.
CREATE TABLE IF NOT EXISTS ollylake.main.tenant (
                                                    tenant_id   BIGINT  NOT NULL,
                                                    customer_id BIGINT  NOT NULL,
                                                    name        VARCHAR NOT NULL,
                                                    is_deleted  BOOLEAN NOT NULL DEFAULT 0
);
ALTER TABLE ollylake.main.tenant SET PARTITIONED BY (customer_id);

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