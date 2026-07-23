-- =============================================================================
-- V9 — v_ai_audit view over entity_change_audit.
--
-- The admin-ui Audit Logs page (Sentrinox-admin-ui src/features/explore/audit-logs)
-- and analytics-search's FilterValuesEngine query a view named v_ai_audit with the
-- column contract of analytics-schema's R__ai_audit.sql (event_type, event_time,
-- tenant/workspace/user ids + names, audit_action, request/response bodies).
--
-- This lake has no ai_audit pipeline, so serve the CDC entity-change history
-- through the same contract instead:
--   event_type    — constant 'audit' (the UI sends event_type = 'audit' as its
--                   always-true fallback filter when no other filter is set)
--   audit_action  — '<ENTITY_TYPE> <ACTION>' (e.g. 'USER CREATE'), which also
--                   feeds the UI's action filter dropdown
--   user/workspace id + name — filled only when the changed entity IS a user /
--                   workspace (conditional dimension joins), NULL otherwise
--   tenant_id     — straight from the audit row; tenant_name resolves from
--                   the V6 tenant dimension (fed by the catalogue /list pull,
--                   keyed per (customer_id, tenant_id)) — NULL until that
--                   pair's tenant row has been captured
--
-- CREATE OR REPLACE keeps this idempotent for re-runs of ollylake-init.
-- NOTE: if the analytics-schema migrator's R__ai_audit.sql is ever applied to
-- this lake, it will replace this view with the real ai_audit-backed one.
-- =============================================================================

CREATE OR REPLACE VIEW ollylake.main.v_ai_audit AS
SELECT
    'audit'                                AS event_type,
    a.changed_at                           AS event_time,
    a.txn_id                               AS transaction_id,
    CAST(NULL AS BIGINT)                   AS sgwe_id,

    a.tenant_id,
    tn.name                                AS tenant_name,
    a.customer_id,
    CASE WHEN a.entity_type = 'WORKSPACE' THEN a.entity_id END AS workspace_id,
    ws.name                                AS workspace_name,
    CASE WHEN a.entity_type = 'USER' THEN a.entity_id END      AS user_id,
    u.name                                 AS user_name,
    changed_user.name                      AS changed_by,
    a.entity_type || ' ' || a.action       AS audit_action,
    a.entity_type,
    a.entity_id,
    a.action,

    CAST(NULL AS INTEGER)                  AS body_enc_key_id,
    a.sccal_payload                        AS request_body,
    a.human_payload                        AS response_body,
    a.changed_at                           AS created_at
FROM ollylake.main.entity_change_audit a
LEFT JOIN ollylake.main.workspace ws ON a.entity_type = 'WORKSPACE' AND ws.workspace_id = a.entity_id
LEFT JOIN ollylake.main."user"    u  ON a.entity_type = 'USER'      AND u.user_id       = a.entity_id
LEFT JOIN ollylake.main."user" changed_user  ON changed_user.user_id = a.changed_by
LEFT JOIN ollylake.main.tenant tn ON tn.customer_id = a.customer_id AND tn.tenant_id = a.tenant_id;
