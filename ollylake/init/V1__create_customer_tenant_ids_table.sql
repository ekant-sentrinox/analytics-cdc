CREATE TABLE IF NOT EXISTS ollylake.main.customer_tenant_reference (
                                                                       customer_id BIGINT NOT NULL,
                                                                       tenant_id   BIGINT NOT NULL
    );

-- The seed pair was rotated; environments that ran the previous version of
-- this file still hold the old row. The pair set defines the capture (and
-- delete) scope, so remove the retired pair explicitly — the guarded INSERT
-- below only checks the new one.
DELETE FROM ollylake.main.customer_tenant_reference
WHERE customer_id = 322680303510360064
  AND tenant_id   = 322706726937694208;

-- Seed row. Guarded so re-running the init is idempotent.
INSERT INTO ollylake.main.customer_tenant_reference (customer_id, tenant_id)
SELECT 66109578638528512, 66108634651693056
WHERE NOT EXISTS (
    SELECT 1 FROM ollylake.main.customer_tenant_reference
    WHERE customer_id = 66109578638528512
      AND tenant_id   = 66108634651693056
);
