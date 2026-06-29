CREATE TABLE IF NOT EXISTS ollylake.main.customer_tenant_reference (
                                                                       customer_id BIGINT NOT NULL,
                                                                       tenant_id   BIGINT NOT NULL
    );

-- Seed row. Guarded so re-running the init is idempotent.
INSERT INTO ollylake.main.customer_tenant_reference (customer_id, tenant_id)
SELECT 322680303510360064, 322706726937694208
WHERE NOT EXISTS (
    SELECT 1 FROM ollylake.main.customer_tenant_reference
    WHERE customer_id = 322680303510360064
      AND tenant_id   = 322706726937694208
);
