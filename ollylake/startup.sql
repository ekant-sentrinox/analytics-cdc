-- =============================================================================
-- Startup SQL for the DuckDB connection (loaded via StartupScriptProvider).
--
-- Installs the DuckLake + S3 + Postgres extensions, points DuckDB at the MinIO
-- object store and ATTACHes the 'ollylake' DuckLake catalog. Data files live in
-- MinIO; the DuckLake catalog metadata (snapshots / tables / schema bookkeeping)
-- lives in the PostgreSQL metadata-db. After this runs, queries can read/write
-- ollylake.main.* tables.
--
-- Placeholders written as dollar-brace VAR are substituted from the process
-- environment by StartupScriptProvider.replaceEnvVariable() and MUST all be set
-- (the substitution scans this whole file, comments included):
--   MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, S3_ENDPOINT, OLLYLAKE_BUCKET,
--   METADATA_DB_HOST, METADATA_DB_PORT, METADATA_DB_NAME,
--   METADATA_DB_USER, METADATA_DB_PASSWORD
-- =============================================================================

INSTALL ducklake;
INSTALL httpfs;
INSTALL postgres;
LOAD ducklake;
LOAD httpfs;
LOAD postgres;

-- Point DuckDB at MinIO (where the DuckLake data files are written).
CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID '${MINIO_ROOT_USER}',
    SECRET '${MINIO_ROOT_PASSWORD}',
    ENDPOINT '${S3_ENDPOINT}',
    URL_STYLE 'path',
    USE_SSL false
);

-- Attach the DuckLake catalog: metadata -> PostgreSQL metadata-db, data files
-- -> the MinIO bucket. The string after 'ducklake:postgres:' is a libpq
-- connection string.
ATTACH IF NOT EXISTS 'ducklake:postgres:host=${METADATA_DB_HOST} port=${METADATA_DB_PORT} dbname=${METADATA_DB_NAME} user=${METADATA_DB_USER} password=${METADATA_DB_PASSWORD}' AS ollylake (DATA_PATH 's3://${OLLYLAKE_BUCKET}/');

-- DuckLake inlines small inserts (< 10 rows) into the Postgres catalog by
-- default instead of writing Parquet to MinIO. Disable that so every table's
-- data lands in the bucket as Parquet, and flush anything already inlined.
-- Both calls are idempotent (the option persists in the catalog; the flush is
-- a no-op when nothing is inlined).
CALL ollylake.set_option('data_inlining_row_limit', 0);
CALL ducklake_flush_inlined_data('ollylake');

USE ollylake;
