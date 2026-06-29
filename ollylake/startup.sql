-- =============================================================================
-- Startup SQL for the DuckDB connection (loaded via StartupScriptProvider).
--
-- Installs the DuckLake + S3 extensions, points DuckDB at the MinIO object
-- store and ATTACHes the 'ollylake' DuckLake catalog (data files -> MinIO,
-- metadata -> the persisted .ducklake file). After this runs, queries can
-- read/write ollylake.main.* tables.
--
-- Placeholders written as dollar-brace VAR are substituted from the process
-- environment by StartupScriptProvider.replaceEnvVariable() and MUST all be set
-- (the substitution scans this whole file, comments included):
--   MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, S3_ENDPOINT, OLLYLAKE_BUCKET, CATALOG_PATH
-- =============================================================================

INSTALL ducklake;
INSTALL httpfs;
LOAD ducklake;
LOAD httpfs;

-- Point DuckDB at MinIO.
CREATE OR REPLACE SECRET minio (
    TYPE s3,
    KEY_ID '${MINIO_ROOT_USER}',
    SECRET '${MINIO_ROOT_PASSWORD}',
    ENDPOINT '${S3_ENDPOINT}',
    URL_STYLE 'path',
    USE_SSL false
);

-- Attach the DuckLake catalog; data files go to the MinIO bucket.
ATTACH IF NOT EXISTS 'ducklake:${CATALOG_PATH}' AS ollylake (DATA_PATH 's3://${OLLYLAKE_BUCKET}/');

USE ollylake;
