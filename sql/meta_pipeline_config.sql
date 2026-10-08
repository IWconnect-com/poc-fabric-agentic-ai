-- Control table for the metadata-driven ingestion pipeline.
-- Lives in the Fabric SQL database `sqldb_control` (NOT in a source database).
-- Re-runnable: creates the table only if missing, seeds only if empty.
-- To upgrade an older version: DROP TABLE meta.pipeline_config; then run this script.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')
    EXEC('CREATE SCHEMA meta');
GO

IF OBJECT_ID('meta.pipeline_config', 'U') IS NULL
CREATE TABLE meta.pipeline_config (
    id               INT          NOT NULL PRIMARY KEY,
    source_type      VARCHAR(16)  NOT NULL DEFAULT 'sql',
    source_schema    VARCHAR(128) NOT NULL,
    table_name       VARCHAR(128) NOT NULL,
    target_table     VARCHAR(128) NOT NULL,
    load_strategy    VARCHAR(24)  NOT NULL DEFAULT 'full',
    watermark_column VARCHAR(128) NULL,
    enabled          BIT          NOT NULL DEFAULT 1,
    load_order       INT          NOT NULL,
    description      VARCHAR(512) NULL,
    CONSTRAINT uq_pipeline_config UNIQUE (source_type, source_schema, table_name)
);
GO

IF NOT EXISTS (SELECT 1 FROM meta.pipeline_config)
INSERT INTO meta.pipeline_config
    (id, source_schema, table_name, target_table, load_order, description)
VALUES
    (1, 'dbo', 'Party',      'bronze_party',      10, 'Individuals and organisations'),
    (2, 'dbo', 'Person',     'bronze_person',     20, 'Natural-person details linked to Party'),
    (3, 'dbo', 'Vehicle',    'bronze_vehicle',    30, 'Insured vehicles'),
    (4, 'dbo', 'Policy',     'bronze_policy',     40, 'Insurance policy master records'),
    (5, 'dbo', 'Coverage',   'bronze_coverage',   50, 'Coverage lines attached to a policy'),
    (6, 'dbo', 'Occurrence', 'bronze_occurrence', 60, 'Loss occurrence / event records'),
    (7, 'dbo', 'Claim',      'bronze_claim',      70, 'Claims filed against a policy');
GO

SELECT * FROM meta.pipeline_config ORDER BY load_order;
