-- Run log for silver/gold notebook executions.
-- Lives in Fabric SQL database `sqldb_control` alongside meta.pipeline_config.
-- Re-runnable: creates the table only if missing.
-- Long-term target: notebooks write here via JDBC + managed identity.
-- Until JDBC is wired, notebooks append to lh_data.gold._run_log (Delta).

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta')
    EXEC('CREATE SCHEMA meta');
GO

IF OBJECT_ID('meta.run_log', 'U') IS NULL
CREATE TABLE meta.run_log (
    id              INT IDENTITY(1,1) PRIMARY KEY,
    run_id          VARCHAR(32)    NOT NULL,
    notebook        VARCHAR(64)    NOT NULL,   -- 'silver' or 'gold'
    table_name      VARCHAR(128)   NOT NULL,
    status          VARCHAR(8)     NOT NULL,   -- 'OK' or 'FAIL'
    rows_in         INT            NOT NULL DEFAULT 0,
    rows_out        INT            NOT NULL DEFAULT 0,
    error_message   VARCHAR(4000)  NULL,
    run_ts          DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Index for querying recent runs
CREATE NONCLUSTERED INDEX ix_run_log_run_id
    ON meta.run_log (run_id, notebook);
GO

SELECT TOP 20 * FROM meta.run_log ORDER BY run_ts DESC;
