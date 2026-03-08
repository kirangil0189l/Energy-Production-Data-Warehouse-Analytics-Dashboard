USE EnergyDataWarehouse;
GO

/* =========================================
   ETL 1: Load Production Hourly
   staging -> fact_production_hourly
   ========================================= */
CREATE OR ALTER PROCEDURE dw.sp_load_production_hourly
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc VARCHAR(50) = 'sp_load_production_hourly';

    IF OBJECT_ID('tempdb..#flagged') IS NOT NULL DROP TABLE #flagged;
    IF OBJECT_ID('tempdb..#valid_rows') IS NOT NULL DROP TABLE #valid_rows;
    IF OBJECT_ID('tempdb..#valid_with_date') IS NOT NULL DROP TABLE #valid_with_date;

    SELECT
        s.load_id,
        s.asset_name,
        s.prod_ts,
        s.oil_bbl,
        s.gas_mcf,
        s.water_bbl,
        s.source_system,
        a.asset_id,
        TRY_CONVERT(DATETIME2(0), s.prod_ts, 120) AS prod_dt,
        TRY_CONVERT(DECIMAL(12,2), s.oil_bbl)   AS oil_bbl_n,
        TRY_CONVERT(DECIMAL(12,2), s.gas_mcf)   AS gas_mcf_n,
        TRY_CONVERT(DECIMAL(12,2), s.water_bbl) AS water_bbl_n,
        CASE
            WHEN a.asset_id IS NULL THEN 'UNKNOWN_ASSET'
            WHEN TRY_CONVERT(DATETIME2(0), s.prod_ts, 120) IS NULL THEN 'BAD_TIMESTAMP'
            WHEN TRY_CONVERT(DECIMAL(12,2), s.oil_bbl) IS NULL
              OR TRY_CONVERT(DECIMAL(12,2), s.gas_mcf) IS NULL
              OR TRY_CONVERT(DECIMAL(12,2), s.water_bbl) IS NULL THEN 'BAD_NUMERIC'
            WHEN TRY_CONVERT(DECIMAL(12,2), s.oil_bbl) < 0
              OR TRY_CONVERT(DECIMAL(12,2), s.gas_mcf) < 0
              OR TRY_CONVERT(DECIMAL(12,2), s.water_bbl) < 0 THEN 'NEGATIVE_VOLUME'
            ELSE NULL
        END AS err_code
    INTO #flagged
    FROM dw.stg_production_hourly s
    LEFT JOIN dw.dim_asset a
      ON UPPER(LTRIM(RTRIM(a.asset_name))) = UPPER(LTRIM(RTRIM(s.asset_name)));

    INSERT INTO dw.etl_error_log(process_name, record_key, err_msg, row_payload)
    SELECT
        @proc,
        CONCAT(asset_name, '|', ISNULL(prod_ts, '')),
        CONCAT('Rejected row: ', err_code),
        CONCAT(
            'asset_name=', ISNULL(asset_name, ''),
            '; prod_ts=', ISNULL(prod_ts, ''),
            '; oil=', ISNULL(oil_bbl, ''),
            '; gas=', ISNULL(gas_mcf, ''),
            '; water=', ISNULL(water_bbl, '')
        )
    FROM #flagged
    WHERE err_code IS NOT NULL;

    SELECT
        asset_id,
        CONVERT(INT, CONVERT(CHAR(8), prod_dt, 112)) AS date_key,
        prod_dt,
        oil_bbl_n   AS oil_bbl,
        gas_mcf_n   AS gas_mcf,
        water_bbl_n AS water_bbl,
        source_system
    INTO #valid_rows
    FROM #flagged
    WHERE err_code IS NULL;

    INSERT INTO dw.etl_error_log(process_name, record_key, err_msg, row_payload)
    SELECT
        @proc,
        CONCAT(v.asset_id, '|', CONVERT(VARCHAR(19), v.prod_dt, 120)),
        CONCAT('Rejected row: MISSING_DIM_DATE date_key=', v.date_key),
        CONCAT(
            'asset_id=', v.asset_id,
            '; prod_dt=', CONVERT(VARCHAR(19), v.prod_dt, 120),
            '; date_key=', v.date_key
        )
    FROM #valid_rows v
    LEFT JOIN dw.dim_date d
      ON d.date_key = v.date_key
    WHERE d.date_key IS NULL;

    SELECT v.*
    INTO #valid_with_date
    FROM #valid_rows v
    INNER JOIN dw.dim_date d
      ON d.date_key = v.date_key;

    MERGE dw.fact_production_hourly AS tgt
    USING #valid_with_date AS src
      ON tgt.asset_id = src.asset_id
     AND tgt.prod_dt  = src.prod_dt
    WHEN MATCHED THEN
        UPDATE SET
            tgt.date_key      = src.date_key,
            tgt.oil_bbl       = src.oil_bbl,
            tgt.gas_mcf       = src.gas_mcf,
            tgt.water_bbl     = src.water_bbl,
            tgt.source_system = src.source_system
    WHEN NOT MATCHED THEN
        INSERT (asset_id, date_key, prod_dt, oil_bbl, gas_mcf, water_bbl, source_system)
        VALUES (src.asset_id, src.date_key, src.prod_dt, src.oil_bbl, src.gas_mcf, src.water_bbl, src.source_system);
END
GO

/* =========================================
   ETL 2: Load Downtime Events
   staging -> fact_downtime_events
   ========================================= */
CREATE OR ALTER PROCEDURE dw.sp_load_downtime_events
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @proc VARCHAR(50) = 'sp_load_downtime_events';

    IF OBJECT_ID('tempdb..#flagged_dt') IS NOT NULL DROP TABLE #flagged_dt;
    IF OBJECT_ID('tempdb..#valid_dt') IS NOT NULL DROP TABLE #valid_dt;

    SELECT
        s.load_id,
        s.asset_name,
        s.start_ts,
        s.end_ts,
        s.reason_code,
        s.reason_desc,
        s.is_planned,
        a.asset_id,
        TRY_CONVERT(DATETIME2(0), s.start_ts, 120) AS start_dt,
        TRY_CONVERT(DATETIME2(0), s.end_ts, 120)   AS end_dt,
        CASE
            WHEN a.asset_id IS NULL THEN 'UNKNOWN_ASSET'
            WHEN TRY_CONVERT(DATETIME2(0), s.start_ts, 120) IS NULL
              OR TRY_CONVERT(DATETIME2(0), s.end_ts, 120) IS NULL THEN 'BAD_TIMESTAMP'
            WHEN TRY_CONVERT(DATETIME2(0), s.end_ts, 120) <= TRY_CONVERT(DATETIME2(0), s.start_ts, 120) THEN 'END_BEFORE_START'
            ELSE NULL
        END AS err_code
    INTO #flagged_dt
    FROM dw.stg_downtime_events s
    LEFT JOIN dw.dim_asset a
      ON UPPER(LTRIM(RTRIM(a.asset_name))) = UPPER(LTRIM(RTRIM(s.asset_name)));

    INSERT INTO dw.etl_error_log(process_name, record_key, err_msg, row_payload)
    SELECT
        @proc,
        CONCAT(asset_name, '|', ISNULL(start_ts, '')),
        CONCAT('Rejected row: ', err_code),
        CONCAT(
            'asset_name=', ISNULL(asset_name, ''),
            '; start_ts=', ISNULL(start_ts, ''),
            '; end_ts=', ISNULL(end_ts, ''),
            '; reason=', ISNULL(reason_code, '')
        )
    FROM #flagged_dt
    WHERE err_code IS NOT NULL;

    SELECT
        asset_id,
        start_dt,
        end_dt,
        CAST(DATEDIFF(SECOND, start_dt, end_dt) / 3600.0 AS DECIMAL(8,2)) AS duration_hours,
        reason_code,
        reason_desc,
        CASE
            WHEN UPPER(LTRIM(RTRIM(ISNULL(is_planned, '')))) IN ('Y','YES','TRUE','1') THEN 'Y'
            ELSE 'N'
        END AS is_planned_norm
    INTO #valid_dt
    FROM #flagged_dt
    WHERE err_code IS NULL;

    INSERT INTO dw.fact_downtime_events
        (event_id, asset_id, start_dt, end_dt, duration_hours, reason_code, reason_desc, is_planned)
    SELECT
        NEXT VALUE FOR dw.seq_downtime_event_id,
        v.asset_id,
        v.start_dt,
        v.end_dt,
        v.duration_hours,
        v.reason_code,
        v.reason_desc,
        v.is_planned_norm
    FROM #valid_dt v;
END
GO