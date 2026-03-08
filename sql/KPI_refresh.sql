CREATE OR ALTER PROCEDURE dw.sp_refresh_kpi_daily
  @from_date DATE,
  @to_date   DATE
AS
BEGIN
  SET NOCOUNT ON;

  DECLARE @from_key INT = CONVERT(INT, CONVERT(CHAR(8), @from_date, 112));
  DECLARE @to_key   INT = CONVERT(INT, CONVERT(CHAR(8), @to_date, 112));

  DELETE FROM dw.kpi_daily_asset
  WHERE date_key BETWEEN @from_key AND @to_key;

  ;WITH prod AS (
    SELECT
      date_key,
      asset_id,
      SUM(oil_bbl)   AS oil_bbl_day,
      SUM(gas_mcf)   AS gas_mcf_day,
      SUM(water_bbl) AS water_bbl_day
    FROM dw.fact_production_hourly
    WHERE date_key BETWEEN @from_key AND @to_key
    GROUP BY date_key, asset_id
  ),
  dt AS (
    SELECT
      CONVERT(INT, CONVERT(CHAR(8), CAST(start_dt AS DATE), 112)) AS date_key,
      asset_id,
      SUM(duration_hours) AS downtime_hours
    FROM dw.fact_downtime_events
    WHERE CAST(start_dt AS DATE) BETWEEN @from_date AND @to_date
    GROUP BY CONVERT(INT, CONVERT(CHAR(8), CAST(start_dt AS DATE), 112)), asset_id
  )
  INSERT INTO dw.kpi_daily_asset
    (date_key, asset_id, oil_bbl_day, gas_mcf_day, water_bbl_day, downtime_hours, uptime_pct, refresh_dtm)
  SELECT
    p.date_key,
    p.asset_id,
    p.oil_bbl_day,
    p.gas_mcf_day,
    p.water_bbl_day,
    ISNULL(d.downtime_hours, 0),
    CAST((24.0 - ISNULL(d.downtime_hours,0)) / 24.0 * 100.0 AS DECIMAL(6,2)),
    SYSDATETIME()
  FROM prod p
  LEFT JOIN dt d
    ON d.date_key = p.date_key
   AND d.asset_id = p.asset_id;
END
GO


--running kpi_daily_asset
EXEC dw.sp_refresh_kpi_daily
  @from_date = '2026-01-01',
  @to_date   = '2026-01-05';


SELECT DB_NAME() AS current_db;

SELECT COUNT(*) AS prod_rows FROM dw.fact_production_hourly;
SELECT COUNT(*) AS dt_rows   FROM dw.fact_downtime_events;
SELECT COUNT(*) AS date_rows FROM dw.dim_date;


DELETE FROM dw.kpi_daily_asset;
GO

INSERT INTO dw.kpi_daily_asset
    (date_key, asset_id, oil_bbl_day, gas_mcf_day, water_bbl_day, downtime_hours, uptime_pct, refresh_dtm)
SELECT
    p.date_key,
    p.asset_id,
    SUM(p.oil_bbl)   AS oil_bbl_day,
    SUM(p.gas_mcf)   AS gas_mcf_day,
    SUM(p.water_bbl) AS water_bbl_day,
    ISNULL(d.downtime_hours, 0) AS downtime_hours,
    CAST((24.0 - ISNULL(d.downtime_hours,0)) / 24.0 * 100.0 AS DECIMAL(6,2)) AS uptime_pct,
    SYSDATETIME()
FROM dw.fact_production_hourly p
LEFT JOIN (
    SELECT
        CONVERT(INT, CONVERT(CHAR(8), CAST(start_dt AS DATE), 112)) AS date_key,
        asset_id,
        SUM(duration_hours) AS downtime_hours
    FROM dw.fact_downtime_events
    GROUP BY CONVERT(INT, CONVERT(CHAR(8), CAST(start_dt AS DATE), 112)), asset_id
) d
    ON p.date_key = d.date_key
   AND p.asset_id = d.asset_id
GROUP BY
    p.date_key,
    p.asset_id,
    d.downtime_hours;
GO

SELECT COUNT(*) AS kpi_rows
FROM dw.kpi_daily_asset;

SELECT *
FROM dw.kpi_daily_asset
ORDER BY date_key, asset_id;