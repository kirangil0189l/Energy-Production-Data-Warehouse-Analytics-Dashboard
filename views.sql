CREATE OR ALTER VIEW dw.vw_kpi_daily AS
SELECT
  d.full_date,
  a.asset_name,
  a.asset_type,
  f.field_name,
  f.operator_name,
  k.oil_bbl_day,
  k.gas_mcf_day,
  k.water_bbl_day,
  k.downtime_hours,
  k.uptime_pct
FROM dw.kpi_daily_asset k
JOIN dw.dim_date d  ON d.date_key = k.date_key
JOIN dw.dim_asset a ON a.asset_id = k.asset_id
JOIN dw.dim_field f ON f.field_id = a.field_id;
GO

CREATE OR ALTER VIEW dw.vw_downtime_reason_daily AS
SELECT
  CAST(e.start_dt AS DATE) AS event_date,
  a.asset_name,
  e.reason_code,
  e.reason_desc,
  SUM(e.duration_hours) AS downtime_hours
FROM dw.fact_downtime_events e
JOIN dw.dim_asset a ON a.asset_id = e.asset_id
GROUP BY CAST(e.start_dt AS DATE), a.asset_name, e.reason_code, e.reason_desc;
GO

CREATE OR ALTER VIEW dw.vw_production_hourly AS
SELECT
  p.prod_dt,
  d.full_date,
  a.asset_name,
  a.asset_type,
  p.oil_bbl,
  p.gas_mcf,
  p.water_bbl,
  p.source_system
FROM dw.fact_production_hourly p
JOIN dw.dim_asset a ON a.asset_id = p.asset_id
JOIN dw.dim_date d ON d.date_key = p.date_key;
GO


SELECT * FROM dw.vw_kpi_daily ORDER BY full_date, asset_name;