Create database EnergyDataWarehouse;
use EnergyDataWarehouse;
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');
GO
--Creating Dimension Tables
-- Dimensions
--dim_filed -> Represents an oil and gas field.
CREATE TABLE dw.dim_field (
  field_id       INT            NOT NULL PRIMARY KEY,
  field_name     VARCHAR(100)   NOT NULL,
  basin          VARCHAR(120)   NULL,
  province       VARCHAR(10)    NULL,
  operator_name  VARCHAR(120)   NULL
);

--dim_asset->Represents physical equipment
CREATE TABLE dw.dim_asset (
  asset_id     INT           NOT NULL PRIMARY KEY,
  asset_name   VARCHAR(100)  NOT NULL UNIQUE,
  asset_type   VARCHAR(30)   NOT NULL CHECK (asset_type IN ('WELL','COMPRESSOR','FACILITY')),
  field_id     INT           NOT NULL FOREIGN KEY REFERENCES dw.dim_field(field_id),
  status       VARCHAR(20)   NOT NULL CHECK (status IN ('ACTIVE','INACTIVE')),
  start_date   DATE          NULL
);
--Standard data warehouse date dimension
CREATE TABLE dw.dim_date (
  date_key   INT        NOT NULL PRIMARY KEY, -- YYYYMMDD
  full_date  DATE       NOT NULL,
  year_num   SMALLINT   NOT NULL,
  month_num  TINYINT    NOT NULL,
  day_num    TINYINT    NOT NULL,
  day_name   CHAR(3)    NOT NULL
);

-- Fact Tables
--main operational measurement table
CREATE TABLE dw.fact_production_hourly (
  asset_id      INT            NOT NULL FOREIGN KEY REFERENCES dw.dim_asset(asset_id),
  date_key      INT            NOT NULL FOREIGN KEY REFERENCES dw.dim_date(date_key),
  prod_dt       DATETIME2(0)   NOT NULL,
  oil_bbl       DECIMAL(12,2)  NOT NULL DEFAULT 0,
  gas_mcf       DECIMAL(12,2)  NOT NULL DEFAULT 0,
  water_bbl     DECIMAL(12,2)  NOT NULL DEFAULT 0,
  source_system VARCHAR(30)    NULL,
  CONSTRAINT pk_fact_production PRIMARY KEY (asset_id, prod_dt)
);
--Stores downtime incidents
CREATE TABLE dw.fact_downtime_events (
  event_id       INT           NOT NULL PRIMARY KEY,
  asset_id       INT           NOT NULL FOREIGN KEY REFERENCES dw.dim_asset(asset_id),
  start_dt       DATETIME2(0)  NOT NULL,
  end_dt         DATETIME2(0)  NOT NULL,
  duration_hours DECIMAL(8,2)  NULL,
  reason_code    VARCHAR(20)   NULL,
  reason_desc    VARCHAR(200)  NULL,
  is_planned     CHAR(1)       NOT NULL CHECK (is_planned IN ('Y','N'))
);

-- Staging (raw)
--Raw version of production data
CREATE TABLE dw.stg_production_hourly (
  load_id       INT IDENTITY(1,1) PRIMARY KEY,
  asset_name    VARCHAR(100) NULL,
  prod_ts       VARCHAR(19)  NULL,  -- 'YYYY-MM-DD HH:MM:SS'
  oil_bbl       VARCHAR(50)  NULL,
  gas_mcf       VARCHAR(50)  NULL,
  water_bbl     VARCHAR(50)  NULL,
  source_system VARCHAR(30)  NULL,
  load_dtm      DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);
--Raw downtime records before validation
CREATE TABLE dw.stg_downtime_events (
  load_id      INT IDENTITY(1,1) PRIMARY KEY,
  asset_name   VARCHAR(100) NULL,
  start_ts     VARCHAR(19)  NULL,
  end_ts       VARCHAR(19)  NULL,
  reason_code  VARCHAR(20)  NULL,
  reason_desc  VARCHAR(200) NULL,
  is_planned   VARCHAR(5)   NULL,
  load_dtm     DATETIME2(0) NOT NULL DEFAULT SYSDATETIME()
);

-- Error Log(Stores rejected records)
CREATE TABLE dw.etl_error_log (
  err_id       INT IDENTITY(1,1) PRIMARY KEY,
  process_name VARCHAR(50)   NOT NULL,
  record_key   VARCHAR(200)  NULL,
  err_msg      VARCHAR(4000) NOT NULL,
  row_payload  VARCHAR(4000) NULL,
  err_dtm      DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME()
);

-- KPI table
CREATE TABLE dw.kpi_daily_asset (
  date_key       INT           NOT NULL FOREIGN KEY REFERENCES dw.dim_date(date_key),
  asset_id       INT           NOT NULL FOREIGN KEY REFERENCES dw.dim_asset(asset_id),
  oil_bbl_day    DECIMAL(14,2) NULL,
  gas_mcf_day    DECIMAL(14,2) NULL,
  water_bbl_day  DECIMAL(14,2) NULL,
  downtime_hours DECIMAL(10,2) NULL,
  uptime_pct     DECIMAL(6,2)  NULL,
  refresh_dtm    DATETIME2(0)  NOT NULL DEFAULT SYSDATETIME(),
  CONSTRAINT pk_kpi_daily PRIMARY KEY (date_key, asset_id)
);

-- Indexes for faster search
CREATE INDEX prod_date_asset ON dw.fact_production_hourly(date_key, asset_id);
CREATE INDEX dt_asset_start  ON dw.fact_downtime_events(asset_id, start_dt);


--Inserting Data
/*Simulating hourly SCADA production ingestion into staging tables with text-based fields to mimic 
raw operational feeds. 
The ETL layer then handles conversion, validation, and loading into structured fact tables*/

DECLARE @dt DATETIME2 = '2026-01-01 00:00:00';
DECLARE @i INT = 0;

WHILE @i < 72
BEGIN
    -- WELL-KARR-01
    INSERT INTO dw.stg_production_hourly
    (asset_name, prod_ts, oil_bbl, gas_mcf, water_bbl, source_system)
    VALUES
    ('WELL-KARR-01',
     FORMAT(@dt,'yyyy-MM-dd HH:mm:ss'),
     CAST(8.2 + (RAND()*0.5) AS VARCHAR),
     CAST(45 + (RAND()*2) AS VARCHAR),
     CAST(2.1 AS VARCHAR),
     'SCADA_SIM');

    -- WELL-KARR-02 (ramping)
    INSERT INTO dw.stg_production_hourly
    VALUES
    ('WELL-KARR-02',
     FORMAT(@dt,'yyyy-MM-dd HH:mm:ss'),
     CAST(4 + (@i*0.03) AS VARCHAR),
     CAST(28 + (@i*0.10) AS VARCHAR),
     CAST(1.6 AS VARCHAR),
     'SCADA_SIM',
     SYSDATETIME());

    -- COMP-FOOT-01 (gas only)
    INSERT INTO dw.stg_production_hourly
    VALUES
    ('COMP-FOOT-01',
     FORMAT(@dt,'yyyy-MM-dd HH:mm:ss'),
     '0',
     CAST(CASE WHEN DATEPART(HOUR,@dt) BETWEEN 7 AND 19 THEN 120 ELSE 85 END AS VARCHAR),
     '0',
     'DCS_SIM',
     SYSDATETIME());

    SET @dt = DATEADD(HOUR,1,@dt);
    SET @i = @i + 1;
END

--Inserting Downtime Events
INSERT INTO dw.stg_downtime_events
(asset_name,start_ts,end_ts,reason_code,reason_desc,is_planned)
VALUES
('WELL-KARR-01','2026-01-02 03:00:00','2026-01-02 07:00:00','MECH_FAIL','Pump maintenance','N'),

('WELL-KARR-02','2026-01-03 11:00:00','2026-01-03 18:00:00','FLOWLINE','Flowline blockage','N'),

('COMP-FOOT-01','2026-01-01 14:00:00','2026-01-01 16:00:00','TRIP','Compressor vibration alarm','N');


SELECT COUNT(*) AS stg_prod FROM dw.stg_production_hourly;
SELECT COUNT(*) AS fact_prod FROM dw.fact_production_hourly;


