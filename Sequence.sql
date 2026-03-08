USE EnergyDataWarehouse;



-- Create sequence for downtime event ids (only if it doesn't exist)
IF NOT EXISTS (SELECT 1 FROM sys.sequences WHERE name = 'seq_downtime_event_id' AND SCHEMA_NAME(schema_id) = 'dw')
BEGIN
    CREATE SEQUENCE dw.seq_downtime_event_id
      AS INT
      START WITH 1
      INCREMENT BY 1;
END

-- Step Below creates a reusable procedure to log rejected ETL records into an error table.
-- Instead of stopping the ETL when bad data appears, 
-- we capture the error details (process name, message, row data) for traceability and data quality monitoring
CREATE OR ALTER PROCEDURE dw.sp_log_etl_error
  @process_name VARCHAR(50),
  @record_key   VARCHAR(200) = NULL,
  @err_msg      VARCHAR(4000),
  @row_payload  VARCHAR(4000) = NULL
AS
BEGIN
  SET NOCOUNT ON;

  INSERT INTO dw.etl_error_log(process_name, record_key, err_msg, row_payload)
  VALUES (@process_name, @record_key, @err_msg, @row_payload);
END
GO