USE EnergyDataWarehouse;
GO

EXEC dw.sp_load_production_hourly;
EXEC dw.sp_load_downtime_events;
GO