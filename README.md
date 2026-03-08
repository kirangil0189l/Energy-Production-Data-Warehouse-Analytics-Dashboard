# Energy-Production-Data-Warehouse-Analytics-Dashboard


##Project Overview
This project demonstrates the design and implementation of a data analytics solution for monitoring upstream energy production and asset performance. 
The system simulates production data from oil and gas assets, processes the data through an ETL pipeline, stores it in a SQL Server data warehouse, and visualizes key operational metrics using Power BI. The dashboard provides insights into daily production trends, asset uptime, and downtime causes to support operational decision-making.

##Architecture
Data Source (CSV / Raw Data)
        ↓
Staging Tables
        ↓
ETL Processing (SQL Stored Procedures)
        ↓
Data Warehouse (Star Schema)
        ↓
KPI Aggregation Views
        ↓
Power BI Dashboard

## Data Warehouse Design

The warehouse follows a star schema design.

Dimension Tables
- dim_asset
- dim_field
- dim_date

Fact Tables
- fact_production_hourly
- fact_downtime_events

KPI Table
- kpi_daily_asset

## ETL Pipeline

The ETL process is implemented using SQL stored procedures.

Steps include:

1. Load raw production and downtime data into staging tables
2. Validate and clean data
3. Map assets to dimension tables
4. Convert timestamps and numeric fields
5. Log invalid records in an ETL error log
6. Load clean records into fact tables
7. Generate daily KPIs for reporting

## Power BI Dashboard

The Power BI dashboard provides operational insights including:

- Total Oil Production
- Total Gas Production
- Total Water Production
- Average Asset Uptime
- Daily Oil Production Trend by Asset
- Downtime Hours by Reason
- Daily Asset Downtime Trend

Interactive filters allow users to analyze performance by asset.

## Technologies Used

- SQL Server
- Docker
- T-SQL
- Power BI
- Data Warehousing (Star Schema)
- ETL Pipeline Design

## Project Structure

energy-production-data-warehouse
│
├── sql
│   ├── DatamartTables.sql
│   ├── Sequence.sql
│   ├── ETLProcess.sql
│   ├── KPI_refresh.sql
│   └── Views.sql
│
├── powerbi
│   └── EnergyProductionDashboard.pbix
│
├── images
│   └── dashboard.png
│
└── README.md


## Key Skills Demonstrated

- Data warehouse design
- ETL pipeline development
- SQL data transformation
- KPI aggregation
- Business intelligence reporting
- Interactive dashboard design

## Project Significance

Energy companies rely heavily on production monitoring dashboards to track well performance and asset reliability. 

This project demonstrates how production and downtime data can be transformed into actionable insights using modern data engineering and analytics tools.
