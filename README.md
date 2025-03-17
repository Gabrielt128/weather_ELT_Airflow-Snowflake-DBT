# weather_ETL_Airflow-Snowflake-DBT

## Setups

### 1. Connect from Airflow to Snowflake

The key is Schema, Login, Password, Account, Warehouse/Database, Region, Role and _Host_ all need to be specified in the connection we created in Airflow!

While the official docs said that all other than Login, Password are optional, which is wrong!

### 2. Connect from DBT to Snowflake

## Implementation details

┌──────────────────────────────────────────────────────────────────────────────────────────┐
│ Docker Container │
│ ┌──────────────────────────────────────────────────────────────────────────────────┐ │
│ │ Airflow DAG (Orchestration) │ │
│ │ ┌──────────────────┐ ┌────────────────────┐ ┌──────────────────┐ │ │
│ │ │ Open-Meteo API │ ───► │ Extract & Load │ ───► │ Snowflake Table │ │ │
│ │ │ (Data Source) │ │ (Airflow + Snowflake) │ │ (Raw Data) │ │ │
│ │ └──────────────────┘ └────────────────────┘ └──────────────────┘ │ │
│ │ │ │
│ │ ┌──────────────────────┐ │ │
│ │ │ dbt Transform │ │ │
│ │ │ (SQL Modeling) │ │ │
│ │ └──────────────────────┘ │ │
│ └──────────────────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────────────────────┘
