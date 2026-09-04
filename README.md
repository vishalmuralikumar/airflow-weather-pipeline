# Weather Data Engineering Pipeline

An end-to-end data engineering project that collects weather data from the Open-Meteo API, processes and validates the data using Python, orchestrates the workflow with Apache Airflow, stores the data in PostgreSQL, and prepares it for analytics and Power BI visualization.

## Project Overview

This project demonstrates how a real-world data pipeline can be designed and automated using modern data engineering tools.

The pipeline collects hourly weather data for Paris, France and processes it through multiple stages:

Open-Meteo API
        ↓
Extract
        ↓
Transform
        ↓
Validate
        ↓
PostgreSQL
        ↓
SQL Analysis
        ↓
Power BI

## Key Objectives

- Extract real-time weather forecast data from a REST API
- Build an automated ETL pipeline using Apache Airflow
- Transform raw JSON data into structured tabular data
- Perform data quality and validation checks
- Store processed data in PostgreSQL
- Implement database-level constraints and duplicate protection
- Use Airflow XCom for task communication
- Implement retries and failure handling
- Add GitHub Actions CI workflow
- Perform SQL-based analytical queries
- Build a Power BI dashboard for data visualization

## Technologies Used

- Python
- Apache Airflow 3.3.1
- PostgreSQL
- Pandas
- REST API
- SQL
- Docker
- Docker Compose
- Git
- GitHub
- GitHub Actions
- Power BI
- UV

## Data Source

Weather data is collected from the Open-Meteo API.

Location:

- City: Paris
- Country: France
- Latitude: 48.8566
- Longitude: 2.3522
- Timezone: Europe/Paris

Weather attributes:

- Temperature
- Relative Humidity
- Wind Speed

The API does not require an API key for this project.

## Pipeline Workflow

### 1. Extract

The extraction script sends a request to the Open-Meteo API and saves the raw JSON response.

File:

`scripts/extract_weather.py`

Output:

`data/paris_weather.json`

### 2. Transform

The transformation step converts the JSON response into a Pandas DataFrame.

Transformations include:

- Converting timestamps
- Renaming columns
- Adding city and country information
- Selecting required columns
- Creating a clean CSV dataset

File:

`scripts/transform_weather.py`

Output:

`data/paris_weather_clean.csv`

### 3. Validate

The validation step checks the quality of the transformed dataset.

Validation checks include:

- Total row count
- Total column count
- Missing values
- Duplicate timestamps
- Temperature range
- Humidity range
- Wind speed range

File:

`scripts/validate_weather.py`

### 4. Load

The validated data is loaded into PostgreSQL using Airflow's `PostgresHook`.

The database table is:

`weather_data`

The table contains:

- timestamp
- city
- country
- temperature_c
- humidity_percent
- wind_speed_kmh

A unique constraint on `(city, timestamp)` prevents duplicate weather records.

### 5. Verify

After loading the data, the pipeline verifies the number of records stored in PostgreSQL.

This provides a final database-level validation step.

## Apache Airflow DAG

DAG name:

`paris_weather_pipeline`

The DAG contains the following tasks:

1. `extract_weather`
2. `transform_weather`
3. `validate_weather`
4. `load_weather`
5. `push_weather_status`
6. `pull_weather_status`
7. `verify_database`

Task dependency:

`extract_weather → transform_weather → validate_weather → load_weather → push_weather_status → pull_weather_status → verify_database`

The DAG is scheduled to run daily.

## Airflow Features Implemented

This project demonstrates several important Apache Airflow concepts:

- DAG creation
- Task dependencies
- BashOperator
- PythonOperator
- PostgreSQL Hook
- Airflow Connections
- XCom
- Retries
- Retry delay
- Logging
- Task failure handling
- Database verification
- Scheduling
- Catchup configuration
- DAG tags

## XCom Implementation

Airflow XCom is used to pass small pieces of information between tasks.

In this project, the pipeline pushes a completion status:

`Weather pipeline completed successfully`

The next task pulls this value and logs it.

XCom is used for metadata and small values rather than transferring large datasets.

## PostgreSQL

PostgreSQL is used as the final data storage layer.

The database table contains structured weather observations and uses database constraints to improve data quality.

Example constraint:

`UNIQUE (city, timestamp)`

This prevents duplicate records for the same city and timestamp.

## SQL Analysis

The stored weather data was also analyzed using SQL.

SQL concepts implemented include:

- GROUP BY
- Aggregate functions
- CTE
- CASE WHEN
- RANK
- ROW_NUMBER
- Window functions
- OVER
- LAG
- LEAD
- Moving averages
- Conditional aggregation
- FILTER
- Data quality checks

These queries were used to analyze weather trends and identify temperature patterns.

## Docker Architecture

The Airflow environment runs using Docker Compose.

Main services include:

- Airflow API Server
- Airflow Scheduler
- Airflow DAG Processor
- Airflow Worker
- Airflow Triggerer
- PostgreSQL
- Redis

Docker provides a reproducible environment for running the pipeline.


## Author name
Vishal

## Project Structure

```text
airflow-weather-pipeline/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── config/
│
├── dags/
│   └── paris_weather_pipeline.py
│
├── scripts/
│   ├── extract_weather.py
│   ├── transform_weather.py
│   └── validate_weather.py
│
├── src/
│   └── airflow_weather_pipeline/
│
├── data/
│
├── logs/
│
├── plugins/
│
├── docker-compose.yaml
├── pyproject.toml
├── uv.lock
├── .gitignore
├── README.md
└── test_api.py
