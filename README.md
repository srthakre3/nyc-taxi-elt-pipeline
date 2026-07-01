# NYC Taxi ELT Pipeline — dbt & Apache Airflow

End-to-end ELT pipeline ingesting NYC TLC trip data into PostgreSQL, transforming with dbt (staging → intermediate → dimensional mart), orchestrated with Apache Airflow, and validated with dbt tests + GitHub Actions CI.

## Architecture

```
NYC TLC API
    │
    ▼
Python Ingestion ──► PostgreSQL (raw)
                          │
                          ▼
                     dbt Staging
                     (clean, type-cast)
                          │
                          ▼
                   dbt Intermediate
                   (business logic)
                          │
                          ▼
                     dbt Marts
               (fct_trips, dim_date)
                          │
                          ▼
                  BI / Analytics
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow |
| Transformation | dbt |
| Warehouse | PostgreSQL (local) |
| Ingestion | Python (requests, pandas) |
| Data Quality | dbt tests |
| CI/CD | GitHub Actions |

## Project Structure

```
nyc-taxi-elt-pipeline/
├── ingestion/
│   └── download_trips.py       # Download & load raw data to PostgreSQL
├── dags/
│   └── nyc_taxi_pipeline.py    # Airflow DAG
├── dbt_project/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/            # stg_trips — clean raw data
│   │   ├── intermediate/       # int_trips_cleaned — business logic
│   │   └── marts/              # fct_trips, dim_date — dimensional model
│   └── tests/                  # Custom dbt tests
├── .github/workflows/
│   └── dbt_ci.yml              # CI: run dbt build + tests on push
├── docker-compose.yml          # Airflow + PostgreSQL local setup
└── requirements.txt
```

## Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- dbt-postgres

### Run locally

```bash
# 1. Start Airflow + PostgreSQL
docker-compose up -d

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Download and load raw data
python ingestion/download_trips.py

# 4. Run dbt transformations
cd dbt_project
dbt deps
dbt run
dbt test

# 5. Trigger Airflow DAG
# Open http://localhost:8080 — enable nyc_taxi_pipeline DAG
```

## Data Model

```
fct_trips
├── trip_id (PK)
├── date_id (FK → dim_date)
├── pickup_datetime
├── dropoff_datetime
├── trip_duration_minutes
├── trip_distance_miles
├── fare_amount
├── tip_amount
└── total_amount

dim_date
├── date_id (PK)
├── date
├── year, month, day
├── day_of_week
└── is_weekend
```

## Data Quality

dbt tests cover:
- `not_null` on all primary keys and critical fields
- `unique` on trip_id and date_id
- `accepted_values` on payment_type
- Custom test: trip duration must be > 0

## Status

- [ ] Ingestion script — download NYC TLC parquet files
- [ ] PostgreSQL raw schema
- [ ] dbt staging models
- [ ] dbt intermediate models
- [ ] dbt mart models (fct_trips, dim_date)
- [ ] Airflow DAG
- [ ] dbt tests
- [ ] GitHub Actions CI
