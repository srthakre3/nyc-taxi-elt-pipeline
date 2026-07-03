"""
NYC TLC Trip Data Ingestion
Downloads yellow taxi trip parquet files and loads raw data into PostgreSQL.
"""

import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────
DB_CONN = os.getenv("DB_CONN", "postgresql://airflow:airflow@localhost:5432/airflow")
RAW_SCHEMA = "raw"
RAW_TABLE = "yellow_trips"

# NYC TLC parquet files — update year/month as needed
DATASET_URLS = [
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
]

# ── Helpers ─────────────────────────────────────────────────────────────────

def download_parquet(url: str) -> pd.DataFrame:
    """Download a parquet file from URL and return as DataFrame."""
    print(f"Downloading: {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()
    tmp_path = "/tmp/trips.parquet"
    with open(tmp_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return pd.read_parquet(tmp_path)


def load_to_postgres(df: pd.DataFrame, engine, schema: str, table: str, if_exists: str = "append") -> None:
    """Load DataFrame into PostgreSQL."""
    print(f"Loading {len(df):,} rows into {schema}.{table}...")
    df.to_sql(table, engine, schema=schema, if_exists=if_exists, index=False, chunksize=10_000)
    print("Done.")


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    engine = create_engine(DB_CONN)

    # Create raw schema if not exists
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))

    # Drop table with CASCADE to remove any dependent dbt views
    # dbt_run will recreate them after ingestion
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {RAW_SCHEMA}.{RAW_TABLE} CASCADE"))
        print(f"Dropped {RAW_SCHEMA}.{RAW_TABLE} (CASCADE)")

    for url in DATASET_URLS:
        df = download_parquet(url)

        # Basic column selection — keep relevant fields
        cols = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance",
            "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "tip_amount", "total_amount"
        ]
        df = df[[c for c in cols if c in df.columns]]

        load_to_postgres(
            df, engine,
            schema=RAW_SCHEMA,
            table=RAW_TABLE,
            if_exists="append"
        )

    print(f"Ingestion complete.")


if __name__ == "__main__":
    main()
