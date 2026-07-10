"""
demo.py — NYC Taxi ELT Pipeline Demo
======================================
Runs the full pipeline locally using Docker (PostgreSQL) + dbt.
No Airflow needed for the demo — we run each step manually in sequence.

Prerequisites:
    - Docker Desktop running
    - pip install -r requirements.txt
    - pip install dbt-postgres

Run:
    python demo.py
"""

import subprocess
import sys
import time
import os

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONN = "postgresql://airflow:airflow@localhost:5432/airflow"
DBT_DIR = os.path.join(os.path.dirname(__file__), "dbt_project")


def run(cmd: str, cwd: str = None, exit_on_fail: bool = True) -> bool:
    """Run a shell command, print output live, return success."""
    print(f"\n$ {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd)
    if result.returncode != 0 and exit_on_fail:
        print(f"\n❌ Command failed: {cmd}")
        sys.exit(1)
    return result.returncode == 0


def wait_for_postgres(retries: int = 15, delay: int = 3):
    """Poll until PostgreSQL is accepting connections."""
    print("\n⏳ Waiting for PostgreSQL to be ready...")
    for i in range(retries):
        result = subprocess.run(
            "docker exec nyc-taxi-elt-pipeline-postgres-1 pg_isready -U airflow",
            shell=True, capture_output=True
        )
        if result.returncode == 0:
            print("✅ PostgreSQL is ready.")
            return
        print(f"   Not ready yet ({i+1}/{retries})... retrying in {delay}s")
        time.sleep(delay)
    print("❌ PostgreSQL did not start in time.")
    sys.exit(1)


def main():
    print("\n" + "="*55)
    print("  NYC Taxi ELT Pipeline — Demo")
    print("="*55)
    print("""
Pipeline:
  NYC TLC API → PostgreSQL (raw) → dbt staging
  → dbt intermediate → dbt marts (fct_trips, dim_date)
""")

    # ── Step 1: Start PostgreSQL via Docker ───────────────────────────────────
    print("\n📦 STEP 1: Start PostgreSQL with Docker")
    print("-"*40)
    print("Docker Compose starts a PostgreSQL container on port 5432.")
    print("This is the same database Airflow would use in production.\n")
    run("docker compose up -d postgres")
    wait_for_postgres()

    # ── Step 2: Ingest raw data ───────────────────────────────────────────────
    print("\n📥 STEP 2: Ingest NYC TLC Trip Data")
    print("-"*40)
    print("Downloads Jan–Mar 2024 yellow taxi parquet files from NYC TLC.")
    print("Loads raw data into PostgreSQL schema: raw.yellow_trips\n")
    run(f"python ingestion/download_trips.py", cwd=os.path.dirname(__file__))

    # ── Step 3: dbt deps ──────────────────────────────────────────────────────
    print("\n📦 STEP 3: Install dbt packages")
    print("-"*40)
    print("dbt_utils provides helper macros like generate_surrogate_key and date_spine.\n")
    run("dbt deps", cwd=DBT_DIR)

    # ── Step 4: dbt run ───────────────────────────────────────────────────────
    print("\n🔄 STEP 4: Run dbt models")
    print("-"*40)
    print("""dbt runs models in dependency order:
  1. stg_trips       (staging view)   — clean & type-cast raw data
  2. int_trips_enriched (intermediate view) — add derived metrics
  3. fct_trips       (mart table)     — final fact table
  4. dim_date        (mart table)     — date dimension
""")
    run("dbt run --profiles-dir .", cwd=DBT_DIR)

    # ── Step 5: dbt test ──────────────────────────────────────────────────────
    print("\n✅ STEP 5: Run dbt tests")
    print("-"*40)
    print("""Tests defined in schema.yml:
  - not_null on all primary keys and critical fields
  - unique on trip_id and date_id
  - accepted_values on payment_type_id
  - relationships: fct_trips.date_id → dim_date.date_id
""")
    run("dbt test --profiles-dir .", cwd=DBT_DIR)

    # ── Step 6: Quick summary query ───────────────────────────────────────────
    print("\n📊 STEP 6: Sample query — trips by day of week")
    print("-"*40)
    print("Running SQL against the mart tables to show the final output...\n")

    query = """
    SELECT
        d.day_name,
        COUNT(*)            AS total_trips,
        ROUND(AVG(f.trip_duration_minutes)::numeric, 1) AS avg_duration_min,
        ROUND(AVG(f.fare_amount)::numeric, 2)           AS avg_fare
    FROM marts.fct_trips f
    JOIN marts.dim_date d ON f.date_id = d.date_id
    GROUP BY d.day_name, d.day_of_week
    ORDER BY d.day_of_week;
    """

    run(
        f'docker exec nyc-taxi-elt-pipeline-postgres-1 psql -U airflow -d airflow -c "{query.strip()}"'
    )

    # ── Done ──────────────────────────────────────────────────────────────────
    print("\n" + "="*55)
    print("  ✅ Pipeline complete!")
    print("="*55)
    print("""
What just ran:
  1. PostgreSQL started in Docker
  2. Raw NYC taxi data downloaded and loaded (raw.yellow_trips)
  3. dbt staging model created (staging.stg_trips)
  4. dbt intermediate model created (intermediate.int_trips_enriched)
  5. dbt mart tables created (marts.fct_trips, marts.dim_date)
  6. dbt tests passed — data quality validated
  7. Sample query ran against the final mart

To explore the data:
  docker exec -it nyc-taxi-elt-pipeline-postgres-1 psql -U airflow -d airflow

To stop Docker:
  docker compose down
""")


if __name__ == "__main__":
    main()
