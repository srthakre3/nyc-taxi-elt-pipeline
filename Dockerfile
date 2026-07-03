FROM apache/airflow:2.9.1

# Install build tools as root (needed to compile psycopg2)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user and install Python packages
USER airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.8.7" \
    "dbt-postgres==1.8.2" \
    "pyarrow>=16.0.0" \
    "requests>=2.31.0" \
    "python-dotenv>=1.0.1"
