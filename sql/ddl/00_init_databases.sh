#!/usr/bin/env bash
# =========================================================
# 00_init_databases.sh
# Runs once, automatically, when the Postgres container's
# data directory is first initialized (mounted into
# /docker-entrypoint-initdb.d/ by docker-compose).
#
# Creates two separate databases with separate users so the
# Airflow metadata store and the Analytics Data Warehouse
# never share credentials (principle of least privilege).
# All values come from environment variables (.env) —
# nothing is hard-coded.
# =========================================================
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    -- Analytics warehouse database already exists as POSTGRES_DB/POSTGRES_USER
    -- (created automatically by the postgres image using those env vars).

    -- Airflow metadata role + database (separate credentials)
    CREATE USER "${AIRFLOW_DB_USER}" WITH PASSWORD '${AIRFLOW_DB_PASSWORD}';
    CREATE DATABASE "${AIRFLOW_DB_NAME}" OWNER "${AIRFLOW_DB_USER}";
    GRANT ALL PRIVILEGES ON DATABASE "${AIRFLOW_DB_NAME}" TO "${AIRFLOW_DB_USER}";
EOSQL

echo "00_init_databases.sh: airflow metadata database and user created."
