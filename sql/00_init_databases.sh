#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" <<-EOSQL

CREATE DATABASE airflow_db;
CREATE DATABASE source_db;
CREATE DATABASE warehouse_db;

EOSQL

echo "Created airflow_db, source_db, warehouse_db"