#!/usr/bin/env bash
# Pure-SQL ETL demo (no Python). Uses its own database so it never touches the Python pipeline's data.
#
#   ./scripts/run_sql_demo.sh                 # full load of raw_orders_full.xml, then incremental load of raw_orders_incremental.xml
#   SQL_DEMO_DB=mydb ./scripts/run_sql_demo.sh
#
# Connection: libpq env vars (PGHOST PGPORT PGUSER PGPASSWORD). The role needs CREATEDB.
set -euo pipefail
cd "$(dirname "$0")/.."
DB="${SQL_DEMO_DB:-etl_sql_demo}"

psql -X -q -d postgres -c "DROP DATABASE IF EXISTS \"$DB\"" -c "CREATE DATABASE \"$DB\""
export PGDATABASE="$DB"
for f in 01_schema 02_reference_data 03_cleaning_functions 04_pipeline_functions 05_analysis_views; do
  psql -X -q -v ON_ERROR_STOP=1 -f "sql/$f.sql"
done
echo "################  LOAD 1: FULL  ################"
SRC_FILE=data/raw_orders_full.xml MODE=full psql -X -q -v ON_ERROR_STOP=1 -f sql/10_demo_sql_etl.sql
echo
echo "################  LOAD 2: INCREMENTAL  ################"
SRC_FILE=data/raw_orders_incremental.xml MODE=incremental psql -X -q -v ON_ERROR_STOP=1 -f sql/10_demo_sql_etl.sql
