.PHONY: install db init full incremental validate sql-demo test demo clean-data
install:      ; pip install -r requirements.txt
db:           ; docker compose up -d
init:         ; python -m etl.pipeline init --reset
full:         ; python -m etl.pipeline run data/raw_orders_full.xml --mode full --batch-size 100
incremental:  ; python -m etl.pipeline run data/raw_orders_incremental.xml --mode incremental --batch-size 50
validate:     ; python -m etl.pipeline validate --expected data/expected_incremental.json
sql-demo:     ; ./scripts/run_sql_demo.sh
test:         ; ETL_TEST_DATABASE_URL=$${ETL_TEST_DATABASE_URL:?point at a disposable db} pytest -v
demo:         ; ./scripts/run_demo.sh          # regenerates everything under evidence/
clean-data:   ; python scripts/generate_data.py  # deterministic: same bytes every time
