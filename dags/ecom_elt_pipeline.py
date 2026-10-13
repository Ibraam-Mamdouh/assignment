import subprocess
from datetime import datetime
from pathlib import Path

import duckdb
from airflow.decorators import dag, task

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "raw_orders.csv"
DBT_DIR = BASE_DIR / "dbt_project"
DB_PATH = "/home/vscode/airflow/dev.duckdb"
DBT_BIN = "/home/vscode/dbt_venv/bin/dbt"


@dag(
    dag_id="ecom_elt_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "dbt", "duckdb"],
)
def ecom_elt_pipeline():

    @task
    def load_raw_csv_to_duckdb():
        con = duckdb.connect(DB_PATH)
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            f"CREATE OR REPLACE TABLE raw.orders AS "
            f"SELECT * FROM read_csv_auto('{CSV_PATH}')"
        )
        count = con.execute("SELECT COUNT(*) FROM raw.orders").fetchone()[0]
        con.close()  # مهم: نقفل الاتصال عشان dbt يعرف يفتح الملف
        print(f"Loaded {count} rows into raw.orders")

    @task
    def run_dbt_models():
        result = subprocess.run(
            [DBT_BIN, "run",
             "--project-dir", str(DBT_DIR),
             "--profiles-dir", str(DBT_DIR)],
            capture_output=True, text=True,
        )
        print(result.stdout)
        print(result.stderr)
        result.check_returncode()

    @task
    def validate_data_quality():
        con = duckdb.connect(DB_PATH, read_only=True)
        count = con.execute("SELECT COUNT(*) FROM main.fct_daily_revenue").fetchone()[0]
        con.close()
        if count == 0:
            raise ValueError("Data quality check failed: fct_daily_revenue is empty!")
        print(f"Data quality passed: {count} rows")

    load_raw_csv_to_duckdb() >> run_dbt_models() >> validate_data_quality()


ecom_elt_pipeline()