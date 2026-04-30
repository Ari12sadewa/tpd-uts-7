"""
GoFood ETL DAG - Apache Airflow
Orchestrates: Generate Data → Setup DB → PySpark ETL → Validate DWH

Cara pakai:
  1. Copy file ini ke $AIRFLOW_HOME/dags/
  2. Copy seluruh folder gofood_sim ke path yang sesuai (lihat BASE_DIR)
  3. Airflow scheduler akan mendeteksi DAG ini otomatis
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import subprocess, sys, os

# ─── Konfigurasi ──────────────────────────────────────────────────────────────
BASE_DIR = "/opt/airflow/gofood_sim"   # sesuaikan path ini

default_args = {
    "owner": "data_engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# ─── DAG Definition ───────────────────────────────────────────────────────────
with DAG(
    dag_id="gofood_etl_pipeline",
    description="GoFood OLTP → Multi-DB → ETL → DWH pipeline",
    schedule_interval="0 1 * * *",          # Jalan tiap hari jam 01:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["gofood", "etl", "data-engineering"],
) as dag:

    # ── Task 1: Generate dummy data CSV ───────────────────────────────────────
    generate_data = BashOperator(
        task_id="generate_dummy_data",
        bash_command=f"cd {BASE_DIR} && python 1_generate_dummy_data.py",
        doc="Generate dummy OLTP data CSV untuk semua tabel GoFood",
    )

    # ── Task 2: Setup multi-database (SQLite) ─────────────────────────────────
    setup_databases = BashOperator(
        task_id="setup_databases",
        bash_command=f"cd {BASE_DIR} && python 2_setup_databases.py",
        doc="Buat schema dan insert data ke DB1 (user_driver) dan DB2 (merchant_order)",
    )

    # ── Task 3: Validasi data di OLTP sebelum ETL ─────────────────────────────
    def validate_oltp(**context):
        import sqlite3, os
        db1 = os.path.join(BASE_DIR, "databases/user_driver.db")
        db2 = os.path.join(BASE_DIR, "databases/merchant_order.db")

        checks = {
            db1: {"users": 3000, "drivers": 1400},
            db2: {"orders": 10000, "merchants": 700, "products": 3000}
        }

        for db_path, tables in checks.items():
            conn = sqlite3.connect(db_path)
            for table, min_rows in tables.items():
                count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                if count < min_rows:
                    raise ValueError(f"[FAIL] {table} only has {count} rows, expected >= {min_rows}")
                print(f"  ✓ {table}: {count:,} rows")
            conn.close()
        print("✅ OLTP validation passed!")

    validate_oltp_task = PythonOperator(
        task_id="validate_oltp_data",
        python_callable=validate_oltp,
        doc="Validasi bahwa data OLTP sudah cukup sebelum ETL",
    )

    # ── Task 4: Jalankan PySpark ETL ──────────────────────────────────────────
    run_etl = BashOperator(
        task_id="run_pyspark_etl",
        bash_command=f"cd {BASE_DIR} && spark-submit 3_etl_pyspark.py",
        doc="PySpark ETL: extract dari 2 DB → transform → load ke DWH star schema",
    )

    # ── Task 5: Validasi DWH setelah ETL ─────────────────────────────────────
    def validate_dwh(**context):
        import sqlite3, os
        dwh = os.path.join(BASE_DIR, "databases/dwh_gofood.db")
        conn = sqlite3.connect(dwh)

        required_tables = ["dim_date", "dim_user", "dim_driver",
                           "dim_merchant", "dim_product",
                           "fact_orders", "fact_order_items"]

        for table in required_tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                raise ValueError(f"[FAIL] DWH table {table} is empty!")
            print(f"  ✓ {table}: {count:,} rows")

        # Cek fact_orders punya data yang valid
        delivered = conn.execute(
            "SELECT COUNT(*) FROM fact_orders WHERE order_status='delivered'"
        ).fetchone()[0]
        print(f"  ✓ delivered orders: {delivered:,}")

        conn.close()
        print("✅ DWH validation passed!")

    validate_dwh_task = PythonOperator(
        task_id="validate_dwh",
        python_callable=validate_dwh,
        doc="Validasi DWH: pastikan semua tabel terisi setelah ETL",
    )

    # ── Task 6: Generate OLAP summary report ──────────────────────────────────
    def generate_olap_report(**context):
        import sqlite3, os
        dwh = os.path.join(BASE_DIR, "databases/dwh_gofood.db")
        conn = sqlite3.connect(dwh)

        queries = {
            "top_merchants_by_revenue": """
                SELECT dm.merchant_name, SUM(fo.total_amount) AS revenue
                FROM fact_orders fo JOIN dim_merchant dm ON fo.merchant_id=dm.merchant_id
                WHERE fo.order_status='delivered' GROUP BY dm.merchant_id
                ORDER BY revenue DESC LIMIT 10
            """,
            "weekday_vs_weekend": """
                SELECT CASE WHEN dd.is_weekend=1 THEN 'Weekend' ELSE 'Weekday' END as day_type,
                       COUNT(*) as orders
                FROM fact_orders fo JOIN dim_date dd ON fo.date_id=dd.date_id
                WHERE fo.order_status='delivered' GROUP BY dd.is_weekend
            """,
            "peak_order_hours": """
                SELECT order_hour, COUNT(*) as orders
                FROM fact_orders WHERE order_status='delivered'
                GROUP BY order_hour ORDER BY orders DESC LIMIT 5
            """
        }

        report_path = os.path.join(BASE_DIR, "olap_report.txt")
        with open(report_path, "w") as f:
            f.write(f"GoFood OLAP Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*60 + "\n\n")
            for title, sql in queries.items():
                import pandas as pd
                df = pd.read_sql_query(sql, conn)
                f.write(f"[{title.upper()}]\n")
                f.write(df.to_string(index=False))
                f.write("\n\n")

        conn.close()
        print(f"✅ OLAP report saved: {report_path}")

    olap_report_task = PythonOperator(
        task_id="generate_olap_report",
        python_callable=generate_olap_report,
        doc="Generate summary OLAP queries dari DWH",
    )

    # ─── Task Dependencies ────────────────────────────────────────────────────
    ##generate_data >> setup_databases >> validate_oltp_task >> run_etl >> validate_dwh_task >> olap_report_task