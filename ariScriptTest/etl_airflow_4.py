from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from io import StringIO

# ==============================
# KONFIGURASI DATABASE
# ==============================
MYSQL_SRC_URL = "mysql+pymysql://root:@192.168.144.1:3306/source_a_uts"
PG_SRC_URL = "postgresql+psycopg2://postgres:12345@192.168.144.1:5432/source_b_uts"
DWH_URL = "mysql+pymysql://root:@192.168.144.1:3306/dwh_uts"

default_args = {
    "owner": "Ari Sadewa",
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def haversine_distance(lat1, lon1, lat2, lon2):
    r = 6371
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    return r * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


# ==============================
# UNIFIED ETL PROCESS
# ==============================


def extract_from_databases(**kwargs):
    engine_mysql = create_engine(MYSQL_SRC_URL)
    engine_pg = create_engine(PG_SRC_URL)

    data = {
        "users": pd.read_sql("SELECT * FROM users", engine_mysql).to_json(),
        "drivers": pd.read_sql("SELECT * FROM drivers", engine_mysql).to_json(),
        "merchants": pd.read_sql("SELECT * FROM merchants", engine_pg).to_json(),
        "products": pd.read_sql("SELECT * FROM products", engine_pg).to_json(),
        "orders": pd.read_sql("SELECT * FROM orders", engine_pg).to_json(),
        "order_items": pd.read_sql("SELECT * FROM order_items", engine_pg).to_json(),
    }
    return data


def unified_transformation(**kwargs):
    ti = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="extract_task")

    df_u = pd.read_json(StringIO(raw["users"]))
    df_m = pd.read_json(StringIO(raw["merchants"]))
    df_o = pd.read_json(StringIO(raw["orders"]))
    df_oi = pd.read_json(StringIO(raw["order_items"]))

    # 1. DEDUP & IDENTITY RESOLUTION (Pondasi Konsistensi)
    df_u["phone_clean"] = df_u["phone"].astype(str).str[-10:]
    # Ambil user pertama yang mendaftar sebagai master
    df_u_master = (
        df_u.sort_values("created_at")
        .drop_duplicates("phone_clean", keep="first")
        .copy()
    )

    # Mapping ID: User yang duplikat akan diarahkan ke ID master
    user_id_map = dict(
        zip(
            df_u["user_id"],
            df_u["phone_clean"].map(
                dict(zip(df_u_master["phone_clean"], df_u_master["user_id"]))
            ),
        )
    )
    df_o["user_id"] = df_o["user_id"].map(user_id_map)

    # 2. DIM_USER (Gabungan Basic & Advanced)
    df_u_master["date_of_birth"] = pd.to_datetime(df_u_master["date_of_birth"])
    current_date = pd.to_datetime(datetime.now().date())
    df_u_master["age"] = (
        (current_date - df_u_master["date_of_birth"]).dt.days / 365.25
    ).astype(int)
    df_u_master["age_group"] = np.where(
        df_u_master["age"] < 25,
        "17-24",
        np.where(df_u_master["age"] < 35, "25-34", "35+"),
    )

    # RFM Logic
    df_o["order_time"] = pd.to_datetime(df_o["order_time"])
    snapshot = df_o["order_time"].max() + timedelta(days=1)
    rfm = (
        df_o[df_o["status"] == "delivered"]
        .groupby("user_id")
        .agg(
            {
                "order_time": lambda x: (snapshot - x.max()).days,
                "order_id": "count",
                "total_amount": "sum",
            }
        )
        .rename(
            columns={
                "order_time": "recency",
                "order_id": "frequency",
                "total_amount": "monetary",
            }
        )
    )

    # Simple Segment
    rfm["segment"] = np.where(
        rfm["frequency"] > rfm["frequency"].median(), "Loyal", "Regular"
    )
    dim_user = df_u_master.merge(rfm[["segment"]], on="user_id", how="left").fillna(
        {"segment": "New"}
    )
    dim_user = dim_user.rename(
        columns={"full_name": "user_name", "address_area": "user_area"}
    )

    # 3. DIM_DATE
    dim_date = pd.DataFrame({"full_date": df_o["order_time"].dt.date.unique()})
    dim_date["date_id"] = (
        pd.to_datetime(dim_date["full_date"]).dt.strftime("%Y%m%d").astype(int)
    )
    dim_date["is_weekend"] = (
        pd.to_datetime(dim_date["full_date"]).dt.dayofweek.isin([5, 6]).astype(int)
    )

    # 4. DIM_MERCHANT
    dim_merchant = df_m.copy()

    # 5. FACT ORDERS (Consolidated)
    # Kita buat satu tabel fakta besar yang mencakup semua metrik
    df_geo = df_o.merge(df_m[["merchant_id", "lat", "lon"]], on="merchant_id")
    df_geo = df_geo.merge(
        df_u_master[["user_id", "lat", "lon"]], on="user_id", suffixes=("_m", "_u")
    )

    df_o["distance_km"] = haversine_distance(
        df_geo["lat_m"], df_geo["lon_m"], df_geo["lat_u"], df_geo["lon_u"]
    )
    df_o["date_id"] = df_o["order_time"].dt.strftime("%Y%m%d").astype(int)
    df_o["hour"] = df_o["order_time"].dt.hour

    fact_orders = df_o[
        [
            "order_id",
            "user_id",
            "driver_id",
            "merchant_id",
            "date_id",
            "total_amount",
            "delivery_fee",
            "distance_km",
            "hour",
            "status",
        ]
    ]

    return {
        "dim_user": dim_user.to_json(),
        "dim_date": dim_date.to_json(),
        "dim_merchant": dim_merchant.to_json(),
        "fact_orders": fact_orders.to_json(),
    }


def load_to_dwh(**kwargs):
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="unified_transform_task")
    engine = create_engine(DWH_URL)

    for table, json_str in data.items():
        df = pd.read_json(StringIO(json_str))
        # Menggunakan 'replace' untuk dimensi agar selalu update dengan info RFM terbaru
        df.to_sql(table, engine, if_exists="replace", index=False)
        print(f" ✓ {table} synchronized.")


# ==============================
# DAG DEFINITION
# ==============================
with DAG(
    "gofood_unified_etl", default_args=default_args, schedule="@hourly", catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_task", python_callable=extract_from_databases
    )
    transform = PythonOperator(
        task_id="unified_transform_task", python_callable=unified_transformation
    )
    load = PythonOperator(task_id="load_task", python_callable=load_to_dwh)

    extract >> transform >> load
