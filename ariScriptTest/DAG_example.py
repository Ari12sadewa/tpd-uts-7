"""
GoFood Analytics - Airflow ETL DAG
====================================
Pipeline ETL untuk membangun Data Warehouse dari 3 sumber data:
  - Source A (MySQL)       : users, drivers
  - Source B (PostgreSQL)  : merchants, products, orders, order_items, reviews
  - GEE (Google Earth Engine) : data cuaca per kota (dinamis, menyesuaikan rentang orders)

Tujuan analisis (README.md):
  1. Merchant dengan revenue terbanyak
  2. Perbandingan rata-rata transaksi weekend vs weekday
  3. Proporsi kategori makanan berdasarkan kelompok usia
  4. Persebaran lokasi merchant
  5. Distribusi order berdasarkan jam
  6. Pengaruh kondisi cuaca terhadap jumlah & pola pemesanan

Schema DWH:
  Dimensions : dim_date, dim_user, dim_driver, dim_merchant, dim_product, dim_weather
  Facts      : fact_orders, fact_order_items
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from google.oauth2 import service_account
from io import StringIO
import pandas as pd
import numpy as np
import ee
import json

# ==============================
# KONFIGURASI DATABASE
# ==============================
MYSQL_SRC_URL = "mysql+pymysql://root:@192.168.144.1:3306/source_a_uts"
PG_SRC_URL = "postgresql+psycopg2://postgres:12345@192.168.144.1:5432/source_b_uts"
DWH_URL = "mysql+pymysql://root:@192.168.144.1:3306/dwh_uts"

# ==============================
# KONFIGURASI GEE
# ==============================
GEE_PROJECT = "paradokstesting"
CITY_COORDS = {
    "jakarta": {"name": "DKI Jakarta", "coord": [106.8, -6.2]},
    "surabaya": {"name": "Surabaya", "coord": [112.75, -7.25]},
    "medan": {"name": "Medan", "coord": [98.67, 3.59]},
}

# ==============================
# DEFAULT ARGS
# ==============================
default_args = {
    "owner": "Ari Sadewa",
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1 — EXTRACT
# ══════════════════════════════════════════════════════════════════════════════


def extract_from_databases(**kwargs):
    """
    Ekstrak semua tabel dari Source A (MySQL) dan Source B (PostgreSQL).
    Hasilnya di-push ke XCom sebagai JSON string.
    """
    print("=== [EXTRACT] Memulai ekstraksi dari database ===")
    engine_mysql = create_engine(MYSQL_SRC_URL)
    engine_pg = create_engine(PG_SRC_URL)

    data = {
        # Source A — MySQL
        "users": pd.read_sql("SELECT * FROM users", engine_mysql).to_json(),
        "drivers": pd.read_sql("SELECT * FROM drivers", engine_mysql).to_json(),
        # Source B — PostgreSQL
        "merchants": pd.read_sql("SELECT * FROM merchants", engine_pg).to_json(),
        "products": pd.read_sql("SELECT * FROM products", engine_pg).to_json(),
        "orders": pd.read_sql("SELECT * FROM orders", engine_pg).to_json(orient="records", date_format="iso"),
        "order_items": pd.read_sql("SELECT * FROM order_items", engine_pg).to_json(),
        "reviews": pd.read_sql("SELECT * FROM reviews", engine_pg).to_json(),
    }

    for key, val in data.items():
        df_tmp = pd.read_json(StringIO(val))
        print(f"  ✓ {key:15s}: {len(df_tmp):,} rows")

    print("=== [EXTRACT] Database selesai ===")
    return data


def extract_weather_from_gee(**kwargs):
    """
    Ekstrak data cuaca dari Google Earth Engine (ERA5-Land Hourly).
    Rentang waktu ditentukan dinamis berdasarkan min/max order_time
    di tabel orders, bukan hardcoded tahun.

    Output: JSON string dari DataFrame cuaca (wilayah, waktu, cuaca).
    """
    print("=== [EXTRACT] Memulai ekstraksi cuaca dari GEE ===")
    ti = kwargs["ti"]

    # ── Ambil rentang waktu dari tabel orders ─────────────────────────────────
    raw_orders = ti.xcom_pull(task_ids="extract_db_task")
    orders_json_str = raw_orders["orders"]

    # Cek apakah format records (list) atau columns (dict)
    parsed = json.loads(orders_json_str)

    if isinstance(parsed, list):
        # Format records — langsung ke DataFrame
        df_o = pd.DataFrame(parsed)
    else:
        # Format columns — pakai read_json biasa
        df_o = pd.read_json(StringIO(orders_json_str), convert_dates=False)

    print(f"  Shape df_o            : {df_o.shape}")
    print(f"  Kolom df_o            : {df_o.columns.tolist()}")
    print(f"  Sample order_time raw : {df_o['order_time'].head(3).tolist()}")

    # Parse datetime dari string ISO
    df_o["order_time"] = pd.to_datetime(df_o["order_time"], errors="coerce")
    df_o = df_o.dropna(subset=["order_time"])

    print(f"  Rows valid order_time : {len(df_o):,}")

    if df_o.empty:
        raise RuntimeError("Tidak ada data order_time yang valid setelah parsing!")

    start_date = df_o["order_time"].min().strftime("%Y-%m-%d")
    end_date   = (df_o["order_time"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Rentang waktu order   : {start_date} → {end_date}")
    print(f"  Rentang waktu order: {start_date} → {end_date}")

    # ── Buat daftar rentang bulanan (mengakali limit 5000 GEE) ───────────────
    date_ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    while current <= end_dt:
        s = current.strftime("%Y-%m-%d")
        if current.month == 12:
            e = current.replace(year=current.year + 1, month=1)
        else:
            e = current.replace(month=current.month + 1)
        date_ranges.append((s, e.strftime("%Y-%m-%d")))
        current = e

    # ── Inisialisasi GEE ──────────────────────────────────────────────────────
    KEY_PATH = "/home/ari_sadewa/airflow/dags/keys/paradokstesting-6e70dda26759.json"

    try:
        # SESUDAH — tambahkan scope GEE yang benar
        credentials = service_account.Credentials.from_service_account_file(
            KEY_PATH,
            scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
            ]
        )
        ee.Initialize(credentials=credentials, project=GEE_PROJECT)
        print("  ✓ Auth GEE menggunakan Service Account berhasil.")
    except Exception as e:
        raise RuntimeError(f"Gagal melakukan autentikasi GEE: {e}")

    # ── Helper: hitung relative humidity ─────────────────────────────────────
    def hitung_rh(image):
        temp = image.select("temperature_2m").subtract(273.15)
        dew = image.select("dewpoint_temperature_2m").subtract(273.15)
        rh = image.expression(
            "100 * (exp((17.625 * td) / (243.04 + td)) / exp((17.625 * t) / (243.04 + t)))",
            {"t": temp, "td": dew},
        ).rename("relative_humidity_2m")
        return image.addBands(rh)

    # ── Helper: klasifikasi cuaca ─────────────────────────────────────────────
    def classify_weather(temp_k, rain, humidity):
        temp_c = (temp_k - 273.15) if temp_k else None
        if rain and rain > 0.002:
            return "Hujan"
        if humidity and humidity > 85:
            return "Berawan"
        if temp_c and temp_c > 30:
            return "Panas"
        return "Cerah"

    # ── Ekstraksi per kota per bulan ─────────────────────────────────────────
    all_rows = []
    for city_key, city_info in CITY_COORDS.items():
        city_name = city_info["name"]
        coord = city_info["coord"]
        point = ee.Geometry.Point(coord)
        print(f"\n  --- Kota: {city_name} ({len(date_ranges)} periode) ---")

        for start_d, end_d in date_ranges:
            print(f"    Mengambil: {start_d} s/d {end_d}")
            dataset = (
                ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                .filterDate(start_d, end_d)
                .select(
                    ["temperature_2m", "total_precipitation", "dewpoint_temperature_2m"]
                )
                .map(hitung_rh)
            )

            def extract_feature(image):
                stats = image.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=point,
                    scale=11132,
                )
                return ee.Feature(
                    None,
                    {
                        "time": image.date().format(),
                        "temp": stats.get("temperature_2m"),
                        "rain": stats.get("total_precipitation"),
                        "humidity": stats.get("relative_humidity_2m"),
                    },
                )

            try:
                features = dataset.map(extract_feature).getInfo()
                for f in features["features"]:
                    prop = f["properties"]
                    cuaca = classify_weather(
                        prop.get("temp"), prop.get("rain"), prop.get("humidity")
                    )
                    all_rows.append(
                        {
                            "city": city_key,
                            "wilayah": city_name,
                            "waktu": prop.get("time"),
                            "cuaca": cuaca,
                        }
                    )
            except Exception as e:
                print(f"    ⚠ Gagal pada {start_d}: {e}")
                continue

    # ── Simpan hasil ─────────────────────────────────────────────────────────
    if not all_rows:
        raise RuntimeError("GEE: Tidak ada data cuaca yang berhasil diambil!")

    df_weather = pd.DataFrame(all_rows)
    df_weather["waktu"] = pd.to_datetime(df_weather["waktu"])
    df_weather = (
        df_weather.groupby(["city", "wilayah", pd.Grouper(key="waktu", freq="h")])[
            "cuaca"
        ]
        .first()
        .reset_index()
    )
    print(f"\n  ✓ Total data cuaca: {len(df_weather):,} baris")
    print("=== [EXTRACT] GEE selesai ===")
    return df_weather.to_json()


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2 — TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════


def transform_dimensions(**kwargs):
    """
    Bangun tabel dimensi dari data mentah:
      dim_date, dim_user, dim_driver, dim_merchant, dim_product, dim_weather
    """
    print("=== [TRANSFORM] Memulai transformasi dimensi ===")
    ti = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="extract_db_task")

    df_u = pd.read_json(StringIO(raw["users"]))
    df_d = pd.read_json(StringIO(raw["drivers"]))
    df_m = pd.read_json(StringIO(raw["merchants"]))
    df_p = pd.read_json(StringIO(raw["products"]))
    df_o = pd.read_json(StringIO(raw["orders"]))
    df_r = pd.read_json(StringIO(raw["reviews"]))
    df_o["order_time"] = pd.to_datetime(df_o["order_time"])

    raw_weather = ti.xcom_pull(task_ids="extract_gee_task")
    df_w = pd.read_json(StringIO(raw_weather))
    df_w["waktu"] = pd.to_datetime(df_w["waktu"])

    # ── dim_date ──────────────────────────────────────────────────────────────
    dates = pd.to_datetime(df_o["order_time"].dt.date.unique())
    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_id"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.strftime("%B")
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["day_name"] = dim_date["full_date"].dt.strftime("%A")
    dim_date["day_of_week"] = dim_date["full_date"].dt.dayofweek + 1  # 1=Mon..7=Sun
    dim_date["is_weekend"] = dim_date["full_date"].dt.dayofweek.isin([5, 6]).astype(int)
    dim_date = dim_date[
        [
            "date_id",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_name",
            "day_of_week",
            "is_weekend",
        ]
    ]
    print(f"  ✓ dim_date     : {len(dim_date):,} rows")

    # ── dim_user ──────────────────────────────────────────────────────────────
    df_u["date_of_birth"] = pd.to_datetime(df_u["date_of_birth"])
    df_u["age"] = ((pd.Timestamp.now() - df_u["date_of_birth"]).dt.days / 365).astype(
        int
    )
    df_u["age_group"] = np.select(
        [
            df_u["age"] < 18,
            df_u["age"] < 25,
            df_u["age"] < 35,
            df_u["age"] < 45,
            df_u["age"] < 55,
        ],
        ["<18", "18-24", "25-34", "35-44", "45-54"],
        default="55+",
    )
    dim_user = df_u.rename(
        columns={
            "full_name": "user_name",
            "address_area": "user_area",
            "is_active": "user_is_active",
        }
    )[
        [
            "user_id",
            "user_name",
            "gender",
            "age",
            "age_group",
            "city",
            "user_area",
            "lat",
            "lon",
            "user_is_active",
        ]
    ].rename(
        columns={"lat": "user_lat", "lon": "user_lon"}
    )
    print(f"  ✓ dim_user     : {len(dim_user):,} rows")

    # ── dim_driver ────────────────────────────────────────────────────────────
    dim_driver = df_d.rename(
        columns={
            "full_name": "driver_name",
            "current_area": "driver_area",
            "rating": "driver_rating",
            "is_active": "driver_is_active",
        }
    )[
        [
            "driver_id",
            "driver_name",
            "vehicle_type",
            "city",
            "driver_area",
            "driver_rating",
            "total_trips",
            "driver_is_active",
        ]
    ]
    print(f"  ✓ dim_driver   : {len(dim_driver):,} rows")

    # ── dim_merchant ──────────────────────────────────────────────────────────
    # Hitung rata-rata merchant_rating dari reviews
    avg_review = (
        df_r.groupby("merchant_id")["merchant_rating"]
        .mean()
        .round(2)
        .reset_index()
        .rename(columns={"merchant_rating": "avg_review_rating"})
    )
    df_m = df_m.merge(avg_review, on="merchant_id", how="left")
    df_m["avg_review_rating"] = df_m["avg_review_rating"].fillna(df_m.get("rating", 0))

    dim_merchant = df_m.rename(
        columns={
            "category": "merchant_category",
            "area": "merchant_area",
            "lat": "merchant_lat",
            "lon": "merchant_lon",
            "rating": "merchant_rating",
            "is_active": "merchant_is_active",
        }
    )[
        [
            "merchant_id",
            "merchant_name",
            "merchant_category",
            "city",
            "merchant_area",
            "merchant_lat",
            "merchant_lon",
            "merchant_rating",
            "avg_review_rating",
            "merchant_is_active",
        ]
    ]
    print(f"  ✓ dim_merchant : {len(dim_merchant):,} rows")

    # ── dim_product ───────────────────────────────────────────────────────────
    dim_product = df_p.rename(columns={"category": "product_category"})[
        [
            "product_id",
            "merchant_id",
            "product_name",
            "product_category",
            "price",
            "is_available",
        ]
    ]
    print(f"  ✓ dim_product  : {len(dim_product):,} rows")

    # ── dim_weather ───────────────────────────────────────────────────────────
    # Surrogate key: weather_id = city + waktu jam (YYYYMMDDHH format)
    df_w["weather_id"] = df_w["city"] + "_" + df_w["waktu"].dt.strftime("%Y%m%d%H")
    df_w["date_id"] = df_w["waktu"].dt.strftime("%Y%m%d").astype(int)
    df_w["hour"] = df_w["waktu"].dt.hour
    dim_weather = df_w.rename(columns={"cuaca": "kondisi_cuaca"})[
        [
            "weather_id",
            "city",
            "wilayah",
            "waktu",
            "date_id",
            "hour",
            "kondisi_cuaca",
        ]
    ]
    print(f"  ✓ dim_weather  : {len(dim_weather):,} rows")

    print("=== [TRANSFORM] Dimensi selesai ===")
    return {
        "dim_date": dim_date.to_json(),
        "dim_user": dim_user.to_json(),
        "dim_driver": dim_driver.to_json(),
        "dim_merchant": dim_merchant.to_json(),
        "dim_product": dim_product.to_json(),
        "dim_weather": dim_weather.to_json(),
    }


def transform_facts(**kwargs):
    """
    Bangun tabel fakta dari data mentah + dimensi yang sudah ada:
      fact_orders      : grain = 1 order
      fact_order_items : grain = 1 baris item dalam order
    Kedua tabel ini di-enrich dengan weather_id untuk analisis cuaca.
    """
    print("=== [TRANSFORM] Memulai transformasi fakta ===")
    ti = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="extract_db_task")
    dim = ti.xcom_pull(task_ids="transform_dim_task")

    df_o = pd.read_json(StringIO(raw["orders"]))
    df_oi = pd.read_json(StringIO(raw["order_items"]))
    df_w = pd.read_json(StringIO(dim["dim_weather"]))

    df_o["order_time"] = pd.to_datetime(df_o["order_time"])
    df_w["waktu"] = pd.to_datetime(df_w["waktu"])

    # ── Buat lookup kunci join cuaca ──────────────────────────────────────────
    # Kunci: city + jam (dibulatkan ke jam)
    df_o["order_hour_ts"] = df_o["order_time"].dt.floor("h")
    df_o["weather_key"] = (
        df_o["city"] + "_" + df_o["order_hour_ts"].dt.strftime("%Y%m%d%H")
    )

    weather_lookup = df_w.set_index("weather_id")[["kondisi_cuaca"]].to_dict()[
        "kondisi_cuaca"
    ]
    df_o["kondisi_cuaca"] = (
        df_o["weather_key"].map(weather_lookup).fillna("Tidak Diketahui")
    )
    df_o["weather_id"] = df_o.apply(
        lambda r: r["weather_key"] if r["weather_key"] in weather_lookup else None,
        axis=1,
    )

    # ── fact_orders ───────────────────────────────────────────────────────────
    df_o["date_id"] = df_o["order_time"].dt.strftime("%Y%m%d").astype(int)
    df_o["order_hour"] = df_o["order_time"].dt.hour
    df_o["is_cancelled"] = df_o["status"].str.contains("cancel", na=False).astype(int)
    df_o["is_delivered"] = (df_o["status"] == "delivered").astype(int)
    df_o["is_weekend"] = df_o["order_time"].dt.dayofweek.isin([5, 6]).astype(int)

    df_o = df_o.rename(columns={"status": "order_status"})

    fact_orders = df_o[
        [
            "order_id",
            "user_id",
            "driver_id",
            "merchant_id",
            "date_id",
            "order_hour",
            "is_weekend",
            "city",
            "order_status",
            "payment_method",
            "subtotal",
            "delivery_fee",
            "discount",
            "total_amount",
            "distance_km",
            "delivery_area",
            "is_delivered",
            "is_cancelled",
            "weather_id",
            "kondisi_cuaca",
        ]
    ]
    print(f"  ✓ fact_orders      : {len(fact_orders):,} rows")

    # ── fact_order_items ──────────────────────────────────────────────────────
    orders_slim = df_o[
        [
            "order_id",
            "user_id",
            "date_id",
            "city",
            "order_status",
            "order_time",
            "weather_id",
            "kondisi_cuaca",
        ]
    ].rename(columns={"order_status": "os"})

    fact_order_items = df_oi.merge(orders_slim, on="order_id", how="left")
    fact_order_items = fact_order_items.rename(
        columns={
            "category": "product_category",
            "subtotal": "item_subtotal",
            "os": "order_status",
        }
    )[
        [
            "order_item_id",
            "order_id",
            "product_id",
            "merchant_id",
            "user_id",
            "date_id",
            "city",
            "product_name",
            "product_category",
            "quantity",
            "unit_price",
            "item_subtotal",
            "order_status",
            "weather_id",
            "kondisi_cuaca",
        ]
    ]
    print(f"  ✓ fact_order_items : {len(fact_order_items):,} rows")

    print("=== [TRANSFORM] Fakta selesai ===")
    return {
        "fact_orders": fact_orders.to_json(),
        "fact_order_items": fact_order_items.to_json(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3 — LOAD
# ══════════════════════════════════════════════════════════════════════════════


def load_dimensions_to_dwh(**kwargs):
    """Load semua tabel dimensi ke Data Warehouse."""
    print("=== [LOAD] Memulai load dimensi ke DWH ===")
    ti = kwargs["ti"]
    dims = ti.xcom_pull(task_ids="transform_dim_task")
    engine = create_engine(DWH_URL)

    for table_name, json_str in dims.items():
        df = pd.read_json(StringIO(json_str))
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"  ✓ {table_name:20s}: {len(df):,} rows → DWH")

    print("=== [LOAD] Dimensi selesai ===")


def load_facts_to_dwh(**kwargs):
    """Load semua tabel fakta ke Data Warehouse."""
    print("=== [LOAD] Memulai load fakta ke DWH ===")
    ti = kwargs["ti"]
    facts = ti.xcom_pull(task_ids="transform_fact_task")
    engine = create_engine(DWH_URL)

    for table_name, json_str in facts.items():
        df = pd.read_json(StringIO(json_str))
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"  ✓ {table_name:20s}: {len(df):,} rows → DWH")

    print("=== [LOAD] Fakta selesai ===")


# ══════════════════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="gofood_analytics_etl",
    default_args=default_args,
    description="ETL Pipeline: GoFood DWH dengan integrasi GEE (cuaca)",
    schedule="@daily",
    catchup=False,
    tags=["gofood", "etl", "dwh", "gee"],
) as dag:

    # ── Step 1: Extract ───────────────────────────────────────────────────────
    t_extract_db = PythonOperator(
        task_id="extract_db_task",
        python_callable=extract_from_databases,
        doc_md="Ekstrak data dari MySQL (users, drivers) dan PostgreSQL (merchants, products, orders, order_items, reviews)",
    )

    t_extract_gee = PythonOperator(
        task_id="extract_gee_task",
        python_callable=extract_weather_from_gee,
        doc_md="Ekstrak data cuaca dari GEE ERA5-Land dengan rentang waktu dinamis dari tabel orders",
    )

    # ── Step 2: Transform ─────────────────────────────────────────────────────
    t_transform_dim = PythonOperator(
        task_id="transform_dim_task",
        python_callable=transform_dimensions,
        doc_md="Bangun 6 tabel dimensi: dim_date, dim_user, dim_driver, dim_merchant, dim_product, dim_weather",
    )

    t_transform_fact = PythonOperator(
        task_id="transform_fact_task",
        python_callable=transform_facts,
        doc_md="Bangun 2 tabel fakta: fact_orders, fact_order_items (di-enrich dengan weather_id)",
    )

    # ── Step 3: Load ──────────────────────────────────────────────────────────
    t_load_dim = PythonOperator(
        task_id="load_dim_task",
        python_callable=load_dimensions_to_dwh,
        doc_md="Load 6 tabel dimensi ke DWH MySQL",
    )

    t_load_fact = PythonOperator(
        task_id="load_fact_task",
        python_callable=load_facts_to_dwh,
        doc_md="Load 2 tabel fakta ke DWH MySQL",
    )

    # ── Dependency Graph ──────────────────────────────────────────────────────
    #
    #   extract_db_task ──┬──► extract_gee_task ──┐
    #                     │                       │
    #                     └───────────────────────┴──► transform_dim_task
    #                                                         │
    #                                                         ▼
    #                                                  transform_fact_task
    #                                                     /         \
    #                                                    ▼           ▼
    #                                               load_dim_task  load_fact_task
    #
    t_extract_db >> t_extract_gee
    [t_extract_db, t_extract_gee] >> t_transform_dim
    t_transform_dim >> t_transform_fact
    t_transform_dim >> t_load_dim
    t_transform_fact >> t_load_fact
