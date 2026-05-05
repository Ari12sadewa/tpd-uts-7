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
MYSQL_SRC_URL = "mysql+pymysql://root:@localhost:3306/source_a_uts"
PG_SRC_URL    = "postgresql+psycopg2://postgres:12345@localhost:5432/source_b_uts"
DWH_URL       = "mysql+pymysql://root:@localhost:3306/dwh_uts"

# ==============================
# KONFIGURASI GEE
# ==============================
GEE_PROJECT  = "paradokstesting"
GEE_KEY_PATH = "/home/ari_sadewa/airflow/dags/keys/paradokstesting-6e70dda26759.json"

CITY_COORDS = {
    "jakarta":  {"name": "DKI Jakarta", "coord": [106.8,  -6.2]},
    "surabaya": {"name": "Surabaya",    "coord": [112.75, -7.25]},
    "medan":    {"name": "Medan",       "coord": [98.67,   3.59]},
}

# ==============================
# DEFAULT ARGS
# ==============================
default_args = {
    "owner":        "Ari Sadewa",
    "start_date":   datetime(2026, 4, 1),
    "retries":      1,
    "retry_delay":  timedelta(minutes=5),
}


# ==========================================================================
#  HELPER — DATA CLEANING
# ==========================================================================

def _log_cleaning(label: str, before: int, after: int) -> None:
    """Cetak ringkasan baris yang dihapus/dimodifikasi saat cleaning."""
    removed = before - after
    pct     = (removed / before * 100) if before > 0 else 0
    print(f"    [{label}] {before:,} → {after:,} rows  (−{removed:,} / {pct:.1f}%)")


def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleaning dim_user / tabel users:
      1. Drop duplikat berdasarkan user_id (pertahankan baris pertama)
      2. Drop baris di mana user_id atau full_name null (kolom kritis)
      3. Imputasi null:
         - date_of_birth  → modus per city  (kategorik-ish, karena tahun lahir)
         - gender         → modus per city
         - lat / lon      → rata-rata per city
         - address_area   → modus per city
         - is_active      → default 1 (asumsi aktif jika tidak diketahui)
    """
    print("  [CLEAN] users ...")
    n0 = len(df)

    # 1. Deduplikasi berdasarkan user_id
    df = df.drop_duplicates(subset=["user_id"], keep="first")
    _log_cleaning("dedup user_id", n0, len(df))

    # 2. Drop jika kolom kritis null
    n1 = len(df)
    df = df.dropna(subset=["user_id", "full_name"])
    _log_cleaning("drop null PK/name", n1, len(df))

    # 3. Imputasi gender → modus per city
    if df["gender"].isna().any():
        gender_mode = df.groupby("city")["gender"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown")
        )
        df["gender"] = df["gender"].fillna(gender_mode).fillna("Unknown")

    # 4. Imputasi date_of_birth → modus per city
    if df["date_of_birth"].isna().any():
        dob_mode = df.groupby("city")["date_of_birth"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else pd.NaT)
        )
        df["date_of_birth"] = df["date_of_birth"].fillna(dob_mode)
        # Jika masih null setelah modus per kota, drop (tidak bisa hitung usia)
        n2 = len(df)
        df = df.dropna(subset=["date_of_birth"])
        _log_cleaning("drop null date_of_birth", n2, len(df))

    # 5. Imputasi lat / lon → rata-rata per city
    for col in ["lat", "lon"]:
        if df[col].isna().any():
            df[col] = df.groupby("city")[col].transform(
                lambda x: x.fillna(x.mean())
            )
            # Fallback global mean jika seluruh kota null
            df[col] = df[col].fillna(df[col].mean())

    # 6. Imputasi address_area → modus per city
    if df["address_area"].isna().any():
        area_mode = df.groupby("city")["address_area"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown")
        )
        df["address_area"] = df["address_area"].fillna(area_mode).fillna("Unknown")

    # 7. Imputasi is_active → default 1
    df["is_active"] = df["is_active"].fillna(1).astype(int)

    print(f"    users bersih : {len(df):,} rows")
    return df.reset_index(drop=True)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
   
    print("  [CLEAN] orders ...")
    n0 = len(df)

    # 1. Deduplikasi order_id
    df = df.drop_duplicates(subset=["order_id"], keep="first")
    _log_cleaning("dedup order_id", n0, len(df))

    # 2. Drop FK kritis null
    n1 = len(df)
    df = df.dropna(subset=["order_id", "user_id", "merchant_id"])
    _log_cleaning("drop null FK", n1, len(df))

    # 3. Drop order_time invalid
    df["order_time"] = pd.to_datetime(df["order_time"], errors="coerce")
    n2 = len(df)
    df = df.dropna(subset=["order_time"])
    _log_cleaning("drop null order_time", n2, len(df))

    # 4. payment_method → modus per city
    if df["payment_method"].isna().any():
        pay_mode = df.groupby("city")["payment_method"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "cash")
        )
        df["payment_method"] = df["payment_method"].fillna(pay_mode).fillna("cash")

    # 5. subtotal → rata-rata per merchant
    if df["subtotal"].isna().any():
        df["subtotal"] = df.groupby("merchant_id")["subtotal"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df["subtotal"] = df["subtotal"].fillna(round(df["subtotal"].mean(), 2))

    # 6. delivery_fee → rata-rata per city
    if df["delivery_fee"].isna().any():
        df["delivery_fee"] = df.groupby("city")["delivery_fee"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df["delivery_fee"] = df["delivery_fee"].fillna(round(df["delivery_fee"].mean(), 2))

    # 7. discount → 0
    df["discount"] = df["discount"].fillna(0.0)

    # 8. total_amount → hitung ulang jika null
    mask_null_total = df["total_amount"].isna()
    if mask_null_total.any():
        df.loc[mask_null_total, "total_amount"] = (
            df.loc[mask_null_total, "subtotal"]
            + df.loc[mask_null_total, "delivery_fee"]
            - df.loc[mask_null_total, "discount"]
        )
    # Fallback jika masih null
    df["total_amount"] = df["total_amount"].fillna(
        round(df["total_amount"].mean(), 2)
    )

    # 9. delivery_area → modus per city
    if df["delivery_area"].isna().any():
        area_mode = df.groupby("city")["delivery_area"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown")
        )
        df["delivery_area"] = df["delivery_area"].fillna(area_mode).fillna("Unknown")

    # 10. distance_km → rata-rata per delivery_area
    if df["distance_km"].isna().any():
        df["distance_km"] = df.groupby("delivery_area")["distance_km"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df["distance_km"] = df["distance_km"].fillna(round(df["distance_km"].mean(), 2))

    print(f"    orders bersih: {len(df):,} rows")
    return df.reset_index(drop=True)


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:

    print("  [CLEAN] order_items ...")
    n0 = len(df)

    # 1. Deduplikasi order_item_id
    df = df.drop_duplicates(subset=["order_item_id"], keep="first")
    _log_cleaning("dedup order_item_id", n0, len(df))

    # 2. Drop FK kritis null
    n1 = len(df)
    df = df.dropna(subset=["order_item_id", "order_id", "product_id"])
    _log_cleaning("drop null FK", n1, len(df))

    # 3. quantity → 1 jika null atau <= 0
    df["quantity"] = df["quantity"].fillna(1)
    df.loc[df["quantity"] <= 0, "quantity"] = 1

    # 4. unit_price → rata-rata per product_id
    if df["unit_price"].isna().any():
        df["unit_price"] = df.groupby("product_id")["unit_price"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df["unit_price"] = df["unit_price"].fillna(round(df["unit_price"].mean(), 2))

    # 5. subtotal → hitung ulang jika null
    mask_null_sub = df["subtotal"].isna()
    if mask_null_sub.any():
        df.loc[mask_null_sub, "subtotal"] = (
            df.loc[mask_null_sub, "quantity"] * df.loc[mask_null_sub, "unit_price"]
        )

    print(f"    order_items bersih: {len(df):,} rows")
    return df.reset_index(drop=True)


# ==========================================================================
#  EXTRACT
# ==========================================================================


def extract_from_databases(**kwargs):
    """
    Ekstrak semua tabel dari Source A (MySQL) dan Source B (PostgreSQL).
    Hasilnya di-push ke XCom sebagai JSON string.
    """
    print("=== [EXTRACT] Memulai ekstraksi dari database ===")
    engine_mysql = create_engine(MYSQL_SRC_URL)
    engine_pg    = create_engine(PG_SRC_URL)

    data = {
        # Source A — MySQL
        "users":       pd.read_sql("SELECT * FROM users",       engine_mysql).to_json(),
        "drivers":     pd.read_sql("SELECT * FROM drivers",     engine_mysql).to_json(),
        # Source B — PostgreSQL
        "merchants":   pd.read_sql("SELECT * FROM merchants",   engine_pg).to_json(),
        "products":    pd.read_sql("SELECT * FROM products",    engine_pg).to_json(),
        "orders":      pd.read_sql("SELECT * FROM orders",      engine_pg).to_json(orient="records", date_format="iso"),
        "order_items": pd.read_sql("SELECT * FROM order_items", engine_pg).to_json(),
        "reviews":     pd.read_sql("SELECT * FROM reviews",     engine_pg).to_json(),
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
    di tabel orders.

    Output: JSON string (wilayah, waktu, cuaca).
    """
    print("=== [EXTRACT] Memulai ekstraksi cuaca dari GEE ===")
    ti = kwargs["ti"]

    # -- Ambil rentang waktu dari tabel orders --------------------------------
    raw_orders = ti.xcom_pull(task_ids="extract_db_task")
    df_o = pd.read_json(StringIO(raw_orders["orders"]), convert_dates=False)

    print(f"  Shape df_o            : {df_o.shape}")
    print(f"  Kolom df_o            : {df_o.columns.tolist()}")
    print(f"  Sample order_time raw : {df_o['order_time'].head(3).tolist()}")

    df_o["order_time"] = pd.to_datetime(df_o["order_time"], errors="coerce")
    prevLen = len(df_o)
    df_o    = df_o.dropna(subset=["order_time"])

    print(f"  Rows before validate order_time : {prevLen:,}")
    print(f"  Rows validated order_time : {len(df_o):,}")

    if df_o.empty:
        raise RuntimeError("Tidak ada data order_time yang valid setelah parsing!")

    start_date = df_o["order_time"].min().strftime("%Y-%m-%d")
    end_date   = (df_o["order_time"].max() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Rentang waktu order   : {start_date} → {end_date}")

    # -- Buat daftar rentang bulanan -----------------------------------------------
    date_ranges = []
    current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end_dt  = datetime.strptime(end_date,   "%Y-%m-%d")
    while current <= end_dt:
        s = current.strftime("%Y-%m-%d")
        e = current.replace(year=current.year + 1, month=1) if current.month == 12 \
            else current.replace(month=current.month + 1)
        date_ranges.append((s, e.strftime("%Y-%m-%d")))
        current = e

    # -- Inisialisasi GEE ---------------------------------------------------------------
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GEE_KEY_PATH,
            scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
            ]
        )
        ee.Initialize(credentials=credentials, project=GEE_PROJECT)
        print("  ✓ Auth GEE menggunakan Service Account berhasil.")
    except Exception as e:
        raise RuntimeError(f"Gagal melakukan autentikasi GEE: {e}")
   
    # -- HELPER Extract Data GEE ---------------------------------------------------
        temp = image.select("temperature_2m").subtract(273.15)
        dew  = image.select("dewpoint_temperature_2m").subtract(273.15)
        rh   = image.expression(
            "100 * (exp((17.625 * td) / (243.04 + td)) / exp((17.625 * t) / (243.04 + t)))",
            {"t": temp, "td": dew},
        ).rename("relative_humidity_2m")
        return image.addBands(rh)

    def classify_weather(temp_k, rain, humidity):
        temp_c = (temp_k - 273.15) if temp_k else None
        if rain and rain > 0.002:
            return "Hujan"
        if humidity and humidity > 85:
            return "Berawan"
        if temp_c and temp_c > 30:
            return "Panas"
        return "Cerah"

    # -- Mulai Extract Data ---------------------------------------------------
    all_rows = []
    for city_key, city_info in CITY_COORDS.items():
        city_name = city_info["name"]
        coord     = city_info["coord"]
        point     = ee.Geometry.Point(coord)
        print(f"\n  --- Kota: {city_name} ({len(date_ranges)} periode) ---")

        for start_d, end_d in date_ranges:
            print(f"    Mengambil: {start_d} s/d {end_d}")
            dataset = (
                ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
                .filterDate(start_d, end_d)
                .select(["temperature_2m", "total_precipitation", "dewpoint_temperature_2m"])
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
                        "time":     image.date().format(),
                        "temp":     stats.get("temperature_2m"),
                        "rain":     stats.get("total_precipitation"),
                        "humidity": stats.get("relative_humidity_2m"),
                    },
                )

            try:
                features = dataset.map(extract_feature).getInfo()
                for f in features["features"]:
                    prop  = f["properties"]
                    cuaca = classify_weather(
                        prop.get("temp"), prop.get("rain"), prop.get("humidity")
                    )
                    all_rows.append({
                        "city":    city_key,
                        "wilayah": city_name,
                        "waktu":   prop.get("time"),
                        "cuaca":   cuaca,
                    })
            except Exception as e:
                print(f"    ⚠ Gagal pada {start_d}: {e}")
                continue

    if not all_rows:
        raise RuntimeError("GEE: Tidak ada data cuaca yang berhasil diambil!")

    df_weather = pd.DataFrame(all_rows)
    df_weather["waktu"] = pd.to_datetime(df_weather["waktu"])
    df_weather = (
        df_weather.groupby(["city", "wilayah", pd.Grouper(key="waktu", freq="h")])["cuaca"]
        .first()
        .reset_index()
    )
    print(df_weather["waktu"].head(n=5))
    print(f"\n  ✓ Total data cuaca: {len(df_weather):,} baris")
    print("=== [EXTRACT] GEE selesai ===")

    return df_weather.to_json(orient="records", date_format="iso")


# ==========================================================================
#  TRANSFORM
# ==========================================================================


def transform_dimensions(**kwargs):

    print("=== [TRANSFORM] Memulai transformasi dimensi ===")
    ti  = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="extract_db_task")

    # -- Load raw DataFrames -------------------------------------------------
    df_u = pd.read_json(StringIO(raw["users"]),       convert_dates=False)
    df_d = pd.read_json(StringIO(raw["drivers"]),     convert_dates=False)
    df_m = pd.read_json(StringIO(raw["merchants"]),   convert_dates=False)
    df_p = pd.read_json(StringIO(raw["products"]),    convert_dates=False)
    df_o = pd.read_json(StringIO(raw["orders"]),      convert_dates=False)
    df_r = pd.read_json(StringIO(raw["reviews"]),     convert_dates=False)

    df_o["order_time"] = pd.to_datetime(df_o["order_time"], errors="coerce")

    raw_weather = ti.xcom_pull(task_ids="extract_gee_task")
    df_w = pd.read_json(StringIO(raw_weather), convert_dates=False)
    df_w["waktu"] = pd.to_datetime(df_w["waktu"])

    print("\n--- [CLEANING] Mulai pembersihan data sumber ---")

    # ==========================================================================
    # CLEANING: users
    # ==========================================================================
    df_u["date_of_birth"] = pd.to_datetime(df_u["date_of_birth"], errors="coerce")
    df_u = clean_users(df_u)

    # ==========================================================================
    # CLEANING: drivers
    # ==========================================================================
    print("  [CLEAN] drivers ...")
    n0 = len(df_d)
    # Deduplikasi driver_id
    df_d = df_d.drop_duplicates(subset=["driver_id"], keep="first")
    _log_cleaning("dedup driver_id", n0, len(df_d))
    # Drop jika driver_id atau full_name null
    n1 = len(df_d)
    df_d = df_d.dropna(subset=["driver_id", "full_name"])
    _log_cleaning("drop null PK", n1, len(df_d))
    # Imputasi rating → rata-rata per kota
    if df_d["rating"].isna().any():
        df_d["rating"] = df_d.groupby("city")["rating"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df_d["rating"] = df_d["rating"].fillna(round(df_d["rating"].mean(), 2))
    # Imputasi vehicle_type → modus global
    if df_d["vehicle_type"].isna().any():
        mode_vt = df_d["vehicle_type"].mode()
        df_d["vehicle_type"] = df_d["vehicle_type"].fillna(
            mode_vt[0] if not mode_vt.empty else "motor"
        )
    # Imputasi total_trips → 0
    df_d["total_trips"] = df_d["total_trips"].fillna(0).astype(int)
    # Imputasi is_active → 1
    df_d["is_active"] = df_d["is_active"].fillna(1).astype(int)
    print(f"    drivers bersih: {len(df_d):,} rows")

    # ==========================================================================
    # CLEANING: merchants
    # ==========================================================================
    print("  [CLEAN] merchants ...")
    n0 = len(df_m)
    df_m = df_m.drop_duplicates(subset=["merchant_id"], keep="first")
    _log_cleaning("dedup merchant_id", n0, len(df_m))
    n1 = len(df_m)
    df_m = df_m.dropna(subset=["merchant_id", "merchant_name"])
    _log_cleaning("drop null PK", n1, len(df_m))
    # Imputasi lat/lon → rata-rata per city
    for col in ["lat", "lon"]:
        if df_m[col].isna().any():
            df_m[col] = df_m.groupby("city")[col].transform(
                lambda x: x.fillna(round(x.mean(), 6))
            )
            df_m[col] = df_m[col].fillna(round(df_m[col].mean(), 6))
    # Imputasi rating → rata-rata per category
    if df_m["rating"].isna().any():
        df_m["rating"] = df_m.groupby("category")["rating"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df_m["rating"] = df_m["rating"].fillna(round(df_m["rating"].mean(), 2))
    # Imputasi area → modus per city
    if df_m["area"].isna().any():
        area_mode = df_m.groupby("city")["area"].transform(
            lambda x: x.fillna(x.mode()[0] if not x.mode().empty else "Unknown")
        )
        df_m["area"] = df_m["area"].fillna(area_mode).fillna("Unknown")
    df_m["is_active"] = df_m["is_active"].fillna(1).astype(int)
    print(f"    merchants bersih: {len(df_m):,} rows")

    # ==========================================================================
    # CLEANING: products
    # ==========================================================================
    print("  [CLEAN] products ...")
    n0 = len(df_p)
    df_p = df_p.drop_duplicates(subset=["product_id"], keep="first")
    _log_cleaning("dedup product_id", n0, len(df_p))
    n1 = len(df_p)
    df_p = df_p.dropna(subset=["product_id", "merchant_id"])
    _log_cleaning("drop null FK", n1, len(df_p))
    # Imputasi price → rata-rata per category
    if df_p["price"].isna().any():
        df_p["price"] = df_p.groupby("category")["price"].transform(
            lambda x: x.fillna(round(x.mean(), 2))
        )
        df_p["price"] = df_p["price"].fillna(round(df_p["price"].mean(), 2))
    # Imputasi product_name → "Unknown Product"
    df_p["product_name"] = df_p["product_name"].fillna("Unknown Product")
    # Imputasi category → modus global
    if df_p["category"].isna().any():
        mode_cat = df_p["category"].mode()
        df_p["category"] = df_p["category"].fillna(
            mode_cat[0] if not mode_cat.empty else "Lainnya"
        )
    df_p["is_available"] = df_p["is_available"].fillna(1).astype(int)
    print(f"    products bersih: {len(df_p):,} rows")

    print("--- [CLEANING] Selesai ---\n")

    # ==========================================================================
    # BUILD DIMENSIONS
    # ==========================================================================

    # -- dim_date ──────────────────────────────────────────────────────────────
    dates    = pd.to_datetime(df_o["order_time"].dt.date.unique())
    dim_date = pd.DataFrame({"full_date": dates})
    dim_date["date_id"]     = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"]        = dim_date["full_date"].dt.year
    dim_date["quarter"]     = dim_date["full_date"].dt.quarter
    dim_date["month"]       = dim_date["full_date"].dt.month
    dim_date["month_name"]  = dim_date["full_date"].dt.strftime("%B")
    dim_date["day"]         = dim_date["full_date"].dt.day
    dim_date["day_name"]    = dim_date["full_date"].dt.strftime("%A")
    dim_date["day_of_week"] = dim_date["full_date"].dt.dayofweek + 1
    dim_date["is_weekend"]  = dim_date["full_date"].dt.dayofweek.isin([5, 6]).astype(int)
    dim_date = dim_date[[
        "date_id", "full_date", "year", "quarter", "month",
        "month_name", "day", "day_name", "day_of_week", "is_weekend",
    ]]
    print(f"  ✓ dim_date     : {len(dim_date):,} rows")

    # -- dim_user ---------------------------------------------------
    df_u["age"] = ((pd.Timestamp.now() - df_u["date_of_birth"]).dt.days / 365).astype(int)
    
    # -- membuat kelompok usia ---------------------------------------------------
    df_u["age_group"] = np.select(
        [df_u["age"] < 18, df_u["age"] < 25, df_u["age"] < 35,
         df_u["age"] < 45, df_u["age"] < 55],
        ["<18", "18-24", "25-34", "35-44", "45-54"],
        default="55+",
    )
    
    # -- rename nama kolom untuk menjaga konsistensi lalu save ------------------------
    dim_user = df_u.rename(columns={
        "full_name":    "user_name",
        "address_area": "user_area",
        "is_active":    "user_is_active",
    })[["user_id", "user_name", "gender", "age", "age_group",
        "city", "user_area", "lat", "lon", "user_is_active"]]\
        .rename(columns={"lat": "user_lat", "lon": "user_lon"})
    print(f"  ✓ dim_user     : {len(dim_user):,} rows")

    # -- dim_driver ---------------------------------------------------
    dim_driver = df_d.rename(columns={
        "full_name":    "driver_name",
        "current_area": "driver_area",
        "rating":       "driver_rating",
        "is_active":    "driver_is_active",
    })[["driver_id", "driver_name", "vehicle_type", "city",
        "driver_area", "driver_rating", "total_trips", "driver_is_active"]]
    print(f"  ✓ dim_driver   : {len(dim_driver):,} rows")

    # -- dim_merchant ---------------------------------------------------
    avg_review = (
        df_r.groupby("merchant_id")["merchant_rating"]
        .mean().round(2).reset_index()
        .rename(columns={"merchant_rating": "avg_review_rating"})
    )
    df_m = df_m.merge(avg_review, on="merchant_id", how="left")
    df_m["avg_review_rating"] = df_m["avg_review_rating"].fillna(df_m.get("rating", 0))

    dim_merchant = df_m.rename(columns={
        "category":  "merchant_category",
        "area":      "merchant_area",
        "lat":       "merchant_lat",
        "lon":       "merchant_lon",
        "rating":    "merchant_rating",
        "is_active": "merchant_is_active",
    })[["merchant_id", "merchant_name", "merchant_category", "city",
        "merchant_area", "merchant_lat", "merchant_lon",
        "merchant_rating", "avg_review_rating", "merchant_is_active"]]
    print(f"  ✓ dim_merchant : {len(dim_merchant):,} rows")

    # -- dim_product ---------------------------------------------------
    dim_product = df_p.rename(columns={"category": "product_category"})[
        ["product_id", "merchant_id", "product_name", "product_category",
         "price", "is_available"]
    ]
    print(f"  ✓ dim_product  : {len(dim_product):,} rows")

    # -- dim_weather ---------------------------------------------------
    df_w["weather_id"] = df_w["city"] + "_" + df_w["waktu"].dt.strftime("%Y%m%d%H")
    df_w["date_id"]    = df_w["waktu"].dt.strftime("%Y%m%d").astype(int)
    df_w["hour"]       = df_w["waktu"].dt.hour
    dim_weather = df_w.rename(columns={"cuaca": "kondisi_cuaca"})[
        ["weather_id", "city", "wilayah", "waktu", "date_id", "hour", "kondisi_cuaca"]
    ]
    print(f"  ✓ dim_weather  : {len(dim_weather):,} rows")

    print("=== [TRANSFORM] Dimensi selesai ===")
    return {
        "dim_date":     dim_date.to_json(),
        "dim_user":     dim_user.to_json(),
        "dim_driver":   dim_driver.to_json(),
        "dim_merchant": dim_merchant.to_json(),
        "dim_product":  dim_product.to_json(),
        "dim_weather":  dim_weather.to_json(orient="records", date_format="iso"),
    }


def transform_facts(**kwargs):
    """
    fact_orders      : grain = 1 order
    fact_order_items : grain = 1 baris item dalam order
    """
    print("=== [TRANSFORM] Memulai transformasi fakta ===")
    ti  = kwargs["ti"]
    raw = ti.xcom_pull(task_ids="extract_db_task")
    dim = ti.xcom_pull(task_ids="transform_dim_task")

    df_o  = pd.read_json(StringIO(raw["orders"]),      convert_dates=False)
    df_oi = pd.read_json(StringIO(raw["order_items"]), convert_dates=False)
    df_w  = pd.read_json(StringIO(dim["dim_weather"]), convert_dates=False)

    df_w["waktu"] = pd.to_datetime(df_w["waktu"])

    print("\n--- [CLEANING] Fakta ---")

    # ==========================================================================
    # CLEANING: orders & order_items
    # ==========================================================================
    df_o  = clean_orders(df_o)
    df_oi = clean_order_items(df_oi)

    print("--- [CLEANING] Selesai ---\n")

    # -- Buat lookup kunci join cuaca ---------------------------------------------------
    df_o["order_hour_ts"] = df_o["order_time"].dt.floor("h")
    df_o["weather_key"]   = (
        df_o["city"] + "_" + df_o["order_hour_ts"].dt.strftime("%Y%m%d%H")
    )
    print("orders weather_key:", df_o["weather_key"].head(n=5))
    print("weather weather_key:", df_w["weather_id"].head(n=5))

    weather_lookup = df_w.set_index("weather_id")[["kondisi_cuaca"]].to_dict()["kondisi_cuaca"]
    df_o["kondisi_cuaca"] = df_o["weather_key"].map(weather_lookup).fillna("Tidak Diketahui")
    df_o["weather_id"]    = df_o.apply(
        lambda r: r["weather_key"] if r["weather_key"] in weather_lookup else None,
        axis=1,
    )

    df_not_match = df_o[~df_o["weather_key"].isin(weather_lookup)]
    print("WEATHER NOT MATCH LENGTH", len(df_not_match))

    # -- fact_orders ---------------------------------------------------
    df_o["date_id"]     = df_o["order_time"].dt.strftime("%Y%m%d").astype(int)
    df_o["order_hour"]  = df_o["order_time"].dt.hour
    df_o["is_cancelled"]= df_o["status"].str.contains("cancel", na=False).astype(int)
    df_o["is_delivered"]= (df_o["status"] == "delivered").astype(int)
    df_o["is_weekend"]  = df_o["order_time"].dt.dayofweek.isin([5, 6]).astype(int)
    df_o = df_o.rename(columns={"status": "order_status"})

    fact_orders = df_o[[
        "order_id", "user_id", "driver_id", "merchant_id",
        "date_id", "order_hour", "is_weekend", "city",
        "order_status", "payment_method",
        "subtotal", "delivery_fee", "discount", "total_amount",
        "distance_km", "delivery_area",
        "is_delivered", "is_cancelled",
        "weather_id", "kondisi_cuaca",
    ]]
    print(f"  ✓ fact_orders      : {len(fact_orders):,} rows")

    # -- fact_order_items ---------------------------------------------------
    orders_slim = df_o[[
        "order_id", "user_id", "date_id", "city","weather_id", "kondisi_cuaca",
        "order_status", "order_time",
    ]].rename(columns={"order_status": "os"})

    fact_order_items = df_oi.merge(orders_slim, on="order_id", how="left")

    # Drop order_items yang tidak punya pasangan order (orphan)
    n_before = len(fact_order_items)
    fact_order_items = fact_order_items.dropna(subset=["user_id", "date_id"])
    _log_cleaning("drop orphan order_items", n_before, len(fact_order_items))

    fact_order_items = fact_order_items.rename(columns={
        "category":  "product_category",
        "subtotal":  "item_subtotal",
        "os":        "order_status",
    })[[
        "order_item_id", "order_id", "product_id", "merchant_id",
        "user_id", "date_id", "city",
        "product_name", "product_category",
        "quantity", "unit_price", "item_subtotal",
        "order_status", "weather_id", "kondisi_cuaca",
    ]]
    print(f"  ✓ fact_order_items : {len(fact_order_items):,} rows")

    print("=== [TRANSFORM] Fakta selesai ===")
    return {
        "fact_orders":      fact_orders.to_json(),
        "fact_order_items": fact_order_items.to_json(),
    }


# ==========================================================================
#  LOAD
# ==========================================================================


def load_dimensions_to_dwh(**kwargs):
    """Load semua tabel dimensi ke Data Warehouse."""
    print("=== [LOAD] Memulai load dimension ke DWH ===")
    ti     = kwargs["ti"]
    dims   = ti.xcom_pull(task_ids="transform_dim_task")
    engine = create_engine(DWH_URL)

    for table_name, json_str in dims.items():
        df = pd.read_json(StringIO(json_str))
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"  ✓ {table_name:20s}: {len(df):,} rows → DWH")

    print("=== [LOAD] Dimension done ===")


def load_facts_to_dwh(**kwargs):
    """Load semua fact table ke Data Warehouse."""
    print("=== [LOAD] Memulai load fakta ke DWH ===")
    ti     = kwargs["ti"]
    facts  = ti.xcom_pull(task_ids="transform_fact_task")
    engine = create_engine(DWH_URL)

    for table_name, json_str in facts.items():
        df = pd.read_json(StringIO(json_str))
        df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"  ✓ {table_name:20s}: {len(df):,} rows → DWH")

    print("=== [LOAD] Fact Table done ===")


# ==========================================================================
# DAG DEFINITION
# ==========================================================================

with DAG(
    dag_id="gofood_analytics_etl",
    default_args=default_args,
    description="ETL Pipeline: GoFood DWH dengan GEE",
    schedule="@daily",
    catchup=False,
) as dag:

    # -- Step 1: Extract ---------------------------------------------------
    t_extract_db = PythonOperator(
        task_id="extract_db_task",
        python_callable=extract_from_databases,
    )

    t_extract_gee = PythonOperator(
        task_id="extract_gee_task",
        python_callable=extract_weather_from_gee,
    )

    # -- Step 2: Transform ---------------------------------------------------
    t_transform_dim = PythonOperator(
        task_id="transform_dim_task",
        python_callable=transform_dimensions,
    )

    t_transform_fact = PythonOperator(
        task_id="transform_fact_task",
        python_callable=transform_facts,
    )

    # -- Step 3: Load ---------------------------------------------------
    t_load_dim = PythonOperator(
        task_id="load_dim_task",
        python_callable=load_dimensions_to_dwh,
    )

    t_load_fact = PythonOperator(
        task_id="load_fact_task",
        python_callable=load_facts_to_dwh,
    )

    t_extract_db >> t_extract_gee
    [t_extract_db, t_extract_gee] >> t_transform_dim
    t_transform_dim >> t_transform_fact
    t_transform_dim >> t_load_dim
    t_transform_fact >> t_load_fact
