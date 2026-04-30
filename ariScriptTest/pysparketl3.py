"""
GoFood ETL Pipeline - PySpark
Extract dari 2 SQLite database → Transform → Load ke DWH (Star Schema)

DWH Tables:
  Fact  : fact_orders
  Dims  : dim_user, dim_driver, dim_merchant, dim_product, dim_date, dim_location
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime
import os

os.environ["PYSPARK_PYTHON"] = "python"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

DB1_PATH  = "databases/user_driver.db"
DB2_PATH  = "databases/merchant_order.db"
DWH_PATH  = "databases/dwh_gofood.db"
TEMP_CSV  = Path("temp_spark")
TEMP_CSV.mkdir(exist_ok=True)

# ─── Init Spark ───────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("GoFood ETL Pipeline") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark Session started")

# ─── Helper: baca SQLite → Spark DF ──────────────────────────────────────────
def sqlite_to_spark(db_path: str, table: str):
    conn = sqlite3.connect(db_path)
    pdf  = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()
    return spark.createDataFrame(pdf)

# ──────────────────────────────────────────────────────────────────────────────
# EXTRACT
# ──────────────────────────────────────────────────────────────────────────────
print("\n── EXTRACT ──────────────────────────────")
users       = sqlite_to_spark(DB1_PATH, "users")
drivers     = sqlite_to_spark(DB1_PATH, "drivers")
merchants   = sqlite_to_spark(DB2_PATH, "merchants")
products    = sqlite_to_spark(DB2_PATH, "products")
orders      = sqlite_to_spark(DB2_PATH, "orders")
order_items = sqlite_to_spark(DB2_PATH, "order_items")

for name, df in [("users", users), ("drivers", drivers), ("merchants", merchants),
                 ("products", products), ("orders", orders), ("order_items", order_items)]:
    print(f"  ✓ {name}: {df.count():,} rows")

# ──────────────────────────────────────────────────────────────────────────────
# TRANSFORM
# ──────────────────────────────────────────────────────────────────────────────
print("\n── TRANSFORM ────────────────────────────")

# ── dim_date: dari rentang tanggal order ──────────────────────────────────────
orders_with_ts = orders.withColumn("order_ts", F.to_timestamp("order_time", "yyyy-MM-dd HH:mm:ss"))

dates_df = orders_with_ts.select(F.to_date("order_ts").alias("full_date")).distinct()
dim_date = dates_df.withColumn("date_id", F.date_format("full_date", "yyyyMMdd").cast("int")) \
    .withColumn("year",        F.year("full_date")) \
    .withColumn("month",       F.month("full_date")) \
    .withColumn("day",         F.dayofmonth("full_date")) \
    .withColumn("quarter",     F.quarter("full_date")) \
    .withColumn("day_of_week", F.dayofweek("full_date")) \
    .withColumn("day_name",    F.date_format("full_date", "EEEE")) \
    .withColumn("is_weekend",  (F.dayofweek("full_date").isin([1, 7])).cast("int")) \
    .withColumn("month_name",  F.date_format("full_date", "MMMM")) \
    .select("date_id", "full_date", "year", "quarter", "month", "month_name",
            "day", "day_name", "day_of_week", "is_weekend")

print(f"  ✓ dim_date: {dim_date.count()} rows")

# ── dim_user ──────────────────────────────────────────────────────────────────
dim_user = users.withColumn("dob_ts", F.to_date("date_of_birth")) \
    .withColumn("age", (F.datediff(F.current_date(), F.col("dob_ts")) / 365).cast("int")) \
    .withColumn("age_group", F.when(F.col("age") < 25, "17-24")
                               .when(F.col("age") < 35, "25-34")
                               .when(F.col("age") < 45, "35-44")
                               .when(F.col("age") < 55, "45-54")
                               .otherwise("55+")) \
    .select(
        F.col("user_id"), F.col("full_name").alias("user_name"),
        F.col("gender"), F.col("age"), F.col("age_group"),
        F.col("address_area").alias("user_area"),
        F.col("lat").alias("user_lat"), F.col("lon").alias("user_lon"),
        F.col("is_active").alias("user_is_active")
    )

print(f"  ✓ dim_user: {dim_user.count()} rows")

# ── dim_driver ────────────────────────────────────────────────────────────────
dim_driver = drivers.select(
    F.col("driver_id"), F.col("full_name").alias("driver_name"),
    F.col("vehicle_type"), F.col("current_area").alias("driver_area"),
    F.col("rating").alias("driver_rating"),
    F.col("total_trips"), F.col("is_active").alias("driver_is_active")
)
print(f"  ✓ dim_driver: {dim_driver.count()} rows")

# ── dim_merchant ──────────────────────────────────────────────────────────────
dim_merchant = merchants.select(
    F.col("merchant_id"), F.col("merchant_name"),
    F.col("category").alias("merchant_category"),
    F.col("area").alias("merchant_area"),
    F.col("lat").alias("merchant_lat"), F.col("lon").alias("merchant_lon"),
    F.col("rating").alias("merchant_rating"), F.col("is_active").alias("merchant_is_active")
)
print(f"  ✓ dim_merchant: {dim_merchant.count()} rows")

# ── dim_product ───────────────────────────────────────────────────────────────
dim_product = products.select(
    F.col("product_id"), F.col("merchant_id"),
    F.col("product_name"), F.col("category").alias("product_category"),
    F.col("price"), F.col("is_available")
)
print(f"  ✓ dim_product: {dim_product.count()} rows")

# ── fact_orders ────────────────────────────────────────────────────────────────
# Tambahkan date_id, hour, is_weekend flag, join dim_date
fact_orders = orders_with_ts \
    .withColumn("date_id",    F.date_format("order_ts", "yyyyMMdd").cast("int")) \
    .withColumn("order_hour", F.hour("order_ts")) \
    .withColumn("is_cancelled", (F.col("status").contains("cancel")).cast("int")) \
    .withColumn("is_delivered", (F.col("status") == "delivered").cast("int")) \
    .select(
        F.col("order_id"), F.col("user_id"), F.col("driver_id"), F.col("merchant_id"),
        F.col("date_id"), F.col("order_hour"),
        F.col("status").alias("order_status"),
        F.col("payment_method"),
        F.col("subtotal"), F.col("delivery_fee"), F.col("discount"),
        F.col("total_amount"),
        F.col("is_delivered"), F.col("is_cancelled"),
        F.col("delivery_area")
    )

print(f"  ✓ fact_orders: {fact_orders.count()} rows")

# ── fact_order_items (grain: per item per order) ──────────────────────────────
orders_slim = orders.select(
    F.col("order_id").alias("o_order_id"),
    F.col("user_id"), F.col("status"), F.col("order_time")
)
fact_order_items = order_items.join(orders_slim, order_items.order_id == orders_slim.o_order_id, "left") \
 .withColumn("order_ts2", F.to_timestamp("order_time", "yyyy-MM-dd HH:mm:ss")) \
 .withColumn("date_id", F.date_format("order_ts2", "yyyyMMdd").cast("int")) \
 .select(
    F.col("order_item_id"), F.col("order_id"), F.col("product_id"),
    order_items["merchant_id"], F.col("user_id"), F.col("date_id"),
    F.col("product_name"), F.col("category").alias("product_category"),
    F.col("quantity"), F.col("unit_price"),
    order_items["subtotal"].alias("item_subtotal"),
    F.col("status").alias("order_status")
)

print(f"  ✓ fact_order_items: {fact_order_items.count()} rows")

# ──────────────────────────────────────────────────────────────────────────────
# LOAD ke DWH (SQLite)
# ──────────────────────────────────────────────────────────────────────────────
print("\n── LOAD ke DWH ──────────────────────────")

def spark_to_sqlite(sdf, db_path: str, table_name: str, if_exists="replace"):
    pdf = sdf.toPandas()
    conn = sqlite3.connect(db_path)
    pdf.to_sql(table_name, conn, if_exists=if_exists, index=False)
    conn.close()
    print(f"  ✓ {table_name}: {len(pdf):,} rows → DWH")

spark_to_sqlite(dim_date,        DWH_PATH, "dim_date")
spark_to_sqlite(dim_user,        DWH_PATH, "dim_user")
spark_to_sqlite(dim_driver,      DWH_PATH, "dim_driver")
spark_to_sqlite(dim_merchant,    DWH_PATH, "dim_merchant")
spark_to_sqlite(dim_product,     DWH_PATH, "dim_product")
spark_to_sqlite(fact_orders,     DWH_PATH, "fact_orders")
spark_to_sqlite(fact_order_items, DWH_PATH, "fact_order_items")

# ─── Validasi: jalankan sample OLAP queries ───────────────────────────────────
print("\n── VALIDASI OLAP QUERIES ────────────────")
conn_dwh = sqlite3.connect(DWH_PATH)

q1 = """
SELECT dm.merchant_name, dm.merchant_category,
       SUM(fo.total_amount) AS total_revenue,
       COUNT(fo.order_id) AS total_orders
FROM fact_orders fo
JOIN dim_merchant dm ON fo.merchant_id = dm.merchant_id
WHERE fo.order_status = 'delivered'
GROUP BY dm.merchant_id
ORDER BY total_revenue DESC LIMIT 5
"""
print("\n[Q1] Top 5 Merchant by Revenue:")
r1 = pd.read_sql_query(q1, conn_dwh)
print(r1.to_string(index=False))

q2 = """
SELECT dd.is_weekend,
       CASE WHEN dd.is_weekend=1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
       COUNT(fo.order_id) AS total_orders
FROM fact_orders fo
JOIN dim_date dd ON fo.date_id = dd.date_id
WHERE fo.order_status = 'delivered'
GROUP BY dd.is_weekend
"""
print("\n[Q2] Weekday vs Weekend Orders:")
r2 = pd.read_sql_query(q2, conn_dwh)
print(r2.to_string(index=False))

q3 = """
SELECT du.age_group, foi.product_category, COUNT(*) AS total_orders
FROM fact_order_items foi
JOIN dim_user du ON foi.user_id = du.user_id
WHERE foi.order_status = 'delivered'
GROUP BY du.age_group, foi.product_category
ORDER BY du.age_group, total_orders DESC
LIMIT 15
"""
print("\n[Q3] Food Category by Age Group (top 15):")
r3 = pd.read_sql_query(q3, conn_dwh)
print(r3.to_string(index=False))

q4 = """
SELECT fo.order_hour, COUNT(*) AS total_orders
FROM fact_orders fo
WHERE fo.order_status = 'delivered'
GROUP BY fo.order_hour
ORDER BY fo.order_hour
"""
print("\n[Q4] Order Distribution by Hour:")
r4 = pd.read_sql_query(q4, conn_dwh)
print(r4.to_string(index=False))

conn_dwh.close()

spark.stop()
print("\n✅ ETL Pipeline selesai! DWH siap untuk analitik.")