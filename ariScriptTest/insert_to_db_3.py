"""
GoFood Multi-Database Setup using SQLAlchemy
- Source A (MySQL): users, drivers
- Source B (PostgreSQL): merchants, products, orders, order_items, reviews
"""

import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# Konfigurasi Path
CSV_DIR = Path("csv_output3")

# URL Koneksi Database
MYSQL_SRC_URL = "mysql+pymysql://root:@localhost:3306/source_a_uts"
PG_SRC_URL = "postgresql+psycopg2://postgres:12345@localhost:5432/source_b_uts"

def load_and_insert(csv_name, table_name, engine):
    """Memuat CSV menggunakan pandas dan insert ke database."""
    path = CSV_DIR / csv_name
    if not path.exists():
        print(f"  x File {csv_name} tidak ditemukan. Lewati.")
        return
    
    # Membaca data
    df = pd.read_csv(path)
    

    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"  ✓ {table_name}: {len(df):,} rows inserted")

# 1. Inisialisasi Engine
engine_mysql = create_engine(MYSQL_SRC_URL)
engine_pg = create_engine(PG_SRC_URL)

# ─── Setup Source A: MySQL (User & Driver Domain) ─────────────────────────────
print("=== Inserting data to Source A (MySQL) ===")
try:
    load_and_insert("users.csv", "users", engine_mysql)
    load_and_insert("drivers.csv", "drivers", engine_mysql)
except Exception as e:
    print(f"Error pada MySQL: {e}")

# ─── Setup Source B: PostgreSQL (Merchant & Order Domain) ─────────────────────
print("\n=== Inserting data to Source B (PostgreSQL) ===")
try:
    load_and_insert("merchants.csv", "merchants", engine_pg)
    load_and_insert("products.csv", "products", engine_pg)
    load_and_insert("orders.csv", "orders", engine_pg)
    load_and_insert("order_items.csv", "order_items", engine_pg)
    load_and_insert("reviews.csv", "reviews", engine_pg)
except Exception as e:
    print(f"Error pada PostgreSQL: {e}")

print("\n✅ Proses migrasi data dummy ke database selesai!")