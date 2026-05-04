"""
GoFood Multi-Database Setup using SQLAlchemy
- Source A (MySQL): users, drivers
- Source B (PostgreSQL): merchants, products, orders, order_items, reviews

Behaviour: jika tabel sudah ada → DELETE semua data terlebih dahulu,
           lalu INSERT ulang dari CSV.
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect

from pathlib import Path

# ──────────────────────────────────────────────────────────────────────────────
# KONFIGURASI
# ──────────────────────────────────────────────────────────────────────────────
CSV_DIR       = Path("csv_output_fixed")
MYSQL_SRC_URL = "mysql+pymysql://root:@localhost:3306/source_a_uts"
PG_SRC_URL    = "postgresql+psycopg2://postgres:12345@localhost:5432/source_b_uts"


# ──────────────────────────────────────────────────────────────────────────────
# HELPER
# ──────────────────────────────────────────────────────────────────────────────
def load_and_insert(csv_name: str, table_name: str, engine) -> None:
    """
    1. Cek apakah file CSV ada.
    2. Cek apakah tabel sudah ada di database.
       - Jika ada  → DELETE semua baris, lalu INSERT dari CSV.
       - Jika belum → langsung INSERT (pandas akan membuat tabel baru).
    """
    path = CSV_DIR / csv_name
    if not path.exists():
        print(f"  ✗ File '{csv_name}' tidak ditemukan. Lewati.")
        return

    df = pd.read_csv(path)

    inspector       = inspect(engine)
    table_exists    = table_name in inspector.get_table_names()

    with engine.begin() as conn:           # begin() → auto-commit / rollback
        if table_exists:
            deleted = conn.execute(text(f"DELETE FROM {table_name}"))
            print(f"  ⚑ Tabel '{table_name}' sudah ada → "
                  f"{deleted.rowcount:,} baris dihapus")
        else:
            print(f"  + Tabel '{table_name}' belum ada → akan dibuat otomatis")

    # INSERT — if_exists='append' agar struktur tabel yang sudah ada dipertahankan;
    # pandas membuat tabel baru jika belum ada.
    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"  ✓ '{table_name}': {len(df):,} baris di-insert\n")


# ──────────────────────────────────────────────────────────────────────────────
# EKSEKUSI
# ──────────────────────────────────────────────────────────────────────────────
engine_mysql = create_engine(MYSQL_SRC_URL)
engine_pg    = create_engine(PG_SRC_URL)

# ─── Source A: MySQL ──────────────────────────────────────────────────────────
print("=" * 55)
print("  Source A — MySQL")
print("=" * 55)
try:
    load_and_insert("users.csv",   "users",   engine_mysql)
    load_and_insert("drivers.csv", "drivers", engine_mysql)
except Exception as e:
    print(f"  ERROR MySQL: {e}")

# ─── Source B: PostgreSQL ─────────────────────────────────────────────────────
print("=" * 55)
print("  Source B — PostgreSQL")
print("=" * 55)
try:
    load_and_insert("merchants.csv",   "merchants",   engine_pg)
    load_and_insert("products.csv",    "products",    engine_pg)
    load_and_insert("orders.csv",      "orders",      engine_pg)
    load_and_insert("order_items.csv", "order_items", engine_pg)
    load_and_insert("reviews.csv",     "reviews",     engine_pg)
except Exception as e:
    print(f"  ERROR PostgreSQL: {e}")

print("=" * 55)
print("  ✅ Migrasi data selesai!")
print("=" * 55)


