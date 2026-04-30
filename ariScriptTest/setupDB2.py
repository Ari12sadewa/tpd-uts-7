"""
GoFood Multi-Database Setup
- DB 1 (user_driver.db): users, drivers
- DB 2 (merchant_order.db): merchants, products, orders, order_items, reviews
"""

import sqlite3
import csv
from pathlib import Path

CSV_DIR = Path("csv_output")
DB_DIR  = Path("databases")
DB_DIR.mkdir(exist_ok=True)

DB1 = DB_DIR / "user_driver.db"
DB2 = DB_DIR / "merchant_order.db"

# ─── DDL Database 1: User & Driver Domain ─────────────────────────────────────
DDL_DB1 = """
CREATE TABLE IF NOT EXISTS users (
    user_id     TEXT PRIMARY KEY,
    full_name   TEXT NOT NULL,
    email       TEXT,
    phone       TEXT,
    date_of_birth TEXT,
    gender      TEXT,
    address_area TEXT,
    lat         REAL,
    lon         REAL,
    created_at  TEXT,
    is_active   INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id   TEXT PRIMARY KEY,
    full_name   TEXT NOT NULL,
    phone       TEXT,
    vehicle_type TEXT,
    vehicle_plate TEXT,
    current_lat  REAL,
    current_lon  REAL,
    current_area TEXT,
    rating      REAL,
    total_trips INTEGER,
    joined_at   TEXT,
    is_active   INTEGER DEFAULT 1
);
"""

# ─── DDL Database 2: Merchant & Order Domain ──────────────────────────────────
DDL_DB2 = """
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   TEXT PRIMARY KEY,
    merchant_name TEXT NOT NULL,
    category      TEXT,
    address       TEXT,
    area          TEXT,
    lat           REAL,
    lon           REAL,
    phone         TEXT,
    rating        REAL,
    is_open       INTEGER,
    joined_at     TEXT,
    is_active     INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS products (
    product_id  TEXT PRIMARY KEY,
    merchant_id TEXT,
    product_name TEXT NOT NULL,
    category    TEXT,
    price       REAL,
    is_available INTEGER DEFAULT 1,
    created_at  TEXT,
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    user_id         TEXT NOT NULL,
    driver_id       TEXT,
    merchant_id     TEXT,
    status          TEXT,
    payment_method  TEXT,
    subtotal        REAL,
    delivery_fee    REAL,
    discount        REAL,
    total_amount    REAL,
    order_time      TEXT,
    delivered_time  TEXT,
    delivery_lat    REAL,
    delivery_lon    REAL,
    delivery_area   TEXT,
    FOREIGN KEY (merchant_id) REFERENCES merchants(merchant_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id      TEXT,
    product_id    TEXT,
    merchant_id   TEXT,
    product_name  TEXT,
    category      TEXT,
    quantity      INTEGER,
    unit_price    REAL,
    subtotal      REAL,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS reviews (
    review_id      TEXT PRIMARY KEY,
    order_id       TEXT,
    user_id        TEXT,
    merchant_id    TEXT,
    driver_id      TEXT,
    merchant_rating INTEGER,
    driver_rating   INTEGER,
    comment        TEXT,
    created_at     TEXT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
"""

def load_csv(filename):
    path = CSV_DIR / filename
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def bulk_insert(conn, table, rows, batch_size=500):
    if not rows:
        return
    cols = list(rows[0].keys())
    placeholders = ",".join(["?" for _ in cols])
    sql = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({placeholders})"
    cur = conn.cursor()
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        cur.executemany(sql, [
            [row[c] if row[c] != "" else None for c in cols]
            for row in batch
        ])
    conn.commit()
    print(f"  ✓ {table}: {len(rows):,} rows inserted")

# ─── Setup DB1 ────────────────────────────────────────────────────────────────
print("=== Setting up Database 1: user_driver.db ===")
conn1 = sqlite3.connect(DB1)
conn1.executescript(DDL_DB1)
conn1.commit()

bulk_insert(conn1, "users",   load_csv("users.csv"))
bulk_insert(conn1, "drivers", load_csv("drivers.csv"))
conn1.close()

# ─── Setup DB2 ────────────────────────────────────────────────────────────────
print("\n=== Setting up Database 2: merchant_order.db ===")
conn2 = sqlite3.connect(DB2)
conn2.executescript(DDL_DB2)
conn2.commit()

bulk_insert(conn2, "merchants",   load_csv("merchants.csv"))
bulk_insert(conn2, "products",    load_csv("products.csv"))
bulk_insert(conn2, "orders",      load_csv("orders.csv"))
bulk_insert(conn2, "order_items", load_csv("order_items.csv"))
bulk_insert(conn2, "reviews",     load_csv("reviews.csv"))
conn2.close()

print("\n✅ Kedua database berhasil dibuat!")
print(f"   {DB1}")
print(f"   {DB2}")