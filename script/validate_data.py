"""
validate_data.py
Validasi logika data GoFood dummy menggunakan pandas.
Jalankan setelah generate_dummy1_fixed.py selesai membuat CSV.
"""

import pandas as pd
import numpy as np
import math

OUTPUT_DIR = "csv_output3"

# ─── Load semua tabel ─────────────────────────────────────────────────────────
print("Loading CSV files...")
orders      = pd.read_csv(f"{OUTPUT_DIR}/orders.csv", parse_dates=["order_time", "delivered_time"])
order_items = pd.read_csv(f"{OUTPUT_DIR}/order_items.csv")
users       = pd.read_csv(f"{OUTPUT_DIR}/users.csv")
drivers     = pd.read_csv(f"{OUTPUT_DIR}/drivers.csv")
merchants   = pd.read_csv(f"{OUTPUT_DIR}/merchants.csv")
products    = pd.read_csv(f"{OUTPUT_DIR}/products.csv")
reviews     = pd.read_csv(f"{OUTPUT_DIR}/reviews.csv")

PASS = "✅ PASS"
FAIL = "❌ FAIL"
WARN = "⚠️  WARN"
results = []

def check(label, condition, details="", level="hard"):
    status = PASS if condition else (FAIL if level == "hard" else WARN)
    results.append({"Check": label, "Status": status, "Detail": details})
    print(f"{status}  {label}" + (f"\n         → {details}" if details else ""))

print("\n" + "="*60)
print("  GoFood Data Validation Report")
print("="*60)

# ══════════════════════════════════════════════════════════════
# 1. KELENGKAPAN DATA
# ══════════════════════════════════════════════════════════════
print("\n[1] Kelengkapan Data")

check("Orders tidak kosong",             len(orders) > 0,      f"{len(orders):,} rows")
check("Users tidak kosong",              len(users) > 0,       f"{len(users):,} rows")
check("Drivers tidak kosong",            len(drivers) > 0,     f"{len(drivers):,} rows")
check("Merchants tidak kosong",          len(merchants) > 0,   f"{len(merchants):,} rows")
check("Products tidak kosong",           len(products) > 0,    f"{len(products):,} rows")
check("Order items tidak kosong",        len(order_items) > 0, f"{len(order_items):,} rows")

# ══════════════════════════════════════════════════════════════
# 2. REFERENTIAL INTEGRITY
# ══════════════════════════════════════════════════════════════
print("\n[2] Referential Integrity (Foreign Keys)")

orphan_user     = orders[~orders["user_id"].isin(users["user_id"])]
orphan_driver   = orders[~orders["driver_id"].isin(drivers["driver_id"])]
orphan_merchant = orders[~orders["merchant_id"].isin(merchants["merchant_id"])]
orphan_oi_order = order_items[~order_items["order_id"].isin(orders["order_id"])]
orphan_oi_prod  = order_items[~order_items["product_id"].isin(products["product_id"])]
orphan_rev_ord  = reviews[~reviews["order_id"].isin(orders["order_id"])]

check("order.user_id → users",            len(orphan_user) == 0,     f"{len(orphan_user)} orphan rows")
check("order.driver_id → drivers",        len(orphan_driver) == 0,   f"{len(orphan_driver)} orphan rows")
check("order.merchant_id → merchants",    len(orphan_merchant) == 0, f"{len(orphan_merchant)} orphan rows")
check("order_items.order_id → orders",    len(orphan_oi_order) == 0, f"{len(orphan_oi_order)} orphan rows")
check("order_items.product_id → products",len(orphan_oi_prod) == 0,  f"{len(orphan_oi_prod)} orphan rows")
check("reviews.order_id → orders",        len(orphan_rev_ord) == 0,  f"{len(orphan_rev_ord)} orphan rows")

# ══════════════════════════════════════════════════════════════
# 3. LOGIKA JARAK (BUG UTAMA)
# ══════════════════════════════════════════════════════════════
print("\n[3] Logika Jarak Pengiriman")

has_dist = "distance_km" in orders.columns
check("Kolom distance_km ada di orders", has_dist)

if has_dist:
    d = orders["distance_km"].dropna()
    check("Semua jarak >= 0 km",           (d >= 0).all(),       f"min={d.min():.2f} km")
    check("Semua jarak <= 15 km (radius GoFood)",
          (d <= 15).all(),
          f"max={d.max():.2f} km, {(d > 15).sum()} order > 15km")
    check("Jarak rata-rata masuk akal (1–8 km)",
          1 <= d.mean() <= 8,
          f"mean={d.mean():.2f} km")
    check("Tidak ada jarak > 100 km (antar kota)",
          (d <= 100).all(),
          f"{(d > 100).sum()} order lintas kota terdeteksi", level="hard")

# Cek apakah delivery coords berbeda dari pickup coords (bukan copy-paste merchant)
if all(c in orders.columns for c in ["delivery_lat","delivery_lon","pickup_lat","pickup_lon"]):
    same_coords = (
        (orders["delivery_lat"] == orders["pickup_lat"]) &
        (orders["delivery_lon"] == orders["pickup_lon"])
    ).sum()
    check("delivery_lat/lon ≠ pickup_lat/lon (bukan merchant coords)",
          same_coords == 0,
          f"{same_coords} baris dengan koordinat identik")

# ══════════════════════════════════════════════════════════════
# 4. KONSISTENSI KOTA (TIDAK CROSS-CITY)
# ══════════════════════════════════════════════════════════════
print("\n[4] Konsistensi Kota (Anti Cross-City)")

if "city" in orders.columns:
    # Gabungkan kota user ke order
    ord_user = orders.merge(users[["user_id","city"]], on="user_id", how="left", suffixes=("","_user"))
    ord_merch = ord_user.merge(merchants[["merchant_id","city"]], on="merchant_id", how="left", suffixes=("","_merchant"))

    cross_city = ord_merch["city"] != ord_merch["city_merchant"]
    check("Kota order = kota merchant",
          cross_city.sum() == 0,
          f"{cross_city.sum()} order lintas kota (user ≠ merchant city)")

    cross_user = ord_merch["city"] != ord_merch["city_user"]
    check("Kota order = kota user",
          cross_user.sum() == 0,
          f"{cross_user.sum()} order dimana user dari kota berbeda")

# ══════════════════════════════════════════════════════════════
# 5. LOGIKA FINANSIAL
# ══════════════════════════════════════════════════════════════
print("\n[5] Logika Finansial")

check("Semua harga produk > 0",
      (products["price"] > 0).all(),
      f"{(products['price'] <= 0).sum()} produk harga nol/negatif")

check("delivery_fee dalam range wajar (Rp 2.000 – Rp 25.000)",
      orders["delivery_fee"].between(2000, 25000).all(),
      f"min=Rp{orders['delivery_fee'].min():,}  max=Rp{orders['delivery_fee'].max():,}")

check("total_amount >= 0",
      (orders["total_amount"] >= 0).all(),
      f"{(orders['total_amount'] < 0).sum()} order dengan total negatif")

check("subtotal > 0 untuk semua order",
      (orders["subtotal"] > 0).all(),
      f"{(orders['subtotal'] <= 0).sum()} order dengan subtotal nol/negatif")

# Verifikasi: subtotal order_items harus sejalan dengan order.subtotal
oi_sum = order_items.groupby("order_id")["subtotal"].sum().reset_index()
oi_sum.columns = ["order_id", "oi_subtotal"]
merged = orders.merge(oi_sum, on="order_id", how="left")
mismatch = (merged["subtotal"] != merged["oi_subtotal"]).sum()
check("order.subtotal = sum(order_items.subtotal)",
      mismatch == 0,
      f"{mismatch} order dengan subtotal tidak cocok")

# total_amount = subtotal + delivery_fee - discount
calc_total = orders["subtotal"] + orders["delivery_fee"] - orders["discount"]
total_diff = (orders["total_amount"] != calc_total.clip(lower=0)).sum()
check("total_amount = subtotal + delivery_fee - discount",
      total_diff == 0,
      f"{total_diff} order dengan kalkulasi total tidak konsisten")

# ══════════════════════════════════════════════════════════════
# 6. LOGIKA WAKTU
# ══════════════════════════════════════════════════════════════
print("\n[6] Logika Waktu")

check("order_time dalam range 2023–2024",
      orders["order_time"].between("2023-01-01","2024-12-31").all(),
      f"min={orders['order_time'].min().date()}  max={orders['order_time'].max().date()}")

delivered = orders[orders["status"] == "delivered"].copy()
delivered["duration_min"] = (
    delivered["delivered_time"] - delivered["order_time"]
).dt.total_seconds() / 60

check("Semua delivered order punya delivered_time",
      delivered["delivered_time"].notna().all(),
      f"{delivered['delivered_time'].isna().sum()} baris tanpa waktu")

check("Durasi delivery 10–120 menit (masuk akal)",
      delivered["duration_min"].between(10, 120).all(),
      f"min={delivered['duration_min'].min():.1f}  max={delivered['duration_min'].max():.1f}  mean={delivered['duration_min'].mean():.1f} menit")

check("delivered_time > order_time",
      (delivered["delivered_time"] > delivered["order_time"]).all(),
      f"{(delivered['delivered_time'] <= delivered['order_time']).sum()} baris tidak logis")

cancelled = orders[orders["status"].str.startswith("cancelled")]
check("Order cancelled tidak punya delivered_time",
      cancelled["delivered_time"].isna().all(),
      f"{cancelled['delivered_time'].notna().sum()} cancelled order dengan delivered_time terisi")

# ══════════════════════════════════════════════════════════════
# 7. LOGIKA STATUS & RATING
# ══════════════════════════════════════════════════════════════
print("\n[7] Status & Rating")

valid_statuses = {"delivered","cancelled_by_customer","cancelled_by_driver"}
invalid_status = ~orders["status"].isin(valid_statuses)
check("Semua order status valid",
      invalid_status.sum() == 0,
      f"{invalid_status.sum()} baris status tidak dikenal")

check("Merchant rating 3.5–5.0",
      merchants["rating"].between(3.5, 5.0).all(),
      f"min={merchants['rating'].min()}  max={merchants['rating'].max()}")

check("Driver rating 3.5–5.0",
      drivers["rating"].between(3.5, 5.0).all(),
      f"min={drivers['rating'].min()}  max={drivers['rating'].max()}")

check("Review merchant_rating antara 1–5",
      reviews["merchant_rating"].between(1, 5).all(),
      f"min={reviews['merchant_rating'].min()}  max={reviews['merchant_rating'].max()}")

check("Review hanya untuk order delivered",
      reviews["order_id"].isin(orders[orders["status"]=="delivered"]["order_id"]).all(),
      f"{(~reviews['order_id'].isin(orders[orders['status']=='delivered']['order_id'])).sum()} review dari order non-delivered")

# ══════════════════════════════════════════════════════════════
# 8. KELENGKAPAN KOLOM (NULL CHECK)
# ══════════════════════════════════════════════════════════════
print("\n[8] Null Check pada Kolom Kritis")

critical_cols = {
    "orders":      ["order_id","user_id","driver_id","merchant_id","status","total_amount","order_time"],
    "users":       ["user_id","full_name","email","phone","lat","lon"],
    "drivers":     ["driver_id","full_name","phone","vehicle_type"],
    "merchants":   ["merchant_id","merchant_name","lat","lon"],
    "products":    ["product_id","merchant_id","product_name","price"],
    "order_items": ["order_item_id","order_id","product_id","quantity","unit_price"],
}
tables = {"orders":orders,"users":users,"drivers":drivers,
          "merchants":merchants,"products":products,"order_items":order_items}

for tbl, cols in critical_cols.items():
    df = tables[tbl]
    for col in cols:
        if col in df.columns:
            nulls = df[col].isna().sum()
            check(f"{tbl}.{col} tidak null",
                  nulls == 0, f"{nulls} null values")

# ══════════════════════════════════════════════════════════════
# RINGKASAN
# ══════════════════════════════════════════════════════════════
print("\n" + "="*60)
total   = len(results)
passed  = sum(1 for r in results if r["Status"] == PASS)
failed  = sum(1 for r in results if r["Status"] == FAIL)
warned  = sum(1 for r in results if r["Status"] == WARN)

print(f"  TOTAL CHECKS : {total}")
print(f"  {PASS}       : {passed}")
print(f"  {WARN}       : {warned}")
print(f"  {FAIL}       : {failed}")
print("="*60)

if failed == 0:
    print("\n🎉 Data lulus semua validasi! Siap untuk analisis.")
else:
    print(f"\n⚠️  Ada {failed} check yang gagal. Periksa generate_dummy1_fixed.py.")

# Export ringkasan ke CSV
pd.DataFrame(results).to_csv(f"{OUTPUT_DIR}/validation_report.csv", index=False)
print(f"\nLaporan disimpan di: {OUTPUT_DIR}/validation_report.csv")