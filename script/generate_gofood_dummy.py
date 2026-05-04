"""
=============================================================
  GoFood Data Pipeline - Dummy Transaction Generator
  UTS Teknologi Perekayasaan Data
  Kelompok 7 - 3SI2
=============================================================

CARA PAKAI:
  1. Download dataset dari Kaggle:
     - https://www.kaggle.com/datasets/iannarsa/gofood-merchant-on-yogyakarta
     - Simpan file CSV-nya di folder yang sama dengan script ini
  2. Ubah variabel KAGGLE_FILE sesuai nama file CSV yang didownload
  3. Jalankan: python generate_gofood_dummy.py
  4. Output: beberapa file CSV siap dipakai untuk pipeline ETL

Kalau belum punya file Kaggle, script ini otomatis pakai
sample merchant data bawaan (built-in fallback).
=============================================================
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker('id_ID')
np.random.seed(42)
random.seed(42)

# ============================================================
# KONFIGURASI
# ============================================================
KAGGLE_FILE = "dataset/gofood_merchant.csv"   # ganti sesuai nama file CSV kamu
N_TRANSACTIONS = 15000   # jumlah transaksi yang digenerate
N_CUSTOMERS    = 1000    # jumlah customer unik
START_DATE     = datetime(2024, 1, 1)
END_DATE       = datetime(2024, 12, 31)
OUTPUT_DIR     = "gofood_pipeline_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# STEP 1 — LOAD / BUAT DATA MERCHANT
# ============================================================
print("=" * 60)
print("  GoFood Dummy Data Generator")
print("=" * 60)

CATEGORIES = [
    "Ayam & Unggas", "Nasi & Lauk", "Mie & Pasta", "Seafood",
    "Burger & Sandwich", "Pizza", "Minuman & Jus", "Kopi & Cafe",
    "Dessert & Snack", "Vegetarian", "Sushi & Japanese", "Bakso & Soto"
]

AREAS = [
    "Sleman", "Bantul", "Kota Yogyakarta", "Godean",
    "Mlati", "Depok", "Umbulharjo", "Gondokusuman"
]

PAYMENT_METHODS = ["GoPay", "GoPay", "GoPay", "Kartu Kredit", "Transfer Bank", "Tunai"]
# GoPay lebih sering dipilih (realistis)

ORDER_STATUSES = ["Selesai", "Selesai", "Selesai", "Selesai",
                  "Dibatalkan", "Dalam Pengiriman"]

def build_fallback_merchants():
    """Buat sample merchant GoFood Yogyakarta kalau file Kaggle belum ada."""
    merchant_names = [
        "Ayam Geprek Pak Ndut", "Warung Nasi Padang Bu Tini",
        "Mie Ayam Bakso Pak Joko", "Seafood 99 Jogja",
        "Burger Kuy Station", "Pizza Hut Yogyakarta",
        "Es Teh Indonesia Jogja", "Kopi Kenangan Malioboro",
        "Mixue Ice Cream UGM", "Ayam Bakar Wong Solo",
        "Soto Lamongan Cak Har", "Warung Makan Bu Dhe",
        "Geprek Bensu Jogja", "Sabana Fried Chicken",
        "Hokben Jogja City Mall", "Sushi Tei Ambarrukmo",
        "McDonald's Ring Road", "KFC Gejayan",
        "Indomaret Point Kafe", "Lontong Cap Go Meh",
        "Sate Pak Jono Pathuk", "Gudeg Yu Djum",
        "Bakmi Jawa Pak Rebo", "Angkringan Lik Man",
        "Warmindo Abadi", "Steak 21 Jogja",
        "Martabak Mas Fuad", "Batagor Kingsley",
        "Rujak Cingur Bu Nah", "Pecel Lele Lela"
    ]
    records = []
    for i, name in enumerate(merchant_names):
        cat  = random.choice(CATEGORIES)
        area = random.choice(AREAS)
        records.append({
            "merchant_id"    : f"MRC{i+1:04d}",
            "merchant_name"  : name,
            "category"       : cat,
            "area"           : area,
            "rating"         : round(random.uniform(3.5, 5.0), 1),
            "total_reviews"  : random.randint(50, 5000),
            "min_order"      : random.choice([0, 10000, 15000, 20000]),
            "delivery_fee"   : random.choice([2000, 3000, 5000, 7000, 9000]),
            "is_open_24h"    : random.choice([True, False]),
            "latitude"       : round(random.uniform(-7.85, -7.70), 6),
            "longitude"      : round(random.uniform(110.30, 110.50), 6),
            "source"         : "built_in_sample"
        })
    return pd.DataFrame(records)

def parse_ratings(rating_str):
    """
    Parse kolom 'ratings' dari format string dict Kaggle.
    Contoh input: "{'average': 4.6, 'total': 0}"
    Output: (avg_float, total_int)
    """
    try:
        import ast
        parsed = ast.literal_eval(str(rating_str))
        avg   = float(parsed.get("average", 0))
        total = int(parsed.get("total", 0))
        return avg, total
    except Exception:
        return round(random.uniform(3.5, 5.0), 1), random.randint(50, 500)

def parse_tags_to_category(tags_str):
    """
    Extract kategori dari kolom 'tags' Kaggle.
    Contoh input: "['Ayam', 'Nasi', 'Indonesia']"
    """
    tag_to_cat = {
        "ayam"      : "Ayam & Unggas",
        "unggas"    : "Ayam & Unggas",
        "chicken"   : "Ayam & Unggas",
        "nasi"      : "Nasi & Lauk",
        "rice"      : "Nasi & Lauk",
        "indonesia" : "Nasi & Lauk",
        "lauk"      : "Nasi & Lauk",
        "mie"       : "Mie & Pasta",
        "bakmi"     : "Mie & Pasta",
        "pasta"     : "Mie & Pasta",
        "seafood"   : "Seafood",
        "ikan"      : "Seafood",
        "udang"     : "Seafood",
        "burger"    : "Burger & Sandwich",
        "sandwich"  : "Burger & Sandwich",
        "pizza"     : "Pizza",
        "minuman"   : "Minuman & Jus",
        "jus"       : "Minuman & Jus",
        "juice"     : "Minuman & Jus",
        "kopi"      : "Kopi & Cafe",
        "coffee"    : "Kopi & Cafe",
        "cafe"      : "Kopi & Cafe",
        "dessert"   : "Dessert & Snack",
        "snack"     : "Dessert & Snack",
        "ice cream" : "Dessert & Snack",
        "vegetarian": "Vegetarian",
        "vegan"     : "Vegetarian",
        "sushi"     : "Sushi & Japanese",
        "japanese"  : "Sushi & Japanese",
        "jepang"    : "Sushi & Japanese",
        "bakso"     : "Bakso & Soto",
        "soto"      : "Bakso & Soto",
    }
    try:
        tags_lower = str(tags_str).lower()
        for keyword, cat in tag_to_cat.items():
            if keyword in tags_lower:
                return cat
    except Exception:
        pass
    return random.choice(CATEGORIES)

def parse_open_periods(periods_str):
    """
    Cek apakah merchant buka 24 jam dari kolom openPeriods.
    Kalau startTime hours=0 dan endTime hours=23 -> anggap 24 jam.
    """
    try:
        s = str(periods_str).lower()
        if "'hours': 0" in s and "'hours': 23" in s:
            return True
        return False
    except Exception:
        return False

def extract_area(city_str):
    """Normalisasi kolom city ke area Yogyakarta."""
    yogya_areas = {
        "sleman"       : "Sleman",
        "bantul"       : "Bantul",
        "yogyakarta"   : "Kota Yogyakarta",
        "godean"       : "Godean",
        "mlati"        : "Mlati",
        "depok"        : "Depok",
        "umbulharjo"   : "Umbulharjo",
        "gondokusuman" : "Gondokusuman",
        "gamping"      : "Gamping",
        "ngaglik"      : "Ngaglik",
        "kalasan"      : "Kalasan",
    }
    try:
        city_lower = str(city_str).lower().strip()
        for key, val in yogya_areas.items():
            if key in city_lower:
                return val
        if city_lower not in ["nan", "none", ""]:
            return str(city_str).strip().title()
    except Exception:
        pass
    return random.choice(AREAS)

def load_kaggle_merchants(filepath):
    """
    Load merchant dari file Kaggle GoFood Merchant Yogyakarta.
    Kolom asli dataset:
      uid, ratings, priceLevel, displayName, description, status,
      openPeriods, createTime, notes, tags, location.latitude,
      location.longitude, brand.displayName, nextCloseTime, postcode, city
    """
    df = pd.read_csv(filepath)
    print(f"  File Kaggle ditemukan! Shape: {df.shape}")
    print(f"  Kolom asli: {list(df.columns)}")

    result = []
    for i, row in df.iterrows():

        # merchant_id: dari uid Kaggle, diformat ulang
        uid = str(row.get("uid", f"MRC{i+1:04d}"))
        merchant_id = "MRC" + uid.replace("-", "")[:8].upper()

        # merchant_name: dari displayName
        merchant_name = str(row.get("displayName", f"Merchant {i+1}")).strip()

        # rating & total_reviews: parse dari kolom 'ratings' (nested dict string)
        rating_avg, total_reviews = parse_ratings(row.get("ratings", "{}"))
        if rating_avg == 0:
            rating_avg = round(random.uniform(3.5, 5.0), 1)
        if total_reviews == 0:
            total_reviews = random.randint(50, 3000)

        # price_level: langsung dari priceLevel (1/2/3)
        try:
            price_level = int(row.get("priceLevel", 1))
        except Exception:
            price_level = random.choice([1, 2, 3])

        # category: extract dari tags
        category = parse_tags_to_category(row.get("tags", ""))

        # is_open_24h: dari openPeriods
        is_open_24h = parse_open_periods(row.get("openPeriods", ""))

        # area: normalisasi dari kolom city
        area = extract_area(row.get("city", ""))

        # koordinat GPS
        try:
            lat = float(row.get("location.latitude", -7.80))
        except Exception:
            lat = round(random.uniform(-7.85, -7.70), 6)
        try:
            lon = float(row.get("location.longitude", 110.36))
        except Exception:
            lon = round(random.uniform(110.30, 110.50), 6)

        # brand
        brand = str(row.get("brand.displayName", "")).strip()
        brand = brand if brand not in ["nan", "None", ""] else None

        # delivery_fee & min_order: estimasi dari priceLevel
        delivery_fee = {1: 3000, 2: 5000, 3: 9000}.get(price_level, 5000)
        min_order    = {1: 0,    2: 10000, 3: 20000}.get(price_level, 0)

        # status aktif: dari kolom status
        try:
            is_active = str(row.get("status", "1")).strip() == "1"
        except Exception:
            is_active = True

        result.append({
            "merchant_id"   : merchant_id,
            "merchant_name" : merchant_name,
            "category"      : category,
            "area"          : area,
            "rating"        : rating_avg,
            "total_reviews" : total_reviews,
            "price_level"   : price_level,
            "min_order"     : min_order,
            "delivery_fee"  : delivery_fee,
            "is_open_24h"   : is_open_24h,
            "is_active"     : is_active,
            "latitude"      : lat,
            "longitude"     : lon,
            "brand"         : brand,
            "source"        : "kaggle"
        })

    df_out = pd.DataFrame(result)
    df_out = df_out.drop_duplicates(subset="merchant_id").reset_index(drop=True)
    print(f"  Parsing selesai: {len(df_out)} merchant valid")
    print(f"  Distribusi kategori:\n{df_out['category'].value_counts().to_string()}")
    return df_out

# Load merchant
if os.path.exists(KAGGLE_FILE):
    print(f"\n[1/5] Memuat data merchant dari Kaggle: {KAGGLE_FILE}")
    df_merchant = load_kaggle_merchants(KAGGLE_FILE)
else:
    print(f"\n[1/5] File Kaggle '{KAGGLE_FILE}' tidak ditemukan.")
    print("       Menggunakan built-in sample merchant GoFood Yogyakarta...")
    df_merchant = build_fallback_merchants()

print(f"       Total merchant: {len(df_merchant)}")

# ============================================================
# STEP 2 — BUAT DATA CUSTOMER (DIM)
# ============================================================
print(f"\n[2/5] Generate data customer ({N_CUSTOMERS} customer)...")

genders    = ["Laki-laki", "Perempuan"]
age_groups = ["18-24", "25-34", "35-44", "45-54", "55+"]
city_list  = ["Yogyakarta", "Sleman", "Bantul", "Gunung Kidul", "Kulon Progo"]

customers = []
for i in range(N_CUSTOMERS):
    gender  = random.choice(genders)
    name    = fake.name_male() if gender == "Laki-laki" else fake.name_female()
    customers.append({
        "customer_id"   : f"CST{i+1:05d}",
        "customer_name" : name,
        "gender"        : gender,
        "age_group"     : random.choice(age_groups),
        "city"          : random.choice(city_list),
        "email"         : fake.email(),
        "phone"         : fake.phone_number(),
        "join_date"     : fake.date_between(start_date="-3y", end_date="-1m"),
        "loyalty_tier"  : random.choices(
                            ["Bronze", "Silver", "Gold", "Platinum"],
                            weights=[50, 30, 15, 5])[0]
    })

df_customer = pd.DataFrame(customers)
print(f"       Done: {len(df_customer)} customers")

# ============================================================
# STEP 3 — BUAT DATA DRIVER (DIM)
# ============================================================
print(f"\n[3/5] Generate data driver (200 driver)...")

N_DRIVERS = 200
drivers = []
for i in range(N_DRIVERS):
    join = fake.date_between(start_date="-4y", end_date="-6m")
    drivers.append({
        "driver_id"        : f"DRV{i+1:04d}",
        "driver_name"      : fake.name_male(),
        "phone"            : fake.phone_number(),
        "vehicle_type"     : random.choices(["Motor", "Motor", "Motor", "Mobil"], weights=[85,5,5,5])[0],
        "vehicle_plate"    : f"AB {random.randint(1000,9999)} {random.choice('ABCDEFGH')}",
        "join_date"        : join,
        "rating_avg"       : round(random.uniform(3.8, 5.0), 2),
        "total_deliveries" : random.randint(100, 8000),
        "area_base"        : random.choice(AREAS),
        "is_active"        : random.choices([True, False], weights=[90, 10])[0]
    })

df_driver = pd.DataFrame(drivers)
print(f"       Done: {len(df_driver)} drivers")

# ============================================================
# STEP 4 — BUAT DATA PRODUK / MENU (DIM)
# ============================================================
print(f"\n[4/5] Generate data produk menu...")

menu_items_by_cat = {
    "Ayam & Unggas"     : ["Ayam Geprek", "Ayam Bakar", "Ayam Goreng Kremes", "Chicken Wings"],
    "Nasi & Lauk"       : ["Nasi Gudeg", "Nasi Padang", "Nasi Campur", "Nasi Goreng Spesial"],
    "Mie & Pasta"       : ["Mie Ayam Original", "Bakmi Jawa", "Mie Goreng Seafood", "Spaghetti"],
    "Seafood"           : ["Udang Goreng Mentega", "Ikan Bakar", "Cumi Saus Tiram", "Kepiting"],
    "Burger & Sandwich" : ["Burger Beef Double", "Chicken Burger", "Club Sandwich", "Hotdog"],
    "Pizza"             : ["Pizza Pepperoni", "Pizza Margherita", "Pizza BBQ Chicken"],
    "Minuman & Jus"     : ["Jus Alpukat", "Es Teh Manis", "Jus Mangga", "Air Mineral"],
    "Kopi & Cafe"       : ["Kopi Susu", "Americano", "Cappuccino", "Matcha Latte"],
    "Dessert & Snack"   : ["Es Krim Coklat", "Brownies", "Pisang Goreng", "Martabak Manis"],
    "Vegetarian"        : ["Gado-Gado", "Pecel Sayur", "Tempe Mendoan", "Tahu Bacem"],
    "Sushi & Japanese"  : ["Salmon Sushi", "California Roll", "Ramen Tonkotsu", "Gyoza"],
    "Bakso & Soto"      : ["Bakso Spesial", "Soto Lamongan", "Soto Betawi", "Bakso Mercon"]
}

price_range = {
    "Ayam & Unggas": (15000, 45000), "Nasi & Lauk": (12000, 40000),
    "Mie & Pasta": (12000, 45000),   "Seafood": (35000, 150000),
    "Burger & Sandwich": (20000, 60000), "Pizza": (45000, 120000),
    "Minuman & Jus": (5000, 20000),  "Kopi & Cafe": (18000, 45000),
    "Dessert & Snack": (8000, 35000),"Vegetarian": (10000, 30000),
    "Sushi & Japanese": (25000, 85000), "Bakso & Soto": (12000, 35000)
}

products = []
prod_id  = 1
for _, merch in df_merchant.iterrows():
    cat   = merch["category"]
    items = menu_items_by_cat.get(cat, ["Menu Spesial", "Menu Pilihan"])
    lo, hi = price_range.get(cat, (10000, 50000))
    for item in items:
        products.append({
            "product_id"   : f"PRD{prod_id:05d}",
            "merchant_id"  : merch["merchant_id"],
            "product_name" : item,
            "category"     : cat,
            "price"        : random.randrange(lo, hi, 1000),
            "is_available" : random.choices([True, False], weights=[90, 10])[0],
            "is_bestseller": random.choices([True, False], weights=[25, 75])[0]
        })
        prod_id += 1

df_product = pd.DataFrame(products)
print(f"       Done: {len(df_product)} produk menu")

# ============================================================
# STEP 5 — GENERATE TRANSAKSI (FACT TABLE)
# ============================================================
print(f"\n[5/5] Generate {N_TRANSACTIONS} transaksi...")

merchant_ids = df_merchant["merchant_id"].tolist()
customer_ids = df_customer["customer_id"].tolist()
driver_ids   = df_driver["driver_id"].tolist()

# Bobot merchant: beberapa merchant lebih populer (pareto)
merchant_weights = np.random.pareto(1.5, len(merchant_ids)) + 1
merchant_weights = merchant_weights / merchant_weights.sum()

# Simulasi jam order yang realistis (peak: siang & malam)
def random_order_time(base_date):
    hour_weights = [
        1,1,1,1,1,2,      # 00-05: sepi
        3,5,6,7,8,10,     # 06-11: naik
        15,12,10,8,7,8,   # 12-17: peak siang
        14,16,12,10,8,5   # 18-23: peak malam
    ]
    hour   = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return base_date.replace(hour=hour, minute=minute, second=second)

transactions = []
date_range   = (END_DATE - START_DATE).days

for i in range(N_TRANSACTIONS):
    order_date   = START_DATE + timedelta(days=random.randint(0, date_range))
    order_time   = random_order_time(order_date)

    merch_id     = random.choices(merchant_ids, weights=merchant_weights)[0]
    merch_row    = df_merchant[df_merchant["merchant_id"] == merch_id].iloc[0]

    # Produk dari merchant ini
    merch_prods  = df_product[df_product["merchant_id"] == merch_id]
    if len(merch_prods) == 0:
        continue

    n_items      = random.randint(1, 4)
    sampled_prod = merch_prods.sample(min(n_items, len(merch_prods)), replace=True)
    quantities   = [random.randint(1, 3) for _ in range(len(sampled_prod))]
    subtotal     = sum(row["price"] * qty for (_, row), qty
                       in zip(sampled_prod.iterrows(), quantities))

    delivery_fee  = int(merch_row["delivery_fee"])
    discount      = random.choices([0, 5000, 10000, 15000], weights=[60,20,15,5])[0]
    total_payment = max(0, subtotal + delivery_fee - discount)

    status = random.choices(
        ORDER_STATUSES,
        weights=[60, 10, 5, 5, 12, 8]
    )[0]

    duration_est    = random.randint(15, 60)
    duration_actual = duration_est + random.randint(-5, 20) if status == "Selesai" else None
    rating_order    = round(random.uniform(3.0, 5.0), 1) if status == "Selesai" else None
    rating_driver   = round(random.uniform(3.5, 5.0), 1) if status == "Selesai" else None

    transactions.append({
        "transaction_id"    : f"TRX{i+1:07d}",
        "order_datetime"    : order_time.strftime("%Y-%m-%d %H:%M:%S"),
        "order_date"        : order_time.strftime("%Y-%m-%d"),
        "order_hour"        : order_time.hour,
        "customer_id"       : random.choice(customer_ids),
        "merchant_id"       : merch_id,
        "driver_id"         : random.choice(driver_ids) if status != "Dibatalkan" else None,
        "product_ids"       : ",".join(sampled_prod["product_id"].tolist()),
        "product_names"     : ",".join(sampled_prod["product_name"].tolist()),
        "n_items"           : len(sampled_prod),
        "subtotal"          : subtotal,
        "delivery_fee"      : delivery_fee,
        "discount"          : discount,
        "total_payment"     : total_payment,
        "payment_method"    : random.choice(PAYMENT_METHODS),
        "order_status"      : status,
        "duration_est_min"  : duration_est,
        "duration_actual_min": duration_actual,
        "rating_merchant"   : rating_order,
        "rating_driver"     : rating_driver,
        "is_weekend"        : order_time.weekday() >= 5,
        "merchant_category" : merch_row["category"],
        "merchant_area"     : merch_row["area"]
    })

df_transaction = pd.DataFrame(transactions)
print(f"       Done: {len(df_transaction)} transaksi")

# ============================================================
# STEP 6 — BUAT TABEL WAKTU (DIM)
# ============================================================
print("\n[6/6] Membuat dimensi waktu...")

all_dates = pd.date_range(start=START_DATE, end=END_DATE, freq="D")
month_id  = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"Mei",6:"Jun",
             7:"Jul",8:"Agt",9:"Sep",10:"Okt",11:"Nov",12:"Des"}
quarter_map = {1:1,2:1,3:1,4:2,5:2,6:2,7:3,8:3,9:3,10:4,11:4,12:4}

dim_time = pd.DataFrame({
    "date_id"     : [d.strftime("%Y%m%d") for d in all_dates],
    "full_date"   : [d.strftime("%Y-%m-%d") for d in all_dates],
    "day"         : [d.day for d in all_dates],
    "month"       : [d.month for d in all_dates],
    "month_name"  : [month_id[d.month] for d in all_dates],
    "quarter"     : [quarter_map[d.month] for d in all_dates],
    "year"        : [d.year for d in all_dates],
    "day_of_week" : [d.strftime("%A") for d in all_dates],
    "is_weekend"  : [d.weekday() >= 5 for d in all_dates],
    "is_holiday"  : False   # bisa diisi manual untuk hari libur nasional
})

# Tandai hari libur nasional 2024 (Indonesia)
holidays_2024 = [
    "2024-01-01","2024-02-08","2024-02-09","2024-02-10",
    "2024-03-11","2024-03-29","2024-04-10","2024-04-11",
    "2024-04-12","2024-05-01","2024-05-09","2024-05-23",
    "2024-06-01","2024-06-17","2024-06-18","2024-08-17",
    "2024-09-16","2024-12-25","2024-12-26"
]
dim_time.loc[dim_time["full_date"].isin(holidays_2024), "is_holiday"] = True
print(f"       Done: {len(dim_time)} hari (dengan {len(holidays_2024)} hari libur nasional)")

# ============================================================
# SIMPAN SEMUA CSV
# ============================================================
print("\n" + "="*60)
print("  Menyimpan file CSV...")
print("="*60)

files = {
    "dim_merchant.csv"   : df_merchant,
    "dim_customer.csv"   : df_customer,
    "dim_driver.csv"     : df_driver,
    "dim_product.csv"    : df_product,
    "dim_time.csv"       : dim_time,
    "fact_transaction.csv": df_transaction
}

for fname, df in files.items():
    path = os.path.join(OUTPUT_DIR, fname)
    df.to_csv(path, index=False)
    print(f"  ✓ {fname:<25} → {len(df):>6} baris  ({os.path.getsize(path)/1024:.1f} KB)")

# ============================================================
# RINGKASAN
# ============================================================
print("\n" + "="*60)
print("  RINGKASAN DATASET")
print("="*60)
print(f"  Periode data  : {START_DATE.date()} s/d {END_DATE.date()}")
print(f"  Total transaksi  : {len(df_transaction):,}")
print(f"  Total merchant   : {len(df_merchant):,}")
print(f"  Total customer   : {len(df_customer):,}")
print(f"  Total driver     : {len(df_driver):,}")
print(f"  Total produk     : {len(df_product):,}")
selesai = df_transaction[df_transaction["order_status"]=="Selesai"]
print(f"  Transaksi selesai: {len(selesai):,} ({len(selesai)/len(df_transaction)*100:.1f}%)")
print(f"  Total revenue    : Rp {df_transaction[df_transaction['order_status']=='Selesai']['total_payment'].sum():,.0f}")
print(f"\n  File tersimpan di folder: ./{OUTPUT_DIR}/")
print("="*60)
print("\n  STRUKTUR DATA WAREHOUSE:")
print("  fact_transaction  ─┬─ dim_merchant  (merchant_id)")
print("                     ├─ dim_customer  (customer_id)")
print("                     ├─ dim_driver    (driver_id)")
print("                     ├─ dim_product   (product_id)")
print("                     └─ dim_time      (order_date)")
print("\n  Siap untuk proses ETL! ✓")
print("="*60)
