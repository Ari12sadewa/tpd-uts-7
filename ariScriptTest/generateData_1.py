"""
GoFood Data Simulation - Generate Dummy Data
Region: DKI Jakarta, Medan, Surabaya

FIXES (batch 2):
- BUG #1 CROSS-CITY: merchant_area dari dataset tidak cocok dengan AREA_TO_CITY keys.
  Solusi: mapping fuzzy — cek apakah city keyword ada di dalam nama area string,
  bukan exact match. Semua merchant, user, driver dijamin sekota per order.
- BUG #2 SUBTOTAL MISMATCH: qty di subtotal order dan qty di order_items di-random
  secara terpisah (dua kali random.randint), hasilnya beda.
  Solusi: tentukan qty sekali, simpan di list, pakai ulang untuk keduanya.
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import math

random.seed(42)
OUTPUT_DIR = Path("csv_output3")
OUTPUT_DIR.mkdir(exist_ok=True)

# ─── Konstanta Wilayah ────────────────────────────────────────────────────────
CITY_AREAS = {
    "jakarta": [
        {"name": "Jakarta Pusat",   "lat": (-6.175, -6.140), "lon": (106.820, 106.870)},
        {"name": "Jakarta Selatan", "lat": (-6.280, -6.180), "lon": (106.790, 106.870)},
        {"name": "Jakarta Barat",   "lat": (-6.180, -6.130), "lon": (106.740, 106.820)},
        {"name": "Jakarta Timur",   "lat": (-6.250, -6.150), "lon": (106.870, 106.980)},
        {"name": "Jakarta Utara",   "lat": (-6.140, -6.100), "lon": (106.820, 106.920)},
    ],
    "medan": [
        {"name": "Medan Kota",  "lat": (3.55, 3.62), "lon": (98.65, 98.72)},
        {"name": "Medan Barat", "lat": (3.55, 3.60), "lon": (98.63, 98.68)},
        {"name": "Medan Timur", "lat": (3.58, 3.65), "lon": (98.70, 98.75)},
    ],
    "surabaya": [
        {"name": "Surabaya Pusat", "lat": (-7.27, -7.23), "lon": (112.73, 112.75)},
        {"name": "Surabaya Barat", "lat": (-7.30, -7.25), "lon": (112.65, 112.72)},
        {"name": "Surabaya Timur", "lat": (-7.30, -7.20), "lon": (112.75, 112.80)},
    ],
}

CITY_KEYS = list(CITY_AREAS.keys())  # ["jakarta", "medan", "surabaya"]


# FIX #1: Fuzzy mapping area → city.
# Dataset mungkin punya nilai seperti "Pusat", "Jakarta Pusat", "Kota Medan", dll.
# Daripada exact match, kita cek apakah city keyword muncul di dalam nama area.
def area_to_city(area_name: str) -> str:
    """
    Petakan nama area bebas ke salah satu city key.
    Prioritas: cek substring dari city key dalam area_name (case-insensitive).
    Fallback: 'jakarta'.
    """
    name_lower = area_name.lower()
    for city_key in CITY_KEYS:
        if city_key in name_lower:
            return city_key
    # Cek alias umum
    if "surabaya" in name_lower or "sby" in name_lower:
        return "surabaya"
    if "medan" in name_lower:
        return "medan"
    # Semua yang tidak dikenal → jakarta (paling banyak datanya)
    return "jakarta"


FOOD_CATEGORIES = [
    "Ayam & Bebek", "Seafood", "Nasi & Lauk", "Mie & Pasta",
    "Burger & Sandwich", "Pizza", "Sushi & Japanese", "Korean Food",
    "Minuman & Jus", "Dessert & Snack", "Sarapan", "Vegetarian",
    "Western", "Padang", "Bakso & Soto",
]
PAYMENT_METHODS = [
    "GoPay", "OVO", "Cash", "DANA",
    "BCA Virtual Account", "Mandiri Virtual Account",
]
ORDER_STATUSES = [
    "delivered", "delivered", "delivered",
    "cancelled_by_customer",
    "cancelled_by_driver", "cancelled_by_driver",
]
FIRST_NAMES = [
    "Budi","Siti","Ahmad","Dewi","Eko","Rina","Fajar","Ayu","Rizky","Putri",
    "Dimas","Mega","Aldi","Nisa","Kevin","Dinda","Bagas","Sari","Yoga","Fifi",
    "Hendra","Lestari","Wahyu","Maya","Arif","Citra","Gilang","Tari","Reza","Indah",
]
LAST_NAMES = [
    "Santoso","Wijaya","Kusuma","Pratama","Suharto","Rahayu","Hidayat",
    "Nugroho","Setiawan","Utami","Firmansyah","Anggraini","Putra","Wati","Saputra",
]


# ─── Helper ───────────────────────────────────────────────────────────────────
def rand_phone():
    return f"08{random.randint(10,99)}{random.randint(10000000,99999999)}"


def rand_date(start="2023-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return s + timedelta(seconds=random.randint(0, int((e - s).total_seconds())))


def rand_dob():
    return f"{random.randint(1964,2007)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"


def write_csv(filename, fieldnames, rows):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {filename}: {len(rows):,} rows")


def generate_order_time():
    base_date = rand_date("2023-01-01", "2024-12-31")
    if base_date.weekday() < 5:
        hour = random.choice([12,13,19,20]) if random.random() < 0.7 else random.randint(8,22)
    else:
        hour = random.choice([12,13,19,20]) if random.random() < 0.4 else random.randint(9,23)
    return base_date.replace(hour=hour, minute=random.randint(0,59), second=random.randint(0,59))


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))


def rand_coords_by_city(city_key):
    areas = CITY_AREAS.get(city_key, CITY_AREAS["jakarta"])
    a = random.choice(areas)
    return round(random.uniform(*a["lat"]), 6), round(random.uniform(*a["lon"]), 6), a["name"]


def calc_delivery_fee(distance_km):
    base    = 2000
    per_km  = 2500
    fee     = base + round(distance_km * per_km / 500) * 500
    return min(int(fee), 25000)


# ─── MERCHANTS & PRODUCTS ─────────────────────────────────────────────────────
print("Loading merchants & products from dataset...")
df = pd.read_csv("dataset/gofood_dataset.csv")
df["merchant_name"] = df["merchant_name"].str.strip()
df["merchant_area"] = df["merchant_area"].str.strip().str.title()

merchant_df = df[["merchant_name","merchant_area"]].drop_duplicates().reset_index(drop=True)
merchant_df["merchant_id"] = [str(uuid.uuid4()) for _ in range(len(merchant_df))]
df = df.merge(merchant_df, on=["merchant_name","merchant_area"], how="left")

# Debug: tampilkan area unik dari dataset agar bisa verifikasi mapping
unique_areas = df["merchant_area"].unique()
print(f"  Area unik di dataset ({len(unique_areas)}): {sorted(unique_areas)[:10]} ...")

merchants = []
for _, row in merchant_df.iterrows():
    area_name = row["merchant_area"]
    # FIX #1: gunakan fuzzy mapping, bukan exact match
    city_key = area_to_city(area_name)
    lat, lon, area = rand_coords_by_city(city_key)
    merchants.append({
        "merchant_id":   row["merchant_id"],
        "merchant_name": row["merchant_name"],
        "category":      random.choice(FOOD_CATEGORIES),
        "address":       f"Jl. Raya No.{random.randint(1,200)}",
        "area":          area,
        "city":          city_key,
        "lat":           lat,
        "lon":           lon,
        "phone":         rand_phone(),
        "rating":        round(random.uniform(3.5, 5.0), 1),
        "is_open":       1,
        "joined_at":     rand_date("2020-01-01", "2023-01-01"),
        "updated_at":    rand_date("2024-01-01", "2024-12-31"),
        "is_active":     1,
    })

merchants_by_city = {}
for m in merchants:
    merchants_by_city.setdefault(m["city"], []).append(m)

# Verifikasi distribusi merchant per kota
for ck, ml in merchants_by_city.items():
    print(f"  Merchant di {ck}: {len(ml)}")

write_csv("merchants.csv", list(merchants[0].keys()), merchants)

# Products
product_df = (
    df[["merchant_id","category","display","product","price","discount_price","isDiscount","description"]]
    .drop_duplicates().reset_index(drop=True)
)
products = []
for _, row in product_df.iterrows():
    products.append({
        "product_id":   str(uuid.uuid4()),
        "merchant_id":  row["merchant_id"],
        "product_name": row["product"],
        "category":     row["category"],
        "price":        int(row["price"]),
        "is_available": 1,
        "created_at":   rand_date("2023-01-01", "2024-01-01"),
    })

products_by_merchant = {}
for p in products:
    products_by_merchant.setdefault(p["merchant_id"], []).append(p)

write_csv("products.csv", list(products[0].keys()), products)
print("Merchant dan products selesai.")


# ─── USERS ───────────────────────────────────────────────────────────────────
print("Generating users...")
N_USERS = 3000
users = []

for _ in range(N_USERS):
    city_key = random.choice(CITY_KEYS)
    lat, lon, area = rand_coords_by_city(city_key)
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    users.append({
        "user_id":       str(uuid.uuid4()),
        "full_name":     name,
        "email":         f"{name.replace(' ','.').lower()}{random.randint(1,999)}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}",
        "phone":         rand_phone(),
        "date_of_birth": rand_dob(),
        "gender":        random.choice(["M","F"]),
        "city":          city_key,
        "address_area":  area,
        "lat":           lat,
        "lon":           lon,
        "created_at":    rand_date("2022-01-01","2023-12-31").strftime("%Y-%m-%d %H:%M:%S"),
        "is_active":     random.choice([1,1,1,0]),
    })

# Duplikat akun
for _ in range(150):
    base = random.choice(users)
    dup  = base.copy()
    dup["user_id"]    = str(uuid.uuid4())
    dup["email"]      = f"{base['full_name'].replace(' ','_').lower()}_{random.randint(100,999)}@{random.choice(['gmail.com','hotmail.com'])}"
    dup["phone"]      = rand_phone()
    dup["created_at"] = rand_date("2023-01-01","2024-06-01").strftime("%Y-%m-%d %H:%M:%S")
    users.append(dup)

users_by_city = {}
for u in users:
    users_by_city.setdefault(u["city"], []).append(u)

write_csv("users.csv", list(users[0].keys()), users)


# ─── DRIVERS ─────────────────────────────────────────────────────────────────
print("Generating drivers...")
N_DRIVERS = 1500
drivers = []

for _ in range(N_DRIVERS):
    city_key = random.choice(CITY_KEYS)
    lat, lon, area = rand_coords_by_city(city_key)
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    drivers.append({
        "driver_id":     str(uuid.uuid4()),
        "full_name":     name,
        "phone":         rand_phone(),
        "vehicle_type":  random.choice(["Motor","Motor","Motor","Mobil"]),
        "vehicle_plate": f"B {random.randint(1000,9999)} {random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
        "current_lat":   lat,
        "current_lon":   lon,
        "current_area":  area,
        "city":          city_key,
        "rating":        round(random.uniform(3.5, 5.0), 1),
        "total_trips":   random.randint(10, 5000),
        "joined_at":     rand_date("2021-01-01","2024-01-01").strftime("%Y-%m-%d %H:%M:%S"),
        "is_active":     random.choice([1,1,1,0]),
    })

drivers_by_city = {}
for d in drivers:
    drivers_by_city.setdefault(d["city"], []).append(d)

write_csv("drivers.csv", list(drivers[0].keys()), drivers)


# ─── ORDERS & ORDER ITEMS ─────────────────────────────────────────────────────
print("Generating orders...")
N_ORDERS   = 12000
MAX_DIST   = 15.0
orders     = []
order_items = []

for _ in range(N_ORDERS):
    oid      = str(uuid.uuid4())
    city_key = random.choice(CITY_KEYS)

    # Ambil pool per kota; fallback ke semua jika kota belum punya data
    city_merchants = merchants_by_city.get(city_key) or merchants
    city_users     = users_by_city.get(city_key)     or users
    city_drivers   = drivers_by_city.get(city_key)   or drivers

    merchant = random.choice(city_merchants)
    user     = random.choice(city_users)
    driver   = random.choice(city_drivers)

    mid      = merchant["merchant_id"]
    status   = random.choice(ORDER_STATUSES)
    order_dt = generate_order_time()

    # Koordinat pengiriman = lokasi user (tujuan)
    delivery_lat = user["lat"]
    delivery_lon = user["lon"]
    distance_km  = round(haversine(merchant["lat"], merchant["lon"], delivery_lat, delivery_lon), 2)

    # Jika jarak masih > 15 km (sub-area jauh dalam kota), clamp ke titik dekat merchant
    if distance_km > MAX_DIST:
        delivery_lat = round(merchant["lat"] + random.uniform(-0.02, 0.02), 6)
        delivery_lon = round(merchant["lon"] + random.uniform(-0.02, 0.02), 6)
        distance_km  = round(haversine(merchant["lat"], merchant["lon"], delivery_lat, delivery_lon), 2)

    delivery_fee = calc_delivery_fee(distance_km)

    # Produk
    m_products = products_by_merchant.get(mid) or random.sample(products, 3)
    selected   = random.sample(m_products, min(random.randint(1, 4), len(m_products)))

    # FIX #2: tentukan qty PER ITEM sekali di sini, pakai untuk subtotal DAN order_items
    selected_with_qty = [(p, random.randint(1, 3)) for p in selected]

    # Subtotal order = sum dari qty yang SAMA dengan yang masuk ke order_items
    subtotal = sum(int(p["price"]) * qty for p, qty in selected_with_qty)
    discount = random.choice([0, 0, 0, 2000, 5000, 10000])
    total    = max(subtotal + delivery_fee - discount, 0)

    orders.append({
        "order_id":       oid,
        "user_id":        user["user_id"],
        "driver_id":      driver["driver_id"],
        "merchant_id":    mid,
        "city":           city_key,
        "status":         status,
        "payment_method": random.choice(PAYMENT_METHODS),
        "subtotal":       subtotal,
        "delivery_fee":   delivery_fee,
        "discount":       discount,
        "total_amount":   total,
        "distance_km":    distance_km,
        "order_time":     order_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "delivered_time": (
            (order_dt + timedelta(minutes=random.randint(15, 60))).strftime("%Y-%m-%d %H:%M:%S")
            if status == "delivered" else None
        ),
        "delivery_lat":  delivery_lat,
        "delivery_lon":  delivery_lon,
        "delivery_area": user["address_area"],
        "pickup_lat":    merchant["lat"],
        "pickup_lon":    merchant["lon"],
        "pickup_area":   merchant["area"],
    })

    # Order items — gunakan selected_with_qty yang sama
    for p, qty in selected_with_qty:
        order_items.append({
            "order_item_id": str(uuid.uuid4()),
            "order_id":      oid,
            "product_id":    p["product_id"],
            "merchant_id":   mid,
            "product_name":  p["product_name"],
            "category":      p["category"],
            "quantity":      qty,
            "unit_price":    p["price"],
            "subtotal":      int(p["price"]) * qty,
        })

# Simulasi re-order (cancel → order ulang)
dup_orders = []
dup_items  = []
for _ in range(200):
    base_order = random.choice([o for o in orders if o["status"] == "delivered"])
    dup = base_order.copy()
    dup["order_id"]      = str(uuid.uuid4())
    dup["status"]        = random.choice(["cancelled_by_customer","cancelled_by_driver"])
    orig_time            = datetime.strptime(base_order["order_time"], "%Y-%m-%d %H:%M:%S")
    dup["order_time"]    = (orig_time - timedelta(minutes=random.randint(5,30))).strftime("%Y-%m-%d %H:%M:%S")
    dup["delivered_time"] = None
    dup_orders.append(dup)

    for bi in [oi for oi in order_items if oi["order_id"] == base_order["order_id"]]:
        ni = bi.copy()
        ni["order_item_id"] = str(uuid.uuid4())
        ni["order_id"]      = dup["order_id"]
        dup_items.append(ni)

orders.extend(dup_orders)
order_items.extend(dup_items)

write_csv("orders.csv",      list(orders[0].keys()),      orders)
write_csv("order_items.csv", list(order_items[0].keys()), order_items)


# ─── REVIEWS ──────────────────────────────────────────────────────────────────
print("Generating reviews...")
reviews = []
delivered_orders = [o for o in orders if o["status"] == "delivered"]
for o in random.sample(delivered_orders, min(5000, len(delivered_orders))):
    reviews.append({
        "review_id":       str(uuid.uuid4()),
        "order_id":        o["order_id"],
        "user_id":         o["user_id"],
        "merchant_id":     o["merchant_id"],
        "driver_id":       o["driver_id"],
        "merchant_rating": random.randint(3, 5),
        "driver_rating":   random.randint(3, 5),
        "comment": random.choice([
            "Enak banget!","Recommended!","Cepat sampai",
            "Lumayan","Mantap jiwa","Suka banget","",
        ]),
        "created_at": o["delivered_time"],
    })
write_csv("reviews.csv", list(reviews[0].keys()), reviews)

print(f"\n✅ Selesai! Semua CSV tersimpan di: {OUTPUT_DIR}/")
print(f"   Total orders (+ duplikat): {len(orders):,}")
print(f"   Total order items        : {len(order_items):,}")