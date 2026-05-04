"""
GoFood Data Simulation - Generate Dummy Data  (FIXED VERSION)
Region: DKI Jakarta, Medan, Surabaya

===== PERBAIKAN DARI generateData_1.py =====

FIX #1  PEAK HOUR TERLALU TAJAM
  Sebelum : random.choice([12,13,19,20]) 70% → spike kotak di 4 jam
  Sesudah : random.choices(range(24), weights=HOUR_WEIGHTS_WEEKDAY/WEEKEND)
            Bobot gradual per jam → kurva bell yang smooth

FIX #2  PEAK DAY TIDAK NATURAL
  Sebelum : rand_date() acak uniform → semua hari frekuensi sama
  Sesudah : pilih dulu hari-dalam-minggu via bobot DAY_WEIGHTS,
            lalu mapping ke tanggal aktual di rentang 2023-2024
            Weekend (Sabtu/Minggu) ~40% lebih ramai dari hari kerja biasa

FIX #3  CATEGORY PRODUK ~333 NILAI UNIK
  Sebelum : row["category"] langsung dari Kaggle → ratusan nilai mentah
  Sesudah : normalize_category() fuzzy-match keyword → 15 kategori standar
            (FOOD_CATEGORIES), fallback random.choice jika tidak cocok

FIX #4  (BONUS) STATUS ORDER FLAT
  Sebelum : random.choice(ORDER_STATUSES) — semua item list peluang sama
  Sesudah : random.choices dengan weights agar "delivered" dominan realistis
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import math

random.seed(42)
OUTPUT_DIR = Path("csv_output_fixed")
OUTPUT_DIR.mkdir(exist_ok=True)

# ─── Konstanta Wilayah ────────────────────────────────────────────────────────
CITY_AREAS = {
    "jakarta":  {"name": "Jakarta",  "lat": (-6.280, -6.100), "lon": (106.740, 106.980)},
    "surabaya": {"name": "Surabaya", "lat": (-7.350, -7.150), "lon": (112.600, 112.850)},
    "medan":    {"name": "Medan",    "lat": (3.500,  3.750),  "lon": (98.600,  98.800)},
}

CITY_KEYS = list(CITY_AREAS.keys())

# ─── FIX #1: Bobot jam gradual (bukan spike 4 jam) ───────────────────────────
# Hari kerja: sarapan ringan pagi, puncak makan siang jam 12, puncak malam jam 19
HOUR_WEIGHTS_WEEKDAY = [
    1, 1, 1, 1, 1, 2,       # 00-05 dini hari: sangat sepi
    4, 6, 7, 6, 7, 10,      # 06-11 pagi: naik bertahap (sarapan jam 7-8)
    18, 14, 10, 8, 7, 9,    # 12-17 siang: peak jam 12, turun bertahap
    15, 18, 14, 11, 8, 4,   # 18-23 malam: peak jam 19, turun gradual
]

# Weekend: tidak ada rush hour kerja, peak siang & malam lebih merata & lebih lama
HOUR_WEIGHTS_WEEKEND = [
    2, 1, 1, 1, 1, 2,       # 00-05: sedikit lebih ramai (begadang weekend)
    3, 5, 7, 8, 9, 11,      # 06-11 pagi: santai, brunch mulai jam 9-10
    16, 15, 13, 11, 10, 11, # 12-17 siang: peak lebih lebar & merata
    16, 18, 16, 13, 10, 6,  # 18-23 malam: peak sedikit lebih lama
]

# ─── FIX #2: Bobot hari-dalam-seminggu ───────────────────────────────────────
# Indeks 0=Senin ... 6=Minggu
# Realita food delivery: Jumat-Sabtu-Minggu paling ramai
DAY_OF_WEEK_WEIGHTS = [
    7,   # Senin   - cukup ramai (awal minggu, malas masak)
    6,   # Selasa  - sedikit sepi
    6,   # Rabu    - sedikit sepi
    8,   # Kamis   - mulai naik
    11,  # Jumat   - TGIF, ramai
    13,  # Sabtu   - paling ramai
    12,  # Minggu  - ramai (malas keluar)
]

FOOD_CATEGORIES = [
    "Ayam & Bebek", "Seafood", "Nasi & Lauk", "Mie & Pasta",
    "Burger & Sandwich", "Pizza", "Sushi & Japanese", "Korean Food",
    "Minuman & Jus", "Dessert & Snack", "Sarapan", "Vegetarian",
    "Western", "Padang", "Bakso & Soto",
]

PAYMENT_METHODS = [
    "GoPay", "GoPay", "GoPay",          # GoPay paling dominan
    "OVO", "OVO",                         # OVO kedua
    "DANA",
    "Cash",
    "BCA Virtual Account",
    "Mandiri Virtual Account",
]

ORDER_STATUSES        = ["delivered", "cancelled_by_customer", "cancelled_by_driver"]
# FIX #4: delivered jauh lebih sering (realistis ~75-80%)
ORDER_STATUS_WEIGHTS  = [78, 13, 9]

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


def rand_dob():
    return f"{random.randint(1964,2007)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"


def write_csv(filename, fieldnames, rows):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {filename}: {len(rows):,} rows")


# ─── FIX #1 & #2: generate_order_time yang natural ───────────────────────────
def rand_date_weighted(start="2023-01-01", end="2024-12-31") -> datetime:
    """
    Pilih tanggal secara berbobot berdasarkan hari-dalam-minggu (DAY_OF_WEEK_WEIGHTS).
    Cara kerja:
      1. Enumerasi semua hari dalam rentang start–end.
      2. Assign bobot dari DAY_OF_WEEK_WEIGHTS sesuai weekday.
      3. random.choices() memilih satu tanggal sesuai proporsi bobot.
    Hasilnya: Jumat/Sabtu/Minggu ~lebih banyak order, Selasa/Rabu ~sepi.
    """
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end,   "%Y-%m-%d")
    all_days    = [s + timedelta(days=i) for i in range((e - s).days + 1)]
    day_weights = [DAY_OF_WEEK_WEIGHTS[d.weekday()] for d in all_days]
    return random.choices(all_days, weights=day_weights, k=1)[0]


def generate_order_time() -> datetime:
    """
    FIX #1 + #2: tanggal berbobot per hari-dalam-minggu,
    jam berbobot per bobot gradual 24-jam (bukan spike 4 jam).
    """
    base_date = rand_date_weighted("2023-01-01", "2024-12-31")
    is_weekend = base_date.weekday() >= 5
    weights    = HOUR_WEIGHTS_WEEKEND if is_weekend else HOUR_WEIGHTS_WEEKDAY
    hour       = random.choices(range(24), weights=weights, k=1)[0]
    return base_date.replace(
        hour=hour,
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )


def haversine(lat1, lon1, lat2, lon2):
    R    = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a    = (math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def rand_coords_by_city(city_key):
    a = CITY_AREAS.get(city_key, CITY_AREAS["jakarta"])
    return round(random.uniform(*a["lat"]), 6), round(random.uniform(*a["lon"]), 6), a["name"]

def calc_delivery_fee(distance_km):
    base   = 2000
    per_km = 2500
    fee    = base + round(distance_km * per_km / 500) * 500
    return min(int(fee), 25000)


def area_to_city(area_name: str) -> str:
    name_lower = area_name.lower()
    for city_key in CITY_KEYS:
        if city_key in name_lower:
            return city_key
    if "surabaya" in name_lower or "sby" in name_lower:
        return "surabaya"
    if "medan" in name_lower:
        return "medan"
    return "jakarta"


# ─── FIX #3: Normalisasi category produk ─────────────────────────────────────
# Mapping keyword → salah satu dari 15 FOOD_CATEGORIES standar.
# Urutan dict penting: keyword lebih spesifik di atas keyword umum.
_CATEGORY_KEYWORDS = {
    "Ayam & Bebek":        ["ayam", "bebek", "chicken", "geprek", "unggas", "bakar ayam",
                             "goreng ayam", "penyet"],
    "Seafood":             ["seafood", "ikan", "udang", "cumi", "kepiting", "lobster",
                             "kerang", "gurame", "lele", "kakap"],
    "Nasi & Lauk":         ["nasi", "rice", "lauk", "gudeg", "rendang", "semur",
                             "liwet", "tumpeng", "campur", "kotak"],
    "Mie & Pasta":         ["mie", "mi ", "bakmi", "pasta", "spaghetti", "fettuccine",
                             "ramen", "udon", "kwetiau", "bihun"],
    "Burger & Sandwich":   ["burger", "sandwich", "hotdog", "sub ", "wrap"],
    "Pizza":               ["pizza", "calzone"],
    "Sushi & Japanese":    ["sushi", "japanese", "jepang", "ramen", "gyoza", "takoyaki",
                             "teriyaki", "bento", "onigiri", "sashimi"],
    "Korean Food":         ["korean", "korea", "kpop food", "bibimbap", "tteokbokki",
                             "bulgogi", "kimbap", "samgyeopsal", "ramyeon"],
    "Minuman & Jus":       ["jus", "juice", "minuman", "es teh", "teh", "lemonade",
                             "smoothie", "milkshake", "boba", "bubble tea", "air mineral"],
    "Dessert & Snack":     ["dessert", "snack", "es krim", "ice cream", "cake", "kue",
                             "brownies", "waffle", "donat", "roti", "pisang goreng",
                             "martabak", "pancake", "biscuit", "keripik"],
    "Sarapan":             ["sarapan", "breakfast", "bubur", "oatmeal", "sereal",
                             "toast", "telur", "omelette", "lontong"],
    "Vegetarian":          ["vegetarian", "vegan", "veggie", "gado", "pecel", "tempe",
                             "tahu", "sayur", "salad"],
    "Western":             ["western", "steak", "ribs", "bbq", "fried chicken",
                             "pasta barat", "fish & chips"],
    "Padang":              ["padang", "minang", "rendang", "gulai", "soto padang"],
    "Bakso & Soto":        ["bakso", "soto", "pempek", "siomay", "batagor",
                             "cilok", "tekwan", "mie ayam bakso"],
}

def normalize_category(raw: str) -> str:
    """
    FIX #3: Map nilai mentah dari Kaggle ke salah satu FOOD_CATEGORIES.
    Pakai keyword fuzzy matching (substring), fallback random jika tidak cocok.
    """
    text = str(raw).lower().strip()
    for category, keywords in _CATEGORY_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return category
    return random.choice(FOOD_CATEGORIES)


# ─── MERCHANTS & PRODUCTS ─────────────────────────────────────────────────────
print("Loading merchants & products from dataset...")
df = pd.read_csv("dataset/gofood_dataset.csv")
df["merchant_name"] = df["merchant_name"].str.strip()
df["merchant_area"] = df["merchant_area"].str.strip().str.title()

merchant_df = df[["merchant_name", "merchant_area"]].drop_duplicates().reset_index(drop=True)
merchant_df["merchant_id"] = [str(uuid.uuid4()) for _ in range(len(merchant_df))]
df = df.merge(merchant_df, on=["merchant_name", "merchant_area"], how="left")

unique_areas = df["merchant_area"].unique()
print(f"  Area unik di dataset ({len(unique_areas)}): {sorted(unique_areas)[:10]} ...")

merchants = []
for _, row in merchant_df.iterrows():
    area_name = row["merchant_area"]
    city_key  = area_to_city(area_name)
    lat, lon, area = rand_coords_by_city(city_key)
    merchants.append({
        "merchant_id":   row["merchant_id"],
        "merchant_name": row["merchant_name"],
        "category":      random.choice(FOOD_CATEGORIES),
        "address":       f"Jl. Raya No.{random.randint(1, 200)}",
        "area":          area,
        "city":          city_key,
        "lat":           lat,
        "lon":           lon,
        "phone":         rand_phone(),
        "rating":        round(random.uniform(3.5, 5.0), 1),
        "is_open":       1,
        "joined_at":     (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 365 * 3))).strftime("%Y-%m-%d"),
        "updated_at":    (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "is_active":     1,
    })

merchants_by_city = {}
for m in merchants:
    merchants_by_city.setdefault(m["city"], []).append(m)

for ck, ml in merchants_by_city.items():
    print(f"  Merchant di {ck}: {len(ml)}")

write_csv("merchants.csv", list(merchants[0].keys()), merchants)

# ─── PRODUCTS ─────────────────────────────────────────────────────────────────
product_df = (
    df[["merchant_id", "category", "display", "product", "price",
        "discount_price", "isDiscount", "description"]]
    .drop_duplicates().reset_index(drop=True)
)

# FIX #3: normalisasi kolom category SEBELUM dipakai
product_df["category"] = product_df["category"].apply(normalize_category)
print(f"  Kategori produk unik setelah normalisasi: {product_df['category'].nunique()}")
print(f"  Distribusi:\n{product_df['category'].value_counts().to_string()}")

products = []
for _, row in product_df.iterrows():
    products.append({
        "product_id":   str(uuid.uuid4()),
        "merchant_id":  row["merchant_id"],
        "product_name": row["product"],
        "category":     row["category"],          # sudah dinormalisasi
        "price":        int(row["price"]),
        "is_available": 1,
        "created_at":   (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
    })

products_by_merchant = {}
for p in products:
    products_by_merchant.setdefault(p["merchant_id"], []).append(p)

write_csv("products.csv", list(products[0].keys()), products)
print("Merchant dan products selesai.")


# ─── USERS ───────────────────────────────────────────────────────────────────
print("Generating users...")
N_USERS = 3000
users   = []

for _ in range(N_USERS):
    city_key = random.choice(CITY_KEYS)
    lat, lon, area = rand_coords_by_city(city_key)
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    created = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 365 * 2))
    users.append({
        "user_id":       str(uuid.uuid4()),
        "full_name":     name,
        "email":         f"{name.replace(' ', '.').lower()}{random.randint(1, 999)}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}",
        "phone":         rand_phone(),
        "date_of_birth": rand_dob(),
        "gender":        random.choice(["M", "F"]),
        "city":          city_key,
        "address_area":  area,
        "lat":           lat,
        "lon":           lon,
        "created_at":    created.strftime("%Y-%m-%d %H:%M:%S"),
        "is_active":     random.choices([1, 0], weights=[90, 10])[0],
    })

# Duplikat akun (simulasi multi-akun)
for _ in range(150):
    base = random.choice(users)
    dup  = base.copy()
    dup["user_id"] = str(uuid.uuid4())
    dup["email"]   = f"{base['full_name'].replace(' ', '_').lower()}_{random.randint(100, 999)}@{random.choice(['gmail.com', 'hotmail.com'])}"
    dup["phone"]   = rand_phone()
    created_dup    = datetime(2023, 1, 1) + timedelta(days=random.randint(0,540))
    dup["created_at"] = created_dup.strftime("%Y-%m-%d %H:%M:%S")
    users.append(dup)

users_by_city = {}
for u in users:
    users_by_city.setdefault(u["city"], []).append(u)

write_csv("users.csv", list(users[0].keys()), users)


# ─── DRIVERS ─────────────────────────────────────────────────────────────────
print("Generating drivers...")
N_DRIVERS = 1500
drivers   = []

for _ in range(N_DRIVERS):
    city_key = random.choice(CITY_KEYS)
    lat, lon, area = rand_coords_by_city(city_key)
    name    = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    joined  = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 365 * 3))
    drivers.append({
        "driver_id":     str(uuid.uuid4()),
        "full_name":     name,
        "phone":         rand_phone(),
        "vehicle_type":  random.choices(["Motor", "Mobil"], weights=[85, 15])[0],
        "vehicle_plate": f"B {random.randint(1000, 9999)} {random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
        "current_lat":   lat,
        "current_lon":   lon,
        "current_area":  area,
        "city":          city_key,
        "rating":        round(random.uniform(3.5, 5.0), 1),
        "total_trips":   random.randint(10, 5000),
        "joined_at":     joined.strftime("%Y-%m-%d %H:%M:%S"),
        "is_active":     random.choices([1, 0], weights=[85, 15])[0],
    })

drivers_by_city = {}
for d in drivers:
    drivers_by_city.setdefault(d["city"], []).append(d)

write_csv("drivers.csv", list(drivers[0].keys()), drivers)


# ─── ORDERS & ORDER ITEMS ─────────────────────────────────────────────────────
print("Generating orders...")
N_ORDERS    = 12000
MAX_DIST    = 15.0
orders      = []
order_items = []

for _ in range(N_ORDERS):
    oid      = str(uuid.uuid4())
    city_key = random.choice(CITY_KEYS)

    city_merchants = merchants_by_city.get(city_key) or merchants
    city_users     = users_by_city.get(city_key)     or users
    city_drivers   = drivers_by_city.get(city_key)   or drivers

    merchant = random.choice(city_merchants)
    user     = random.choice(city_users)
    driver   = random.choice(city_drivers)

    mid      = merchant["merchant_id"]

    # FIX #4: status dengan bobot → delivered dominan
    status   = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    # FIX #1 & #2: waktu order natural (jam + hari berbobot)
    order_dt = generate_order_time()

    delivery_lat = user["lat"]
    delivery_lon = user["lon"]
    distance_km  = round(haversine(merchant["lat"], merchant["lon"], delivery_lat, delivery_lon), 2)

    if distance_km > MAX_DIST:
        delivery_lat = round(merchant["lat"] + random.uniform(-0.02, 0.02), 6)
        delivery_lon = round(merchant["lon"] + random.uniform(-0.02, 0.02), 6)
        distance_km  = round(haversine(merchant["lat"], merchant["lon"], delivery_lat, delivery_lon), 2)

    delivery_fee = calc_delivery_fee(distance_km)

    m_products         = products_by_merchant.get(mid) or random.sample(products, 3)
    selected           = random.sample(m_products, min(random.randint(1, 4), len(m_products)))
    selected_with_qty  = [(p, random.randint(1, 3)) for p in selected]

    subtotal = sum(int(p["price"]) * qty for p, qty in selected_with_qty)
    discount = random.choices([0, 2000, 5000, 10000, 15000],
                               weights=[55, 15, 15, 10, 5], k=1)[0]
    total    = max(subtotal + delivery_fee - discount, 0)

    # Waktu selesai hanya ada jika delivered
    delivered_time = (
        (order_dt + timedelta(minutes=random.randint(15, 60))).strftime("%Y-%m-%d %H:%M:%S")
        if status == "delivered" else None
    )

    orders.append({
        "order_id":        oid,
        "user_id":         user["user_id"],
        "driver_id":       driver["driver_id"],
        "merchant_id":     mid,
        "city":            city_key,
        "status":          status,
        "payment_method":  random.choices(PAYMENT_METHODS,
                               weights=[5,5,5,2,2,1,1,1,1], k=1)[0],
        "subtotal":        subtotal,
        "delivery_fee":    delivery_fee,
        "discount":        discount,
        "total_amount":    total,
        "distance_km":     distance_km,
        "order_time":      order_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "delivered_time":  delivered_time,
        "delivery_lat":    delivery_lat,
        "delivery_lon":    delivery_lon,
        "delivery_area":   user["address_area"],
        "pickup_lat":      merchant["lat"],
        "pickup_lon":      merchant["lon"],
        "pickup_area":     merchant["area"],
        "day_of_week":     order_dt.strftime("%A"),
        "is_weekend":      int(order_dt.weekday() >= 5),
    })

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

# Simulasi re-order (cancel → order ulang, sesaat sebelum order asli)
dup_orders = []
dup_items  = []
for _ in range(200):
    base_order = random.choice([o for o in orders if o["status"] == "delivered"])
    dup = base_order.copy()
    dup["order_id"]       = str(uuid.uuid4())
    dup["status"]         = random.choice(["cancelled_by_customer", "cancelled_by_driver"])
    dup["delivered_time"] = None
    orig_time             = datetime.strptime(base_order["order_time"], "%Y-%m-%d %H:%M:%S")
    dup["order_time"]     = (orig_time - timedelta(minutes=random.randint(5, 30))).strftime("%Y-%m-%d %H:%M:%S")
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
reviews          = []
delivered_orders = [o for o in orders if o["status"] == "delivered"]
review_sample    = random.sample(delivered_orders, min(5000, len(delivered_orders)))

POSITIVE_COMMENTS = [
    "Enak banget!", "Recommended!", "Cepat sampai", "Mantap jiwa",
    "Suka banget", "Porsinya pas", "Harga terjangkau, rasanya oke",
    "Selalu puas pesan sini", "Drivernya ramah",
]
NEUTRAL_COMMENTS = [
    "Lumayan", "Oke lah", "Standar tapi cukup",
    "Agak lama tapi enak", "",
]
NEGATIVE_COMMENTS = [
    "Pesanan tidak sesuai", "Agak lama pengirimannya",
    "Kurang puas", "Harusnya lebih cepat",
]

for o in review_sample:
    merchant_rating = random.choices([5, 4, 3, 2, 1], weights=[40, 35, 15, 6, 4])[0]
    driver_rating   = random.choices([5, 4, 3, 2, 1], weights=[45, 35, 12, 5, 3])[0]

    # Komentar disesuaikan dengan rating
    avg_rating = (merchant_rating + driver_rating) / 2
    if avg_rating >= 4:
        comment = random.choice(POSITIVE_COMMENTS)
    elif avg_rating >= 3:
        comment = random.choice(NEUTRAL_COMMENTS)
    else:
        comment = random.choice(NEGATIVE_COMMENTS)

    reviews.append({
        "review_id":       str(uuid.uuid4()),
        "order_id":        o["order_id"],
        "user_id":         o["user_id"],
        "merchant_id":     o["merchant_id"],
        "driver_id":       o["driver_id"],
        "merchant_rating": merchant_rating,
        "driver_rating":   driver_rating,
        "comment":         comment,
        "created_at":      o["delivered_time"],
    })

write_csv("reviews.csv", list(reviews[0].keys()), reviews)


# ─── RINGKASAN ────────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print("  RINGKASAN DATASET (FIXED VERSION)")
print(f"{'='*55}")
print(f"  Output folder    : {OUTPUT_DIR}/")
print(f"  Total orders     : {len(orders):,}  (termasuk {len(dup_orders)} re-order)")
print(f"  Total order items: {len(order_items):,}")
print(f"  Total merchants  : {len(merchants):,}")
print(f"  Total products   : {len(products):,}")
print(f"  Total users      : {len(users):,}")
print(f"  Total drivers    : {len(drivers):,}")
print(f"  Total reviews    : {len(reviews):,}")
delivered_count = sum(1 for o in orders if o["status"] == "delivered")
print(f"  Delivered        : {delivered_count:,} ({delivered_count/len(orders)*100:.1f}%)")
print(f"\n  Kategori produk unik: {product_df['category'].nunique()} (target: 15)")
print(f"{'='*55}")
print("  PERBAIKAN YANG DITERAPKAN:")
print("  [FIX 1] Peak hour  : bobot gradual 24 jam (HOUR_WEIGHTS_*)")
print("  [FIX 2] Peak day   : bobot per hari-dalam-minggu (DAY_OF_WEEK_WEIGHTS)")
print("  [FIX 3] Category   : normalize_category() → 15 kategori standar")
print("  [FIX 4] Status     : weighted choices, delivered ~78%")
print(f"{'='*55}")
print("  Siap untuk proses ETL! ✓")