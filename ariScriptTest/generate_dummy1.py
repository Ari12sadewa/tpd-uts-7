"""
GoFood Data Simulation - Generate Dummy Data
Menghasilkan data dummy untuk semua tabel OLTP GoFood
Region: DKI Jakarta
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)
OUTPUT_DIR = Path("csv_output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ─── Konstanta Wilayah DKI Jakarta ───────────────────────────────────────────
JAKARTA_AREAS = [
    {"name": "Jakarta Pusat",   "lat_range": (-6.175, -6.140), "lon_range": (106.820, 106.870)},
    {"name": "Jakarta Utara",   "lat_range": (-6.140, -6.100), "lon_range": (106.820, 106.920)},
    {"name": "Jakarta Barat",   "lat_range": (-6.180, -6.130), "lon_range": (106.740, 106.820)},
    {"name": "Jakarta Selatan", "lat_range": (-6.280, -6.180), "lon_range": (106.790, 106.870)},
    {"name": "Jakarta Timur",   "lat_range": (-6.250, -6.150), "lon_range": (106.870, 106.980)},
]

FOOD_CATEGORIES = ["Ayam & Bebek", "Seafood", "Nasi & Lauk", "Mie & Pasta", "Burger & Sandwich",
                   "Pizza", "Sushi & Japanese", "Korean Food", "Minuman & Jus", "Dessert & Snack",
                   "Sarapan", "Vegetarian", "Western", "Padang", "Bakso & Soto"]

PAYMENT_METHODS = ["GoPay", "OVO", "Cash", "DANA", "BCA Virtual Account", "Mandiri Virtual Account"]
ORDER_STATUSES  = ["delivered", "delivered", "delivered", "cancelled_by_customer",
                   "cancelled_by_driver", "cancelled_by_driver"]  # weighted

FIRST_NAMES = ["Budi", "Siti", "Ahmad", "Dewi", "Eko", "Rina", "Fajar", "Ayu", "Rizky", "Putri",
               "Dimas", "Mega", "Aldi", "Nisa", "Kevin", "Dinda", "Bagas", "Sari", "Yoga", "Fifi",
               "Hendra", "Lestari", "Wahyu", "Maya", "Arif", "Citra", "Gilang", "Tari", "Reza", "Indah"]
LAST_NAMES  = ["Santoso", "Wijaya", "Kusuma", "Pratama", "Suharto", "Rahayu", "Hidayat", "Nugroho",
               "Setiawan", "Utami", "Firmansyah", "Anggraini", "Putra", "Wati", "Saputra"]

MERCHANT_PREFIXES = ["Warung", "Rumah Makan", "Kedai", "Depot", "Resto", "Dapur", "Kafe"]
MERCHANT_NAMES    = ["Mak Inah", "Pak Budi", "Bu Sri", "Mas Joko", "Bang Ali", "Neng Geulis",
                     "Mbak Yuni", "Cak Amin", "Si Udin", "Mpok Siti", "Om Deni", "Teh Rini"]

# ─── Helper ───────────────────────────────────────────────────────────────────
def rand_phone():
    return f"08{random.randint(10,99)}{random.randint(10000000,99999999)}"

def rand_jakarta_coords(area=None):
    a = area or random.choice(JAKARTA_AREAS)
    lat = round(random.uniform(*a["lat_range"]), 6)
    lon = round(random.uniform(*a["lon_range"]), 6)
    return lat, lon, a["name"]

def rand_date(start="2023-01-01", end="2024-12-31"):
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    return s + timedelta(seconds=random.randint(0, int((e - s).total_seconds())))

def rand_dob():
    # Usia 17-60
    year = random.randint(1964, 2007)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    return f"{year}-{month:02d}-{day:02d}"

def write_csv(filename, fieldnames, rows):
    path = OUTPUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ {filename}: {len(rows):,} rows")

# ─── 1. USERS ─────────────────────────────────────────────────────────────────
print("Generating users...")
N_USERS = 3000
users = []
for i in range(N_USERS):
    uid  = str(uuid.uuid4())
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    email = f"{name.replace(' ', '.').lower()}{random.randint(1,999)}@{random.choice(['gmail.com','yahoo.com','outlook.com'])}"
    phone = rand_phone()
    dob   = rand_dob()
    lat, lon, area = rand_jakarta_coords()
    created = rand_date("2022-01-01", "2023-12-31")
    users.append({
        "user_id": uid, "full_name": name, "email": email, "phone": phone,
        "date_of_birth": dob, "gender": random.choice(["M", "F"]),
        "address_area": area, "lat": lat, "lon": lon,
        "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
        "is_active": random.choice([1, 1, 1, 0])
    })

# ── Simulasi duplikat: beberapa user punya 2 akun (email beda, HP beda, orang sama)
dup_users = []
for _ in range(150):  # 150 orang punya akun duplikat
    base = random.choice(users)
    dup = base.copy()
    dup["user_id"]    = str(uuid.uuid4())
    dup["email"]      = f"{base['full_name'].replace(' ', '_').lower()}_{random.randint(100,999)}@{random.choice(['gmail.com','hotmail.com'])}"
    dup["phone"]      = rand_phone()
    dup["created_at"] = rand_date("2023-01-01", "2024-06-01").strftime("%Y-%m-%d %H:%M:%S")
    dup_users.append(dup)

users.extend(dup_users)
user_ids = [u["user_id"] for u in users]
write_csv("users.csv", list(users[0].keys()), users)

# ─── 2. DRIVERS ───────────────────────────────────────────────────────────────
print("Generating drivers...")
N_DRIVERS = 1500
drivers = []
for i in range(N_DRIVERS):
    did  = str(uuid.uuid4())
    name = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
    lat, lon, area = rand_jakarta_coords()
    joined = rand_date("2021-01-01", "2023-12-31")
    drivers.append({
        "driver_id": did, "full_name": name,
        "phone": rand_phone(),
        "vehicle_type": random.choice(["Motor", "Motor", "Motor", "Mobil"]),
        "vehicle_plate": f"B {random.randint(1000,9999)} {random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
        "current_lat": lat, "current_lon": lon, "current_area": area,
        "rating": round(random.uniform(3.5, 5.0), 1),
        "total_trips": random.randint(10, 5000),
        "joined_at": joined.strftime("%Y-%m-%d %H:%M:%S"),
        "is_active": random.choice([1, 1, 1, 0])
    })
driver_ids = [d["driver_id"] for d in drivers]
write_csv("drivers.csv", list(drivers[0].keys()), drivers)

# ─── 3. MERCHANTS ─────────────────────────────────────────────────────────────
print("Generating merchants...")
N_MERCHANTS = 800
merchants = []
for i in range(N_MERCHANTS):
    mid  = str(uuid.uuid4())
    area_obj = random.choice(JAKARTA_AREAS)
    lat, lon, area = rand_jakarta_coords(area_obj)
    cat  = random.choice(FOOD_CATEGORIES)
    name = f"{random.choice(MERCHANT_PREFIXES)} {random.choice(MERCHANT_NAMES)} {i+1}"
    joined = rand_date("2020-01-01", "2023-01-01")
    merchants.append({
        "merchant_id": mid, "merchant_name": name,
        "category": cat,
        "address": f"Jl. {random.choice(['Sudirman','Thamrin','Gatot Subroto','Rasuna Said','MT Haryono'])} No.{random.randint(1,200)}",
        "area": area, "lat": lat, "lon": lon,
        "phone": rand_phone(),
        "rating": round(random.uniform(3.0, 5.0), 1),
        "is_open": random.choice([1, 1, 0]),
        "joined_at": joined.strftime("%Y-%m-%d %H:%M:%S"),
        "is_active": 1
    })
merchant_ids = [m["merchant_id"] for m in merchants]
write_csv("merchants.csv", list(merchants[0].keys()), merchants)

# ─── 4. PRODUCTS ──────────────────────────────────────────────────────────────
print("Generating products...")
FOOD_ITEMS = {
    "Ayam & Bebek":    ["Ayam Bakar", "Ayam Goreng", "Bebek Goreng", "Ayam Penyet", "Sate Ayam"],
    "Seafood":         ["Ikan Bakar", "Udang Saus Tiram", "Cumi Goreng", "Kepiting Asam Manis"],
    "Nasi & Lauk":     ["Nasi Goreng", "Nasi Uduk", "Nasi Kuning", "Nasi Liwet"],
    "Mie & Pasta":     ["Mie Goreng", "Bihun Goreng", "Spaghetti", "Kwetiau"],
    "Burger & Sandwich":["Burger Beef", "Burger Chicken", "Club Sandwich", "BLT"],
    "Pizza":           ["Pizza Margherita", "Pizza Pepperoni", "Pizza Veggie"],
    "Sushi & Japanese":["Salmon Roll", "Tuna Nigiri", "Takoyaki", "Ramen"],
    "Korean Food":     ["Bibimbap", "Tteokbokki", "Korean Fried Chicken", "Japchae"],
    "Minuman & Jus":   ["Es Teh Manis", "Jus Alpukat", "Kopi Susu", "Thai Tea"],
    "Dessert & Snack": ["Martabak Manis", "Klepon", "Es Doger", "Churros"],
    "Sarapan":         ["Nasi Uduk Lengkap", "Bubur Ayam", "Roti Bakar", "Lontong Sayur"],
    "Vegetarian":      ["Gado-Gado", "Pecel", "Sayur Lodeh", "Tempe Bacem"],
    "Western":         ["Steak Ayam", "Fish & Chips", "BBQ Ribs"],
    "Padang":          ["Rendang", "Gulai Ikan", "Dendeng Balado", "Ayam Pop"],
    "Bakso & Soto":    ["Bakso Sapi", "Bakso Urat", "Soto Ayam", "Soto Betawi"]
}

products = []
for m in merchants:
    cat   = m["category"]
    items = FOOD_ITEMS.get(cat, ["Menu Spesial"])
    n_products = random.randint(4, 12)
    for j in range(n_products):
        item_name = random.choice(items)
        pid = str(uuid.uuid4())
        products.append({
            "product_id": pid,
            "merchant_id": m["merchant_id"],
            "product_name": f"{item_name} {random.choice(['Spesial','Komplit','Original','Super','Jumbo'])}",
            "category": cat,
            "price": random.choice([8000, 10000, 12000, 15000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000]),
            "is_available": random.choice([1, 1, 1, 0]),
            "created_at": m["joined_at"]
        })
product_ids = [p["product_id"] for p in products]
write_csv("products.csv", list(products[0].keys()), products)

# ─── 5. ORDERS & ORDER ITEMS ──────────────────────────────────────────────────
print("Generating orders...")
N_ORDERS = 12000
orders = []
order_items = []

for i in range(N_ORDERS):
    oid      = str(uuid.uuid4())
    user_id  = random.choice(user_ids)
    driver_id = random.choice(driver_ids)
    merchant = random.choice(merchants)
    mid      = merchant["merchant_id"]
    status   = random.choice(ORDER_STATUSES)
    order_dt = rand_date("2023-01-01", "2024-12-31")

    # harga
    m_products = [p for p in products if p["merchant_id"] == mid]
    if not m_products:
        m_products = random.sample(products, 3)
    selected = random.sample(m_products, min(random.randint(1, 4), len(m_products)))
    subtotal = sum(int(p["price"]) * random.randint(1, 3) for p in selected)
    delivery_fee = random.choice([2000, 3000, 5000, 7000, 10000])
    discount     = random.choice([0, 0, 0, 2000, 5000, 10000])
    total        = max(subtotal + delivery_fee - discount, 0)

    orders.append({
        "order_id": oid,
        "user_id": user_id,
        "driver_id": driver_id,
        "merchant_id": mid,
        "status": status,
        "payment_method": random.choice(PAYMENT_METHODS),
        "subtotal": subtotal,
        "delivery_fee": delivery_fee,
        "discount": discount,
        "total_amount": total,
        "order_time": order_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "delivered_time": (order_dt + timedelta(minutes=random.randint(15, 60))).strftime("%Y-%m-%d %H:%M:%S") if status == "delivered" else None,
        "delivery_lat": merchant["lat"],
        "delivery_lon": merchant["lon"],
        "delivery_area": merchant["area"]
    })

    # order items
    for p in selected:
        qty = random.randint(1, 3)
        order_items.append({
            "order_item_id": str(uuid.uuid4()),
            "order_id": oid,
            "product_id": p["product_id"],
            "merchant_id": mid,
            "product_name": p["product_name"],
            "category": p["category"],
            "quantity": qty,
            "unit_price": p["price"],
            "subtotal": int(p["price"]) * qty
        })

# ── Simulasi order duplikat: re-order yang sebenarnya cancel lalu re-order lagi
dup_orders = []
dup_items  = []
for _ in range(200):
    base_order = random.choice([o for o in orders if o["status"] == "delivered"])
    dup = base_order.copy()
    dup["order_id"] = str(uuid.uuid4())
    dup["status"]   = random.choice(["cancelled_by_customer", "cancelled_by_driver"])
    orig_time       = datetime.strptime(base_order["order_time"], "%Y-%m-%d %H:%M:%S")
    dup["order_time"] = (orig_time - timedelta(minutes=random.randint(5, 30))).strftime("%Y-%m-%d %H:%M:%S")
    dup["delivered_time"] = None
    dup_orders.append(dup)

    # duplikat order items
    base_items = [oi for oi in order_items if oi["order_id"] == base_order["order_id"]]
    for bi in base_items:
        ni = bi.copy()
        ni["order_item_id"] = str(uuid.uuid4())
        ni["order_id"]      = dup["order_id"]
        dup_items.append(ni)

orders.extend(dup_orders)
order_items.extend(dup_items)

write_csv("orders.csv", list(orders[0].keys()), orders)
write_csv("order_items.csv", list(order_items[0].keys()), order_items)

# ─── 6. REVIEWS ───────────────────────────────────────────────────────────────
print("Generating reviews...")
reviews = []
delivered_orders = [o for o in orders if o["status"] == "delivered"]
for o in random.sample(delivered_orders, min(5000, len(delivered_orders))):
    reviews.append({
        "review_id": str(uuid.uuid4()),
        "order_id": o["order_id"],
        "user_id": o["user_id"],
        "merchant_id": o["merchant_id"],
        "driver_id": o["driver_id"],
        "merchant_rating": random.randint(3, 5),
        "driver_rating": random.randint(3, 5),
        "comment": random.choice(["Enak banget!", "Recommended!", "Cepat sampai", "Lumayan", "Mantap jiwa", "Suka banget", ""]),
        "created_at": o["delivered_time"]
    })
write_csv("reviews.csv", list(reviews[0].keys()), reviews)

print(f"\n✅ Selesai! Semua CSV tersimpan di folder: {OUTPUT_DIR}/")
print(f"   Total orders (termasuk duplikat): {len(orders):,}")
print(f"   Total order items: {len(order_items):,}")