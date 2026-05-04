import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from datetime import datetime

print("="*60)
print("  🚀 MEMULAI PIPELINE ETL DENGAN PANDAS (UTS)")
print("="*60)

# ==========================================
# 0. KONFIGURASI KONEKSI DATABASE
# ==========================================
# (UBAH PASSWORD POSTGRESQL DI BAWAH INI SESUAI MILIKMU)
pg_password = urllib.parse.quote_plus("12345") # <-- UBAH INI

MYSQL_SRC_URL = "mysql+pymysql://root:@localhost:3306/source_a_uts"
PG_SRC_URL = f"postgresql+psycopg2://postgres:12345@localhost:5432/source_b_uts"
# MYSQL_DWH_URL = "mysql+pymysql://root:@localhost:3306/dwh"

engine_mysql = create_engine(MYSQL_SRC_URL)
engine_pg = create_engine(PG_SRC_URL)
# engine_dwh = create_engine(MYSQL_DWH_URL)

df = pd.read_sql("SELECT * FROM dim_merchant",PG_SRC_URL)

try:
    # ==========================================
    # 1. EXTRACT (Tarik Data dari Source A & B)
    # ==========================================
    print("\n[1/3] EXTRACT: Menarik data dari PostgreSQL dan MySQL...")
    df_merchant = pd.read_sql("SELECT * FROM dim_merchant", engine_pg)
    df_product = pd.read_sql("SELECT * FROM dim_product", engine_pg)
    df_customer = pd.read_sql("SELECT * FROM dim_customer", engine_mysql)
    df_driver = pd.read_sql("SELECT * FROM dim_driver", engine_mysql)
    df_time = pd.read_sql("SELECT * FROM dim_time", engine_mysql)
    df_fact = pd.read_sql("SELECT * FROM fact_transaction", engine_mysql)
    print("      ✓ Data berhasil ditarik ke dalam memori.")

    # ==========================================
    # 2. TRANSFORM (Pembersihan & Pengayaan Data)
    # ==========================================
    print("\n[2/3] TRANSFORM: Melakukan pembersihan data...")
    
    # A. Mengisi nilai kosong (NULL) pada rating transaksi yang dibatalkan
    df_fact['rating_merchant'] = df_fact['rating_merchant'].fillna(0.0)
    df_fact['rating_driver'] = df_fact['rating_driver'].fillna(0.0)
    
    # B. Menghapus data transaksi yang duplikat (jika ada)
    df_fact = df_fact.drop_duplicates(subset=['transaction_id'])
    
    # C. Standardisasi nama kota (Huruf kapital di awal kata)
    df_customer['city'] = df_customer['city'].str.title()
    
    # D. Menambahkan waktu eksekusi ETL (Auditing)
    load_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_merchant['dwh_load_time'] = load_timestamp
    df_product['dwh_load_time'] = load_timestamp
    df_customer['dwh_load_time'] = load_timestamp
    df_driver['dwh_load_time'] = load_timestamp
    df_time['dwh_load_time'] = load_timestamp
    df_fact['dwh_load_time'] = load_timestamp
    print("      ✓ Transformasi berhasil. Data siap dikirim.")

    # ==========================================
    # 3. LOAD (Simpan ke Data Warehouse)
    # ==========================================
    print("\n[3/3] LOAD: Menyimpan data yang sudah bersih ke DWH...")
    df_merchant.to_sql("dim_merchant", engine_dwh, if_exists="replace", index=False)
    df_product.to_sql("dim_product", engine_dwh, if_exists="replace", index=False)
    df_customer.to_sql("dim_customer", engine_dwh, if_exists="replace", index=False)
    df_driver.to_sql("dim_driver", engine_dwh, if_exists="replace", index=False)
    df_time.to_sql("dim_time", engine_dwh, if_exists="replace", index=False)
    df_fact.to_sql("fact_transaction", engine_dwh, if_exists="replace", index=False)

    print("\n✅ PIPELINE SELESAI! Seluruh data berhasil mendarat di Data Warehouse (dwh).")
    print("="*60)

except Exception as e:
    print(f"\n[ERROR] ETL Gagal: {e}")