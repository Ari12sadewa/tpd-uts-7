from sqlalchemy import create_engine
import pandas as pd
import os

MYSQL_SRC_URL = "mysql+pymysql://root:@192.168.144.1:3306/source_a_uts"
PG_SRC_URL = "postgresql+psycopg2://postgres:12345@192.168.144.1:5432/source_b_uts"
MYSQL_DWH_SRC_URL = "mysql+pymysql://root:@192.168.144.1:3306/dwh_uts"

OUTPUT_DIR = "sample_output"

def extract_sample_to_csv():
    # ========================
    # Create folder if not exist
    # ========================
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    engine_mysql = create_engine(MYSQL_SRC_URL)
    engine_pg = create_engine(PG_SRC_URL)
    engine_dwh = create_engine(MYSQL_DWH_SRC_URL)

    tables = {
        # Source A
        "users": ("SELECT * FROM users LIMIT 20", engine_mysql),
        "drivers": ("SELECT * FROM drivers LIMIT 20", engine_mysql),

        # Source B
        "merchants": ("SELECT * FROM merchants LIMIT 20", engine_pg),
        "products": ("SELECT * FROM products LIMIT 20", engine_pg),
        "orders": ("SELECT * FROM orders LIMIT 20", engine_pg),
        "order_items": ("SELECT * FROM order_items LIMIT 20", engine_pg),
        "reviews": ("SELECT * FROM reviews LIMIT 20", engine_pg),

        #DWH
        "dim_date": ("SELECT * FROM dim_date LIMIT 20", engine_dwh),
        "dim_driver": ("SELECT * FROM dim_driver LIMIT 20", engine_dwh),
        "dim_merchant": ("SELECT * FROM dim_merchant LIMIT 20", engine_dwh),
        "dim_product": ("SELECT * FROM dim_product LIMIT 20", engine_dwh),
        "dim_user": ("SELECT * FROM dim_user LIMIT 20", engine_dwh),
        "dim_weather": ("SELECT * FROM dim_weather LIMIT 20", engine_dwh),
        "fact_orders": ("SELECT * FROM fact_orders LIMIT 20", engine_dwh),
        "fact_order_items": ("SELECT * FROM fact_order_items LIMIT 20", engine_dwh),
    }

    for name, (query, engine) in tables.items():
        df = pd.read_sql(query, engine)

        # simpan ke folder
        file_path = os.path.join(OUTPUT_DIR, f"{name}.csv")
        df.to_csv(file_path, index=False)

        print(f"✓ {name}: {len(df)} rows → {file_path}")

extract_sample_to_csv()