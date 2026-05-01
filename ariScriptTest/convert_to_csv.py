import pandas as pd
from sqlalchemy import create_engine

# koneksi ke DWH
DWH_URL = "mysql+pymysql://root:@192.168.144.1:3306/dwh_uts"
engine = create_engine(DWH_URL)

# daftar tabel
tables = ["dim_date", "dim_merchant", "dim_user", "fact_orders"]

# loop extract + save ke csv
for table in tables:
    query = f"SELECT * FROM {table} LIMIT 20"
    df = pd.read_sql(query, engine)

    filename = f"{table}_top20.csv"
    df.to_csv(filename, index=False)

    print(f"✓ {filename} berhasil dibuat ({len(df)} rows)")