# import pandas as pd
# import numpy as np

# fact = pd.read_csv("../dataset/fact_transaction_with_location.csv")
# dim_merchant = pd.read_csv("../dataset/dim_merchant.csv")
# dim_customer = pd.read_csv("../dataset/dim_customer.csv")
# df = fact.merge(dim_merchant, on="merchant_id", suffixes=("", "_merchant"))
# df = df.merge(dim_customer, on="customer_id", suffixes=("", "_customer"))


# def haversine(lat1, lon1, lat2, lon2):
#     R = 6371  # km
#     lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

#     dlat = lat2 - lat1
#     dlon = lon2 - lon1

#     a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
#     c = 2 * np.arcsin(np.sqrt(a))

#     return R * c


# df["distance_km"] = haversine(
#     df["latitude"], df["longitude"], df["latitude_customer"], df["longitude_customer"]
# )

# print(df)



# import pandas as pd
# import numpy as np

# # load data
# fact = pd.read_csv("../dataset/fact_transaction.csv")
# merchant = pd.read_csv("../dataset/dim_merchant.csv")

# # merge merchant location
# df = fact.merge(merchant[['merchant_id', 'latitude', 'longitude']], on='merchant_id')

# # fungsi generate lokasi sekitar merchant
# def generate_delivery_location(lat, lon, radius_km=3):
#     radius_deg = radius_km / 111
#     delta_lat = np.random.uniform(-radius_deg, radius_deg)
#     delta_lon = np.random.uniform(-radius_deg, radius_deg)
#     return lat + delta_lat, lon + delta_lon

# # generate lokasi delivery
# df[['delivery_latitude', 'delivery_longitude']] = df.apply(
#     lambda row: pd.Series(generate_delivery_location(row['latitude'], row['longitude'])),
#     axis=1
# )

# # =========================
# # 🔥 Tambah data Jogja
# # =========================

# n_jogja = 100  # jumlah order baru

# jogja_lat = -7.7956
# jogja_lon = 110.3695

# def generate_jogja_location():
#     return generate_delivery_location(jogja_lat, jogja_lon, radius_km=5)

# jogja_data = pd.DataFrame({
#     'transaction_id': [f'JOGJA_{i}' for i in range(n_jogja)],
#     'merchant_id': np.random.choice(df['merchant_id'], n_jogja),
#     'customer_id': np.random.choice(df['customer_id'], n_jogja),
#     'driver_id': np.random.choice(df['driver_id'], n_jogja),
#     'subtotal': np.random.randint(20000, 100000, n_jogja),
#     'delivery_fee': np.random.randint(5000, 15000, n_jogja),
#     'total_payment': np.random.randint(30000, 120000, n_jogja),
#     'duration_est_min': np.random.randint(10, 40, n_jogja),
#     'duration_actual_min': np.random.randint(10, 60, n_jogja),
#     'keterlambatan_menit': np.random.randint(0, 20, n_jogja),
#     'rating_merchant': np.random.uniform(3.0, 5.0, n_jogja)
# })

# # generate lokasi jogja
# jogja_coords = [generate_jogja_location() for _ in range(n_jogja)]
# jogja_data['delivery_latitude'] = [c[0] for c in jogja_coords]
# jogja_data['delivery_longitude'] = [c[1] for c in jogja_coords]

# # =========================
# # 🔗 Gabungkan data
# # =========================
# final_df = pd.concat([df, jogja_data], ignore_index=True)

# # =========================
# # 💾 Simpan ke CSV
# # =========================
# final_df.to_csv("../dataset/fact_transaction_with_location.csv", index=False)

# print("✅ Data berhasil disimpan!")




