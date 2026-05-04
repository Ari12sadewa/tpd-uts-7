import ee
import pandas as pd
from datetime import datetime, timedelta

# ========================
# INIT GEE
# ========================
ee.Initialize(project='paradokstesting')

# ========================
# KOORDINAT KOTA
# ========================
cities = {
    "DKI Jakarta": [106.8, -6.2],
    "Surabaya": [112.75, -7.25],
    "Medan": [98.67, 3.59]
}

# ========================
# PARAMETER WAKTU
# ========================
START_YEAR = 2023
END_YEAR = 2024

# Membuat daftar rentang bulan (untuk mengakali limit 5000 GEE)
date_ranges = []
for year in range(START_YEAR, END_YEAR + 1):
    for month in range(1, 13):
        start = f"{year}-{month:02d}-01"
        # Logika untuk mendapatkan akhir bulan
        if month == 12:
            end = f"{year+1}-01-01"
        else:
            end = f"{year}-{month+1:02d}-01"
        date_ranges.append((start, end))

# ========================
# FUNGSI-FUNGSI GEE
# ========================
def hitung_rh(image):
    temp = image.select('temperature_2m').subtract(273.15)
    dew = image.select('dewpoint_temperature_2m').subtract(273.15)
    rh = image.expression(
        '100 * (exp((17.625 * td) / (243.04 + td)) / exp((17.625 * t) / (243.04 + t)))', {
            't': temp, 'td': dew
        }).rename('relative_humidity_2m')
    return image.addBands(rh)

def classify_weather(temp_k, rain, humidity):
    temp_c = temp_k - 273.15 if temp_k else None
    if rain and rain > 0.002: return "Hujan"
    elif humidity and humidity > 85: return "Berawan"
    elif temp_c and temp_c > 30: return "Panas"
    else: return "Cerah"

# ========================
# EKSTRAKSI DATA (LOOPING PER BULAN)
# ========================
all_rows = []

for city_name, coord in cities.items():
    print(f"\n--- Memulai: {city_name} ---")
    point = ee.Geometry.Point(coord)

    for start_d, end_d in date_ranges:
        print(f"  Mengambil periode: {start_d} s/d {end_d}")
        
        # Filter koleksi per bulan
        dataset = (
            ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
            .filterDate(start_d, end_d)
            .select(["temperature_2m", "total_precipitation", "dewpoint_temperature_2m"])
            .map(hitung_rh)
        )

        def extract(image):
            stats = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=point,
                scale=11132
            )
            return ee.Feature(None, {
                "time": image.date().format(),
                "temp": stats.get("temperature_2m"),
                "rain": stats.get("total_precipitation"),
                "humidity": stats.get("relative_humidity_2m")
            })

        try:
            # Ambil data bulan ini
            features = dataset.map(extract).getInfo()

            for f in features["features"]:
                prop = f["properties"]
                cuaca = classify_weather(prop.get("temp"), prop.get("rain"), prop.get("humidity"))
                
                all_rows.append({
                    "wilayah": city_name,
                    "waktu": prop.get("time"),
                    "cuaca": cuaca
                })
        except Exception as e:
            print(f"    Gagal pada periode {start_d}: {e}")
            continue

# ========================
# SIMPAN DATA
# ========================
if all_rows:
    df = pd.DataFrame(all_rows)
    df["waktu"] = pd.to_datetime(df["waktu"])
    
    # Pengelompokan dan Mode (opsional jika data sudah per jam)
    df_final = (
        df.groupby(["wilayah", pd.Grouper(key="waktu", freq="h")])["cuaca"]
        .first() # Karena data sudah per jam, ambil yang pertama saja
        .reset_index()
    )

    df_final.to_csv("cuaca_gee_mode.csv", index=False)
    print(f"\nSelesai! Total data: {len(df_final)} baris disimpan ke cuaca_gee_mode.csv")
else:
    print("\nTidak ada data yang berhasil diambil.")