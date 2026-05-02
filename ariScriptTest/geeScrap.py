import ee
import pandas as pd

# ========================
# INIT GEE
# ========================
ee.Authenticate()
ee.Initialize()
# ========================
# KOORDINAT KOTA
# ========================
cities = {
    "DKI Jakarta": [106.8, -6.2],
    "Surabaya": [112.75, -7.25],
    "Medan": [98.67, 3.59]
}

# ========================
# PARAMETER
# ========================
START_DATE = "2023-01-01"
END_DATE = "2024-12-31"

# ERA5 hourly
dataset = (
    ee.ImageCollection("ECMWF/ERA5_LAND/HOURLY")
    .filterDate(START_DATE, END_DATE)
    .select([
        "temperature_2m",
        "total_precipitation",
        "relative_humidity_2m"
    ])
)

# ========================
# FUNGSI KATEGORI CUACA
# ========================
def classify_weather(temp_k, rain, humidity):
    temp_c = temp_k - 273.15 if temp_k else None

    if rain and rain > 0.002:
        return "Hujan"
    elif humidity and humidity > 85:
        return "Berawan"
    elif temp_c and temp_c > 30:
        return "Panas"
    else:
        return "Cerah"

# ========================
# EXTRACT DATA
# ========================
all_rows = []

for city_name, coord in cities.items():
    print(f"Ambil: {city_name}")
    
    point = ee.Geometry.Point(coord)

    def extract(image):
        stats = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=point,
            scale=1000
        )
        
        return ee.Feature(None, {
            "time": image.date().format(),
            "temp": stats.get("temperature_2m"),
            "rain": stats.get("total_precipitation"),
            "humidity": stats.get("relative_humidity_2m"),
            "city": city_name
        })

    features = dataset.map(extract).getInfo()

    for f in features["features"]:
        prop = f["properties"]

        cuaca = classify_weather(
            prop.get("temp"),
            prop.get("rain"),
            prop.get("humidity")
        )

        all_rows.append({
            "wilayah": prop.get("city"),
            "waktu": prop.get("time"),
            "cuaca": cuaca
        })

# ========================
# DATAFRAME
# ========================
df = pd.DataFrame(all_rows)

# ========================
# PREPROCESSING WAKTU
# ========================
df["waktu"] = pd.to_datetime(df["waktu"])

# floor ke jam
df["waktu_group"] = df["waktu"].dt.floor("h")

# ========================
# MODE CUACA
# ========================
def mode_cuaca(x):
    m = x.mode()
    return m.iloc[0] if not m.empty else None

df_final = (
    df.groupby(["wilayah", "waktu_group"])["cuaca"]
    .apply(mode_cuaca)
    .reset_index()
)

df_final.columns = ["wilayah", "waktu", "cuaca"]

# ========================
# SIMPAN
# ========================
df_final.to_csv("cuaca_gee_mode.csv", index=False)

print("Selesai:", len(df_final))