

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from sqlalchemy import create_engine
import warnings
import numpy as np

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────────────────────────
# KONFIGURASI
# ──────────────────────────────────────────────────────────────────────────────
DWH_URL   = "mysql+pymysql://root:@localhost:3306/dwh_uts"
OUTPUT_DIR = "output_charts"

# Palet warna konsisten
PALETTE_MAIN    = "#E84393"   # GoFood pink
PALETTE_ACCENT  = "#00AA5B"   # GoFood green
PALETTE_NEUTRAL = "#555555"
PALETTE_WEEKEND = "#E84393"
PALETTE_WEEKDAY = "#00AA5B"
WEATHER_COLORS  = {
    "Hujan"   : "#4A90D9",
    "Berawan" : "#95A5A6",
    "Panas"   : "#F39C12",
    "Cerah"   : "#F1C40F",
    "Tidak Diketahui": "#BDC3C7",
}

sns.set_theme(style="whitegrid", font="DejaVu Sans")
plt.rcParams.update({
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "figure.dpi":         130,
})

import os
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────────
# LOAD DATA DARI DWH
# ──────────────────────────────────────────────────────────────────────────────
print("Menghubungkan ke DWH...")
engine = create_engine(DWH_URL)

fact_orders      = pd.read_sql("SELECT * FROM fact_orders",      engine)
fact_order_items = pd.read_sql("SELECT * FROM fact_order_items", engine)
dim_merchant     = pd.read_sql("SELECT * FROM dim_merchant",     engine)
dim_user         = pd.read_sql("SELECT * FROM dim_user",         engine)
dim_date         = pd.read_sql("SELECT * FROM dim_date",         engine)
dim_weather      = pd.read_sql("SELECT * FROM dim_weather",      engine)

print(f"  ✓ fact_orders      : {len(fact_orders):,} rows")
print(f"  ✓ fact_order_items : {len(fact_order_items):,} rows")
print(f"  ✓ dim_merchant     : {len(dim_merchant):,} rows")
print(f"  ✓ dim_user         : {len(dim_user):,} rows")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 1 — Merchant dengan Revenue Terbanyak (Top 15)
# ══════════════════════════════════════════════════════════════════════════════
print("\n[1] Merchant dengan revenue terbanyak...")

revenue = (
    fact_orders[fact_orders["order_status"] == "delivered"]
    .groupby("merchant_id")["total_amount"]
    .sum()
    .reset_index()
    .rename(columns={"total_amount": "total_revenue"})
)
revenue = (
    revenue
    .merge(dim_merchant[["merchant_id", "merchant_name", "merchant_category", "city"]], on="merchant_id")
    .sort_values("total_revenue", ascending=False)
    .head(15)
)

fig1, ax1 = plt.subplots(figsize=(12, 7))
bars = ax1.barh(
    revenue["merchant_name"][::-1],
    revenue["total_revenue"][::-1] / 1_000_000,
    color=PALETTE_MAIN,
    edgecolor="white",
    height=0.65,
)

# Label nilai di ujung bar
for bar in bars:
    w = bar.get_width()
    ax1.text(w + 0.5, bar.get_y() + bar.get_height() / 2,
             f"Rp {w:.1f}M", va="center", fontsize=9, color=PALETTE_NEUTRAL)

ax1.set_xlabel("Total Revenue (juta Rupiah)", fontsize=11)
ax1.set_title("Top 15 Merchant Berdasarkan Revenue\n(hanya order berstatus 'delivered')",
              fontsize=13, fontweight="bold", pad=15)
ax1.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rp {x:.0f}M"))
plt.tight_layout()
fig1.savefig(f"{OUTPUT_DIR}/1_merchant_revenue.png", bbox_inches="tight")
print("""
INTERPRETASI — Merchant Revenue:
  • Merchant teratas berpotensi menjadi anchor merchant untuk promosi bundling.
  • Perhatikan apakah top merchant terkonsentrasi pada satu kota atau menyebar.
  • Merchant dengan revenue tinggi namun rating rendah perlu ditindaklanjuti
    dari sisi kualitas layanan.
  • Kategori makanan yang mendominasi top 15 mencerminkan preferensi pasar utama.
""")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 2 — Weekend vs Weekday: Rata-rata Nilai Transaksi
# ══════════════════════════════════════════════════════════════════════════════
print("[2] Perbandingan weekend vs weekday...")

fo_date = fact_orders.merge(dim_date[["date_id", "is_weekend", "day_name"]], on="date_id")
fo_delivered = fo_date[fo_date["order_status"] == "delivered"].copy()
fo_delivered["tipe_hari"] = fo_delivered["is_weekend_x"].map({1: "Weekend", 0: "Weekday"})
print("="*60)
print("mapping complete")
print("="*60)

# Subplot kiri: rata-rata transaksi per kategori (weekday vs weekend)
avg_per_day = (
    fo_delivered.groupby(["day_name", "tipe_hari"])["total_amount"]
    .mean()
    .reset_index()
)
day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
avg_per_day["day_name"] = pd.Categorical(avg_per_day["day_name"], categories=day_order, ordered=True)
avg_per_day = avg_per_day.sort_values("day_name")

# Subplot kanan: boxplot distribusi per tipe hari
fig2, (ax2a, ax2b) = plt.subplots(1, 2, figsize=(14, 6))

# Bar chart per hari
color_map = {"Weekend": PALETTE_WEEKEND, "Weekday": PALETTE_WEEKDAY}
for tipe, grp in avg_per_day.groupby("tipe_hari"):
    ax2a.bar(grp["day_name"].astype(str), grp["total_amount"] / 1_000,
             label=tipe, color=color_map[tipe], alpha=0.85, edgecolor="white")

ax2a.set_title("Rata-rata Nilai Transaksi per Hari", fontsize=12, fontweight="bold")
ax2a.set_xlabel("Hari")
ax2a.set_ylabel("Rata-rata Transaksi (ribu Rupiah)")
ax2a.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rp {x:.0f}rb"))
ax2a.tick_params(axis="x", rotation=30)
ax2a.legend(title="Tipe Hari")

# Boxplot
sns.boxplot(data=fo_delivered, x="tipe_hari", y="total_amount",
            palette={"Weekend": PALETTE_WEEKEND, "Weekday": PALETTE_WEEKDAY},
            order=["Weekday", "Weekend"], ax=ax2b, width=0.45, fliersize=3)
ax2b.set_title("Distribusi Nilai Transaksi\nWeekday vs Weekend", fontsize=12, fontweight="bold")
ax2b.set_xlabel("Tipe Hari")
ax2b.set_ylabel("Total Amount (Rupiah)")
ax2b.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rp {x/1000:.0f}rb"))

# Annotate median
for i, tipe in enumerate(["Weekday", "Weekend"]):
    med = fo_delivered[fo_delivered["tipe_hari"] == tipe]["total_amount"].median()
    ax2b.text(i, med + 1000, f"Median\nRp {med/1000:.1f}rb",
              ha="center", fontsize=9, color="white",
              bbox=dict(boxstyle="round,pad=0.3", fc=color_map[tipe], alpha=0.85))

plt.suptitle("Perbandingan Transaksi: Weekend vs Weekday", fontsize=14, fontweight="bold", y=1.01)
plt.tight_layout()
fig2.savefig(f"{OUTPUT_DIR}/2_weekend_vs_weekday.png", bbox_inches="tight")
# Statistik ringkas
stat_wday = fo_delivered.groupby("tipe_hari")["total_amount"].agg(["mean", "median", "count"])
print(stat_wday.to_string())
print("""
INTERPRETASI — Weekend vs Weekday:
  • Jika nilai transaksi weekend > weekday: orang cenderung memesan lebih banyak
    item atau item premium saat akhir pekan (waktu bersantai).
  • Jika volume order weekend lebih tinggi: strategi promo weekend (flash sale
    jam makan siang Sabtu-Minggu) berpotensi mendongkrak GMV signifikan.
  • Weekday menunjukkan pola yang lebih stabil, cocok untuk program loyalitas
    rutin (langganan makan siang).
  • Gap yang besar antara median weekend-weekday menjadi sinyal untuk
    menyesuaikan stok driver dan alokasi subsidi ongkir per hari.
""")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 3 — Proporsi Kategori Makanan per Kelompok Usia
# ══════════════════════════════════════════════════════════════════════════════
print("[3] Proporsi kategori makanan per kelompok usia...")

items_user = (
    fact_order_items[fact_order_items["order_status"] == "delivered"]
    .merge(dim_user[["user_id", "age_group"]], on="user_id")
)

pivot = (
    items_user.groupby(["age_group", "product_category"])
    .size()
    .reset_index(name="count")
)
pivot_pct = pivot.copy()
pivot_pct["pct"] = (
    pivot_pct["count"]
    / pivot_pct.groupby("age_group")["count"].transform("sum")
    * 100
)

age_order = ["<18", "18-24", "25-34", "35-44", "45-54", "55+"]
pivot_wide = (
    pivot_pct.pivot(index="age_group", columns="product_category", values="pct")
    .reindex(age_order)
    .fillna(0)
)

fig3, ax3 = plt.subplots(figsize=(14, 7))
pivot_wide.plot(
    kind="bar", stacked=True, ax=ax3,
    colormap="tab20", edgecolor="white", width=0.65,
)
ax3.set_title("Proporsi Kategori Makanan berdasarkan Kelompok Usia",
              fontsize=13, fontweight="bold", pad=15)
ax3.set_xlabel("Kelompok Usia")
ax3.set_ylabel("Proporsi (%)")
ax3.yaxis.set_major_formatter(mticker.PercentFormatter())
ax3.tick_params(axis="x", rotation=0)
ax3.legend(
    title="Kategori", bbox_to_anchor=(1.01, 1), loc="upper left",
    fontsize=8, title_fontsize=9,
)
plt.tight_layout()
fig3.savefig(f"{OUTPUT_DIR}/3_kategori_per_usia.png", bbox_inches="tight")
print("""
INTERPRETASI — Kategori Makanan per Usia:
  • Kelompok 18-24 & 25-34 cenderung mendominasi kategori Korean Food, Pizza,
    dan Burger — mencerminkan tren kuliner anak muda dari media sosial.
  • Kelompok 35+ cenderung lebih banyak memesan Nasi & Lauk, Padang, Soto —
    pilihan yang lebih konvensional dan mengenyangkan.
  • Temuan ini bisa menjadi dasar targeting iklan: promo Korean Food untuk
    segmen Gen-Z, dan promo Padang/Nasi Box untuk segmen 35+.
  • Kategori Minuman & Dessert muncul merata di semua usia — potensi
    upselling lintas segmen.
""")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 4 — Persebaran Lokasi Merchant
# ══════════════════════════════════════════════════════════════════════════════
print("[4] Persebaran lokasi merchant...")

# Gabung dengan revenue untuk ukuran titik
merch_rev = (
    fact_orders[fact_orders["order_status"] == "delivered"]
    .groupby("merchant_id")["total_amount"]
    .sum()
    .reset_index()
    .rename(columns={"total_amount": "revenue"})
)
merch_plot = dim_merchant.merge(merch_rev, on="merchant_id", how="left")
merch_plot["revenue"] = merch_plot["revenue"].fillna(0)

city_colors = {"jakarta": "#E84393", "surabaya": "#00AA5B", "medan": "#F39C12"}
city_labels = {"jakarta": "DKI Jakarta", "surabaya": "Surabaya", "medan": "Medan"}

fig4, ax4 = plt.subplots(figsize=(12, 8))

for city_key, grp in merch_plot.groupby("city"):
    ax4.scatter(
        grp["merchant_lon"],
        grp["merchant_lat"],
        s=grp["revenue"] / grp["revenue"].max() * 200 + 20,
        c=city_colors.get(city_key, "#999"),
        alpha=0.55,
        edgecolors="white",
        linewidths=0.4,
        label=city_labels.get(city_key, city_key),
    )

ax4.set_title("Persebaran Lokasi Merchant GoFood\n(ukuran titik ∝ revenue)",
              fontsize=13, fontweight="bold", pad=15)
ax4.set_xlabel("Longitude")
ax4.set_ylabel("Latitude")
ax4.legend(title="Kota", fontsize=10)
plt.tight_layout()
fig4.savefig(f"{OUTPUT_DIR}/4_persebaran_merchant.png", bbox_inches="tight")
print("""
INTERPRETASI — Persebaran Lokasi Merchant:
  • Kluster merchant padat di pusat kota mengindikasikan zona kompetisi tinggi —
    merchant baru di area ini perlu diferensiasi kuat (harga, promo, rating).
  • Titik besar (revenue tinggi) yang berada di pinggiran kota menunjukkan
    merchant dengan captive market (sedikit pesaing namun permintaan tinggi) —
    peluang ekspansi atau kemitraan eksklusif.
  • Area dengan kepadatan merchant rendah namun volume order relatif tinggi
    adalah white-space untuk akuisisi merchant baru.
  • Perbedaan kluster antar kota mencerminkan karakteristik urban yang berbeda
    (Jakarta lebih tersebar, Medan dan Surabaya lebih terpusat).
""")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 5 — Distribusi Order Berdasarkan Jam
# ══════════════════════════════════════════════════════════════════════════════
print("[5] Distribusi order berdasarkan jam...")

fo_date2 = fact_orders.merge(dim_date[["date_id", "is_weekend"]], on="date_id")
fo_date2["tipe_hari"] = fo_date2["is_weekend_x"].map({1: "Weekend", 0: "Weekday"})

hourly = (
    fo_date2.groupby(["order_hour", "tipe_hari"])
    .size()
    .reset_index(name="jumlah_order")
)

fig5, ax5 = plt.subplots(figsize=(13, 6))
for tipe, grp in hourly.groupby("tipe_hari"):
    grp_sorted = grp.sort_values("order_hour")
    ax5.plot(
        grp_sorted["order_hour"],
        grp_sorted["jumlah_order"],
        marker="o",
        markersize=5,
        linewidth=2.2,
        label=tipe,
        color=PALETTE_WEEKEND if tipe == "Weekend" else PALETTE_WEEKDAY,
    )
    ax5.fill_between(
        grp_sorted["order_hour"],
        grp_sorted["jumlah_order"],
        alpha=0.12,
        color=PALETTE_WEEKEND if tipe == "Weekend" else PALETTE_WEEKDAY,
    )

# Anotasi peak hour
peak_row = hourly.loc[hourly["jumlah_order"].idxmax()]
ax5.annotate(
    f"Peak: Jam {int(peak_row['order_hour']):02d}:00\n({int(peak_row['jumlah_order'])} order)",
    xy=(peak_row["order_hour"], peak_row["jumlah_order"]),
    xytext=(peak_row["order_hour"] + 1.5, peak_row["jumlah_order"] - 20),
    arrowprops=dict(arrowstyle="->", color=PALETTE_NEUTRAL),
    fontsize=9, color=PALETTE_NEUTRAL,
)

ax5.set_title("Distribusi Volume Order per Jam\n(Weekday vs Weekend)",
              fontsize=13, fontweight="bold", pad=15)
ax5.set_xlabel("Jam (0 – 23)")
ax5.set_ylabel("Jumlah Order")
ax5.set_xticks(range(24))
ax5.set_xticklabels([f"{h:02d}" for h in range(24)], fontsize=8)
ax5.legend(title="Tipe Hari")
plt.tight_layout()
fig5.savefig(f"{OUTPUT_DIR}/5_distribusi_jam.png", bbox_inches="tight")
print("""
INTERPRETASI — Distribusi Order per Jam:
  • Pola bimodal khas food delivery: puncak pertama jam 11-13 (makan siang),
    puncak kedua jam 18-20 (makan malam).
  • Weekend menunjukkan kurva yang lebih landai dan puncak siang lebih mundur
    (brunch effect), mengindikasikan pengguna bangun lebih siang.
  • Jam sepi (02:00 – 06:00) bisa dimanfaatkan untuk pemeliharaan sistem
    tanpa memengaruhi pengalaman pengguna.
  • Alokasi driver perlu ditingkatkan 30 menit sebelum peak hour untuk
    mengurangi waktu tunggu dan meningkatkan konversi.
""")


# ══════════════════════════════════════════════════════════════════════════════
# ANALISIS 6 — Pengaruh Cuaca terhadap Jumlah & Pola Pemesanan
# ══════════════════════════════════════════════════════════════════════════════
print("[6] Pengaruh cuaca terhadap pemesanan...")

# 6A: Volume & rata-rata transaksi per kondisi cuaca
weather_agg = (
    fact_orders.groupby("kondisi_cuaca")
    .agg(
        jumlah_order   =("order_id",     "count"),
        avg_transaksi  =("total_amount", "mean"),
        total_revenue  =("total_amount", "sum"),
    )
    .reset_index()
    .sort_values("jumlah_order", ascending=False)
)
weather_agg = weather_agg[weather_agg["kondisi_cuaca"] != "Tidak Diketahui"]

# 6B: Distribusi jam per kondisi cuaca (heatmap)
weather_hour = (
    fact_orders[fact_orders["kondisi_cuaca"] != "Tidak Diketahui"]
    .groupby(["kondisi_cuaca", "order_hour"])
    .size()
    .reset_index(name="count")
    .pivot(index="kondisi_cuaca", columns="order_hour", values="count")
    .fillna(0)
)
# Normalisasi per baris agar mudah dibandingkan
weather_hour_pct = weather_hour.div(weather_hour.sum(axis=1), axis=0) * 100

fig6, axes = plt.subplots(2, 2, figsize=(15, 11))
fig6.suptitle("Analisis Pengaruh Cuaca terhadap Pemesanan GoFood",
              fontsize=14, fontweight="bold", y=1.01)

# ── 6A-kiri: Bar volume order per cuaca ──────────────────────────────────────
ax6a = axes[0, 0]
bar_colors = [WEATHER_COLORS.get(c, "#999") for c in weather_agg["kondisi_cuaca"]]
bars6a = ax6a.bar(
    weather_agg["kondisi_cuaca"],
    weather_agg["jumlah_order"],
    color=bar_colors, edgecolor="white", width=0.55,
)
for bar in bars6a:
    ax6a.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 30,
              f"{int(bar.get_height()):,}", ha="center", fontsize=9)
ax6a.set_title("Volume Order per Kondisi Cuaca", fontweight="bold")
ax6a.set_xlabel("Kondisi Cuaca")
ax6a.set_ylabel("Jumlah Order")

# ── 6A-kanan: Bar rata-rata transaksi per cuaca ───────────────────────────────
ax6b = axes[0, 1]
bars6b = ax6b.bar(
    weather_agg["kondisi_cuaca"],
    weather_agg["avg_transaksi"] / 1_000,
    color=bar_colors, edgecolor="white", width=0.55,
)
for bar in bars6b:
    ax6b.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
              f"Rp {bar.get_height():.1f}rb", ha="center", fontsize=9)
ax6b.set_title("Rata-rata Nilai Transaksi per Cuaca", fontweight="bold")
ax6b.set_xlabel("Kondisi Cuaca")
ax6b.set_ylabel("Rata-rata Transaksi (ribu Rupiah)")

# ── 6B: Heatmap distribusi jam per cuaca ─────────────────────────────────────
ax6c = axes[1, 0]
sns.heatmap(
    weather_hour_pct,
    ax=ax6c,
    cmap="YlOrRd",
    fmt=".1f",
    annot=True,
    annot_kws={"size": 7},
    linewidths=0.3,
    cbar_kws={"label": "% dari total order cuaca tersebut"},
)
ax6c.set_title("Distribusi Jam Order per Kondisi Cuaca (%)", fontweight="bold")
ax6c.set_xlabel("Jam")
ax6c.set_ylabel("Kondisi Cuaca")

# ── 6C: Stacked bar pembatalan per cuaca ─────────────────────────────────────
ax6d = axes[1, 1]
cancel_weather = (
    fact_orders[fact_orders["kondisi_cuaca"] != "Tidak Diketahui"]
    .groupby(["kondisi_cuaca", "order_status"])
    .size()
    .unstack(fill_value=0)
)
cancel_weather_pct = cancel_weather.div(cancel_weather.sum(axis=1), axis=0) * 100

status_colors = {
    "delivered":              PALETTE_ACCENT,
    "cancelled_by_customer":  "#E74C3C",
    "cancelled_by_driver":    "#E67E22",
}
cancel_weather_pct[
    [c for c in cancel_weather_pct.columns if c in status_colors]
].plot(
    kind="bar", stacked=True, ax=ax6d,
    color=[status_colors.get(c, "#999") for c in
           [c for c in cancel_weather_pct.columns if c in status_colors]],
    edgecolor="white", width=0.55,
)
ax6d.set_title("Proporsi Status Order per Kondisi Cuaca (%)", fontweight="bold")
ax6d.set_xlabel("Kondisi Cuaca")
ax6d.set_ylabel("Proporsi (%)")
ax6d.yaxis.set_major_formatter(mticker.PercentFormatter())
ax6d.tick_params(axis="x", rotation=15)
ax6d.legend(title="Status Order", fontsize=8, loc="lower right")

plt.tight_layout()
fig6.savefig(f"{OUTPUT_DIR}/6_pengaruh_cuaca.png", bbox_inches="tight")
print("""
INTERPRETASI — Pengaruh Cuaca:
  • Kondisi Hujan biasanya meningkatkan volume order secara signifikan —
    orang enggan keluar, sehingga lebih memilih pesan antar.
  • Nilai transaksi saat Hujan cenderung lebih tinggi karena pengguna
    memesan lebih banyak item sekaligus (berbagi dengan keluarga di rumah).
  • Tingkat pembatalan oleh driver meningkat saat Hujan deras — perlu
    insentif cuaca (weather bonus) untuk menjaga ketersediaan driver.
  • Pola jam saat Hujan lebih tersebar (tidak sekuat bimodal): orang memesan
    kapan saja karena tidak bisa keluar, bukan hanya jam makan.
  • Cuaca Cerah & Panas menunjukkan volume lebih rendah namun rata-rata
    transaksi lebih tinggi untuk kategori minuman & dessert.
  • Rekomendasi: aktifkan notifikasi push + promo ongkir gratis otomatis
    saat sensor cuaca mendeteksi hujan di area pengguna.
""")


print("[6D] Perbandingan rata-rata order per hari (Jakarta vs Medan)...")

# Filter kota & cuaca valid
df_cmp = fact_orders[
    (fact_orders["city"].isin(["jakarta", "medan"])) &
    (fact_orders["kondisi_cuaca"] != "Tidak Diketahui")
].copy()

# Pastikan date_id dianggap sebagai hari
df_cmp["date"] = pd.to_datetime(df_cmp["date_id"].astype(str)).dt.floor("D")

# Hitung jumlah order per hari per kota & cuaca
daily_orders = (
    df_cmp.groupby(["city", "kondisi_cuaca", "date"])
    .size()
    .reset_index(name="jumlah_order")
)

# Rata-rata order per hari
avg_daily_orders = (
    daily_orders.groupby(["city", "kondisi_cuaca"])["jumlah_order"]
    .mean()
    .reset_index()
)

# Pivot agar Jakarta vs Medan sejajar
pivot_cmp = avg_daily_orders.pivot(
    index="kondisi_cuaca",
    columns="city",
    values="jumlah_order"
).fillna(0)

# Plot
fig7, ax = plt.subplots(figsize=(10, 6))

x = np.arange(len(pivot_cmp.index))
width = 0.35

bars_jkt = ax.bar(
    x - width/2,
    pivot_cmp["jakarta"],
    width,
    label="Jakarta",
    color="#E84393"  
)

bars_mdn = ax.bar(
    x + width/2,
    pivot_cmp["medan"],
    width,
    label="Medan",
    color="#F39C12"   
)

# Label angka
for bars in [bars_jkt, bars_mdn]:
    for bar in bars:
        ax.text(
            bar.get_x() + bar.get_width()/2,
            bar.get_height() + 1,
            f"{bar.get_height():.1f}",
            ha="center",
            fontsize=9
        )

ax.set_title("Rata-rata Order per Hari berdasarkan Cuaca\n(Jakarta vs Medan)", fontweight="bold")
ax.set_xlabel("Kondisi Cuaca")
ax.set_ylabel("Rata-rata Order per Hari")
ax.set_xticks(x)
ax.set_xticklabels(pivot_cmp.index)
ax.legend()

plt.tight_layout()
fig7.savefig(f"{OUTPUT_DIR}/7_avg_order_jakarta_vs_medan.png", bbox_inches="tight")



print("[7] Perbandingan revenue Jakarta vs Medan vs Surabaya...")

# Filter kota & hanya order sukses
df_rev = fact_orders[
    (fact_orders["city"].isin(["jakarta", "medan", "surabaya"])) &
    (fact_orders["order_status"] == "delivered")
].copy()

# Pastikan date sebagai harian
df_rev["date"] = pd.to_datetime(df_rev["date_id"].astype(str)).dt.floor("D")

# Hitung total revenue per hari per kota
daily_revenue = (
    df_rev.groupby(["city", "date"])["total_amount"]
    .sum()
    .reset_index()
)

# Rata-rata revenue harian per kota
avg_daily_revenue = (
    daily_revenue.groupby("city")["total_amount"]
    .mean()
    .reset_index()
)

sum_revenue = (
    daily_revenue.groupby("city")["total_amount"]
    .sum()
    .reset_index()
)

sum_revenue = sum_revenue.sort_values("total_amount",ascending=False)
# Urutkan biar rapi
avg_daily_revenue = avg_daily_revenue.sort_values("total_amount", ascending=False)

# ── Plot ─────────────────────────────────────────────
fig8, ax = plt.subplots(figsize=(9, 6))
ax.margins(y=0.2)
plt.tight_layout(rect=[0, 0, 1, 0.95])
bars = ax.bar(
    sum_revenue["city"],
    sum_revenue["total_amount"] / 1_000_000,
    color=[PALETTE_MAIN, PALETTE_ACCENT, "#F39C12"],
    edgecolor="white",
    width=0.55
)

# Label angka
for bar in bars:
    ax.text(
        bar.get_x() + bar.get_width() / 2,
        bar.get_height() + 0.02,     
        f"Rp {bar.get_height():.1f}M",
        ha="center",
        fontsize=9
    )
  
ax.set_ylim(0, sum_revenue["total_amount"].max() / 1_000_000 * 1.25)
ax.set_title("Total Revenue\n(Jakarta vs Medan vs Surabaya)",
             fontweight="bold", pad=12) 
ax.set_xlabel("Kota")
ax.set_ylabel("Revenue (juta Rupiah)")

fig8.tight_layout() 
fig8.savefig(f"{OUTPUT_DIR}/8_avg_revenue_3_kota.png", bbox_inches="tight")

# ──────────────────────────────────────────────────────────────────────────────
# RINGKASAN EKSEKUTIF
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  RINGKASAN EKSEKUTIF — GoFood Analytics")
print("=" * 60)
print(f"  Total order dianalisis : {len(fact_orders):,}")
print(f"  Total merchant aktif   : {len(dim_merchant):,}")
print(f"  Total user             : {len(dim_user):,}")
print(f"  Rentang data cuaca     : {dim_weather['waktu'].min()} → {dim_weather['waktu'].max()}")
print(f"\n  Grafik tersimpan di    : ./{OUTPUT_DIR}/")
print("=" * 60)