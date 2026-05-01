"""
GoFood OLAP Charts
Generate 5 visualisasi insight dari DWH (fact_orders + fact_order_items)
Output: PNG files di folder charts/
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine


DWH_URL = "mysql+pymysql://root:@192.168.144.1:3306/dwh_uts"
OUT_DIR   = Path("charts")
OUT_DIR.mkdir(exist_ok=True)

conn = create_engine(DWH_URL)

# ── Style global ──────────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":   "DejaVu Sans",
    "font.size":     11,
    "axes.spines.top":    False,
    "axes.spines.right":  False,
    "axes.spines.left":   False,
    "axes.spines.bottom": False,
    "axes.grid":          True,
    "grid.color":         "#e8e8e8",
    "grid.linewidth":     0.6,
    "figure.facecolor":   "white",
    "axes.facecolor":     "white",
    "xtick.color":        "#555",
    "ytick.color":        "#555",
})

PURPLE  = "#7F77DD"
TEAL    = "#1D9E75"
CORAL   = "#D85A30"
AMBER   = "#BA7517"
BLUE    = "#378ADD"
GRAY    = "#888780"

def save(fig, name):
    path = OUT_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  ✓ {name}")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 1 — Merchant dengan revenue terbanyak (Top 15, horizontal bar)
# ══════════════════════════════════════════════════════════════════════════════
sql1 = """
SELECT
    dm.merchant_name,
    dm.merchant_category,
    SUM(fo.total_amount)  AS total_revenue,
    COUNT(fo.order_id)    AS total_orders
FROM   fact_orders fo
JOIN   dim_merchant dm ON fo.merchant_id = dm.merchant_id
WHERE  fo.order_status = 'delivered'
GROUP  BY dm.merchant_id
ORDER  BY total_revenue DESC
LIMIT  15
"""
df1 = pd.read_sql_query(sql1, conn)
df1["label"] = df1["merchant_name"].str.slice(0, 32)

fig, ax = plt.subplots(figsize=(11, 7))

colors = [PURPLE if i == 0 else "#AFA9EC" for i in range(len(df1))]
bars = ax.barh(df1["label"][::-1], df1["total_revenue"][::-1],
               color=colors[::-1], height=0.62, zorder=2)

for bar, rev, orders in zip(bars, df1["total_revenue"][::-1], df1["total_orders"][::-1]):
    ax.text(bar.get_width() + 15000, bar.get_y() + bar.get_height() / 2,
            f"Rp {rev:,.0f}  ({orders} orders)",
            va="center", fontsize=9, color="#555")

ax.set_xlabel("Total Revenue (Rp)", labelpad=8, color="#555", fontsize=10)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"Rp {x/1e6:.1f}M"))
ax.set_xlim(0, df1["total_revenue"].max() * 1.45)
ax.tick_params(left=False)
ax.set_title("Merchant dengan revenue terbanyak\n(top 15, delivered orders)",
             fontsize=14, fontweight="bold", pad=14, loc="left", color="#222")

fig.text(0.01, -0.01,
         "SQL: SELECT dm.merchant_name, SUM(fo.total_amount) AS total_revenue "
         "FROM fact_orders fo JOIN dim_merchant dm ON fo.merchant_id = dm.merchant_id "
         "WHERE fo.order_status = 'delivered' GROUP BY dm.merchant_id ORDER BY total_revenue DESC LIMIT 15",
         fontsize=7.5, color="#aaa", fontstyle="italic", wrap=True)

save(fig, "1_top_merchant_revenue.png")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 2 — Perbandingan transaksi Weekday vs Weekend
# ══════════════════════════════════════════════════════════════════════════════
sql2a = """
SELECT
    dd.is_weekend,
    CASE WHEN dd.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(fo.order_id)        AS total_orders,
    SUM(fo.total_amount)      AS total_revenue,
    AVG(fo.total_amount)      AS avg_order_value,
    SUM(fo.is_cancelled)      AS cancelled_orders
FROM   fact_orders fo
JOIN   dim_date dd ON fo.date_id = dd.date_id
WHERE  fo.order_status IN ('delivered', 'cancelled_by_customer', 'cancelled_by_driver')
GROUP  BY dd.is_weekend
"""
sql2b = """
SELECT
    dd.day_name,
    dd.day_of_week,
    COUNT(fo.order_id) AS total_orders
FROM   fact_orders fo
JOIN   dim_date dd ON fo.date_id = dd.date_id
WHERE  fo.order_status = 'delivered'
GROUP  BY dd.day_name, dd.day_of_week
ORDER  BY dd.day_of_week
"""
df2a = pd.read_sql_query(sql2a, conn)
df2b = pd.read_sql_query(sql2b, conn)

day_order = ["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
df2b["day_name"] = pd.Categorical(df2b["day_name"], categories=day_order, ordered=True)
df2b = df2b.sort_values("day_name")
df2b["is_weekend"] = df2b["day_name"].isin(["Saturday","Sunday"])

fig, axes = plt.subplots(1, 2, figsize=(13, 5))
fig.suptitle("Perbandingan transaksi: Weekday vs Weekend",
             fontsize=14, fontweight="bold", x=0.01, ha="left", y=1.01, color="#222")

# Sub-chart kiri: grouped bar weekday vs weekend
ax = axes[0]
cats  = df2a["day_type"].tolist()
vals  = df2a["total_orders"].tolist()
cols  = [TEAL if c == "Weekday" else CORAL for c in cats]
b     = ax.bar(cats, vals, color=cols, width=0.45, zorder=2)
for bar, v in zip(b, vals):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 30,
            f"{v:,}", ha="center", fontsize=11, fontweight="bold", color="#333")

ax.set_ylabel("Jumlah transaksi", fontsize=10, color="#555")
ax.set_ylim(0, max(vals) * 1.25)
ax.tick_params(bottom=False)
ax.set_title("Total transaksi", fontsize=11, color="#555", pad=8)
patches = [mpatches.Patch(color=TEAL, label="Weekday"),
           mpatches.Patch(color=CORAL, label="Weekend")]
ax.legend(handles=patches, fontsize=9, frameon=False)

# Sub-chart kanan: per-hari dalam seminggu
ax2 = axes[1]
day_cols = [CORAL if w else TEAL for w in df2b["is_weekend"]]
b2 = ax2.bar(df2b["day_name"].astype(str).str[:3], df2b["total_orders"],
             color=day_cols, width=0.55, zorder=2)
for bar, v in zip(b2, df2b["total_orders"]):
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 8,
             str(v), ha="center", fontsize=9, color="#333")
ax2.set_ylabel("Jumlah transaksi", fontsize=10, color="#555")
ax2.set_ylim(0, df2b["total_orders"].max() * 1.2)
ax2.set_title("Distribusi per hari", fontsize=11, color="#555", pad=8)
ax2.tick_params(bottom=False)

fig.text(0.01, -0.04,
         "SQL: SELECT dd.is_weekend, COUNT(fo.order_id) AS total_orders FROM fact_orders fo "
         "JOIN dim_date dd ON fo.date_id = dd.date_id GROUP BY dd.is_weekend",
         fontsize=7.5, color="#aaa", fontstyle="italic")

plt.tight_layout()
save(fig, "2_weekday_vs_weekend.png")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 3 — Proporsi kategori makanan berdasarkan kelompok usia (heatmap)
# ══════════════════════════════════════════════════════════════════════════════
sql3 = """
SELECT
    du.age_group,
    foi.product_category,
    COUNT(*) AS total_orders
FROM   fact_order_items foi
JOIN   dim_user du ON foi.user_id = du.user_id
WHERE  foi.order_status = 'delivered'
  AND  du.age_group IS NOT NULL
GROUP  BY du.age_group, foi.product_category
ORDER  BY du.age_group, total_orders DESC
"""
df3 = pd.read_sql_query(sql3, conn)

pivot = df3.pivot_table(index="product_category", columns="age_group",
                        values="total_orders", aggfunc="sum", fill_value=0)
age_order = ["17-24", "25-34", "35-44", "45-54", "55+"]
pivot = pivot.reindex(columns=[c for c in age_order if c in pivot.columns])

# Normalisasi per kolom (proporsi dalam kelompok usia)
pivot_pct = pivot.div(pivot.sum(axis=0), axis=1) * 100

fig, ax = plt.subplots(figsize=(10, 8))
im = ax.imshow(pivot_pct.values, cmap="BuPu", aspect="auto", vmin=0, vmax=pivot_pct.values.max())

ax.set_xticks(range(len(pivot_pct.columns)))
ax.set_xticklabels(pivot_pct.columns, fontsize=11)
ax.set_yticks(range(len(pivot_pct.index)))
ax.set_yticklabels(pivot_pct.index, fontsize=10)

for i in range(len(pivot_pct.index)):
    for j in range(len(pivot_pct.columns)):
        val = pivot_pct.values[i, j]
        raw = pivot.values[i, j]
        text_col = "white" if val > pivot_pct.values.max() * 0.55 else "#333"
        ax.text(j, i, f"{val:.1f}%\n({raw:,})",
                ha="center", va="center", fontsize=8.5, color=text_col)

cbar = plt.colorbar(im, ax=ax, shrink=0.6, pad=0.02)
cbar.set_label("Proporsi dalam kelompok usia (%)", fontsize=9, color="#555")
cbar.ax.tick_params(labelsize=8)

ax.set_title("Proporsi kategori makanan berdasarkan kelompok usia\n(% dalam tiap kelompok usia, delivered orders)",
             fontsize=13, fontweight="bold", pad=14, loc="left", color="#222")
ax.set_xlabel("Kelompok usia", labelpad=8, fontsize=10, color="#555")
ax.set_ylabel("Kategori makanan", labelpad=8, fontsize=10, color="#555")
ax.grid(False)

fig.text(0.01, -0.02,
         "SQL: SELECT du.age_group, foi.product_category, COUNT(*) AS total_orders "
         "FROM fact_order_items foi JOIN dim_user du ON foi.user_id = du.user_id "
         "WHERE foi.order_status = 'delivered' GROUP BY du.age_group, foi.product_category",
         fontsize=7.5, color="#aaa", fontstyle="italic")

save(fig, "3_kategori_per_usia.png")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 4 — Persebaran lokasi merchant (scatter map DKI Jakarta)
# ══════════════════════════════════════════════════════════════════════════════
sql4 = """
SELECT
    dm.merchant_name,
    dm.merchant_category,
    dm.merchant_area,
    dm.merchant_lat  AS lat,
    dm.merchant_lon  AS lon,
    dm.merchant_rating,
    COUNT(fo.order_id)   AS total_orders,
    SUM(fo.total_amount) AS total_revenue
FROM   dim_merchant dm
LEFT JOIN fact_orders fo
       ON dm.merchant_id = fo.merchant_id
      AND fo.order_status = 'delivered'
WHERE  dm.merchant_lat IS NOT NULL
GROUP  BY dm.merchant_id
"""
df4 = pd.read_sql_query(sql4, conn)
df4["total_orders"]  = df4["total_orders"].fillna(0)
df4["total_revenue"] = df4["total_revenue"].fillna(0)

area_colors = {
    "Jakarta Pusat":   "#534AB7",
    "Jakarta Utara":   "#1D9E75",
    "Jakarta Barat":   "#D85A30",
    "Jakarta Selatan": "#BA7517",
    "Jakarta Timur":   "#378ADD",
}
df4["color"] = df4["merchant_area"].map(area_colors).fillna(GRAY)

# Bubble size ~ total_orders
size_min, size_max = 20, 300
orders_min, orders_max = df4["total_orders"].min(), df4["total_orders"].max()
if orders_max > orders_min:
    df4["bubble"] = size_min + (df4["total_orders"] - orders_min) / (orders_max - orders_min) * (size_max - size_min)
else:
    df4["bubble"] = 60

fig, ax = plt.subplots(figsize=(11, 9))

for area, grp in df4.groupby("merchant_area"):
    ax.scatter(grp["lon"], grp["lat"],
               s=grp["bubble"], c=area_colors.get(area, GRAY),
               alpha=0.65, linewidths=0.3, edgecolors="white",
               zorder=3, label=area)

# Legend area
legend_handles = [
    mpatches.Patch(color=c, label=a) for a, c in area_colors.items()
]
ax.legend(handles=legend_handles, title="Wilayah", fontsize=9,
          title_fontsize=9, frameon=True, framealpha=0.9,
          loc="lower right")

# Bubble size legend
for sz, lbl in [(20, "Sedikit"), (100, "Sedang"), (300, "Banyak")]:
    ax.scatter([], [], s=sz, c=GRAY, alpha=0.6, label=lbl)
size_leg = ax.legend(title="Ukuran = total orders", fontsize=8,
                     title_fontsize=8, frameon=True, framealpha=0.9,
                     loc="lower left",
                     handles=[
                         plt.scatter([], [], s=20,  c=GRAY, alpha=0.6),
                         plt.scatter([], [], s=100, c=GRAY, alpha=0.6),
                         plt.scatter([], [], s=300, c=GRAY, alpha=0.6),
                     ],
                     labels=["Sedikit", "Sedang", "Banyak"])
ax.add_artist(legend_handles[0] if False else size_leg)
ax.legend(handles=legend_handles, title="Wilayah", fontsize=9,
          title_fontsize=9, frameon=True, framealpha=0.9, loc="lower right")

ax.set_xlabel("Longitude", fontsize=10, color="#555")
ax.set_ylabel("Latitude",  fontsize=10, color="#555")
ax.set_title("Persebaran lokasi merchant DKI Jakarta\n(ukuran bubble = jumlah delivered orders)",
             fontsize=13, fontweight="bold", pad=14, loc="left", color="#222")

# Batas approx DKI Jakarta
ax.set_xlim(106.72, 107.00)
ax.set_ylim(-6.36,  -6.08)
ax.tick_params(labelsize=9)

# Anotasi wilayah
for area, grp in df4.groupby("merchant_area"):
    cx, cy = grp["lon"].mean(), grp["lat"].mean()
    ax.text(cx, cy, area.replace("Jakarta ", "Jkt\n"), fontsize=7.5,
            ha="center", va="center", color="#333",
            bbox=dict(boxstyle="round,pad=0.2", fc="white", alpha=0.5, lw=0))

fig.text(0.01, -0.02,
         "SQL: SELECT dm.merchant_lat, dm.merchant_lon, dm.merchant_area, "
         "COUNT(fo.order_id) AS total_orders FROM dim_merchant dm "
         "LEFT JOIN fact_orders fo ON dm.merchant_id = fo.merchant_id "
         "WHERE fo.order_status = 'delivered' GROUP BY dm.merchant_id",
         fontsize=7.5, color="#aaa", fontstyle="italic")

save(fig, "4_lokasi_merchant.png")


# ══════════════════════════════════════════════════════════════════════════════
# Chart 5 — Distribusi order berdasarkan jam (area + bar, dual view)
# ══════════════════════════════════════════════════════════════════════════════
sql5a = """
SELECT
    fo.order_hour,
    COUNT(fo.order_id)   AS total_orders,
    SUM(fo.total_amount) AS total_revenue,
    AVG(fo.total_amount) AS avg_order_value
FROM   fact_orders fo
WHERE  fo.order_status = 'delivered'
GROUP  BY fo.order_hour
ORDER  BY fo.order_hour
"""
sql5b = """
SELECT
    fo.order_hour,
    dd.is_weekend,
    CASE WHEN dd.is_weekend=1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(fo.order_id) AS total_orders
FROM   fact_orders fo
JOIN   dim_date dd ON fo.date_id = dd.date_id
WHERE  fo.order_status = 'delivered'
GROUP  BY fo.order_hour, dd.is_weekend
ORDER  BY fo.order_hour
"""
df5a = pd.read_sql_query(sql5a, conn)
df5b = pd.read_sql_query(sql5b, conn)

wday = df5b[df5b["is_weekend"]==0].set_index("order_hour")["total_orders"]
wend = df5b[df5b["is_weekend"]==1].set_index("order_hour")["total_orders"]
hours = list(range(24))
wday = wday.reindex(hours, fill_value=0)
wend = wend.reindex(hours, fill_value=0)

fig, axes = plt.subplots(2, 1, figsize=(13, 9), gridspec_kw={"hspace": 0.45})
fig.suptitle("Distribusi order berdasarkan jam",
             fontsize=14, fontweight="bold", x=0.01, ha="left", y=1.01, color="#222")

# Sub-chart atas: area chart total orders + avg order value
ax = axes[0]
ax.fill_between(df5a["order_hour"], df5a["total_orders"],
                color=PURPLE, alpha=0.18, zorder=1)
ax.plot(df5a["order_hour"], df5a["total_orders"],
        color=PURPLE, linewidth=2.2, zorder=2, marker="o", markersize=4)

ax2_twin = ax.twinx()
ax2_twin.plot(df5a["order_hour"], df5a["avg_order_value"],
              color=AMBER, linewidth=1.5, linestyle="--", zorder=2, marker="s", markersize=3)
ax2_twin.set_ylabel("Avg order value (Rp)", fontsize=9, color=AMBER)
ax2_twin.tick_params(colors=AMBER, labelsize=8)
ax2_twin.spines["right"].set_visible(True)
ax2_twin.spines["right"].set_color("#e8e8e8")

ax.set_ylabel("Jumlah orders", fontsize=10, color=PURPLE)
ax.set_xlabel("Jam (0-23)", fontsize=10, color="#555")
ax.set_xticks(range(24))
ax.tick_params(labelsize=8)
ax.set_title("Total delivered orders & avg order value per jam", fontsize=11, color="#555", pad=8)

# Anotasi peak hours
peak_idx = df5a["total_orders"].idxmax()
peak_hour = df5a.loc[peak_idx, "order_hour"]
peak_val  = df5a.loc[peak_idx, "total_orders"]
ax.annotate(f"Peak: jam {peak_hour:02d}:00\n{peak_val} orders",
            xy=(peak_hour, peak_val),
            xytext=(peak_hour + 2, peak_val + 8),
            fontsize=8.5, color=PURPLE,
            arrowprops=dict(arrowstyle="->", color=PURPLE, lw=0.8))

patches2 = [mpatches.Patch(color=PURPLE, alpha=0.7, label="Total orders"),
            mpatches.Patch(color=AMBER,  alpha=0.7, label="Avg order value")]
ax.legend(handles=patches2, fontsize=8.5, frameon=False, loc="upper left")

# Sub-chart bawah: stacked bar weekday vs weekend per jam
ax3 = axes[1]
x = np.arange(24)
b1 = ax3.bar(x, wday.values, color=TEAL,  width=0.7,   label="Weekday", zorder=2)
b2 = ax3.bar(x, wend.values, color=CORAL, width=0.7,   label="Weekend",
             bottom=wday.values, zorder=2)

ax3.set_xticks(x)
ax3.set_xticklabels([f"{h:02d}" for h in hours], fontsize=7.5)
ax3.set_xlabel("Jam (0-23)", fontsize=10, color="#555")
ax3.set_ylabel("Jumlah orders", fontsize=10, color="#555")
ax3.set_title("Breakdown Weekday vs Weekend per jam", fontsize=11, color="#555", pad=8)
ax3.legend(fontsize=9, frameon=False, loc="upper left")

# Shading jam makan siang & malam
for start, end, label in [(11, 13, "Makan\nsiang"), (17, 20, "Makan\nmalam")]:
    ax3.axvspan(start - 0.5, end + 0.5, color="#faf0e6", alpha=0.6, zorder=0)
    ax3.text((start + end) / 2, ax3.get_ylim()[1] * 0.88,
             label, ha="center", fontsize=7.5, color=AMBER)

fig.text(0.01, -0.03,
         "SQL: SELECT fo.order_hour, dd.is_weekend, COUNT(fo.order_id) AS total_orders "
         "FROM fact_orders fo JOIN dim_date dd ON fo.date_id = dd.date_id "
         "WHERE fo.order_status = 'delivered' GROUP BY fo.order_hour, dd.is_weekend",
         fontsize=7.5, color="#aaa", fontstyle="italic")

save(fig, "5_distribusi_per_jam.png")

conn.close()
print(f"\n✅ 5 chart tersimpan di folder: {OUT_DIR}/")