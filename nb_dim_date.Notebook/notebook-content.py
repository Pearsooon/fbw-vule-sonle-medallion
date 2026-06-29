# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "233f4c6f-57c9-4c54-bea6-60a9126b4070",
# META       "default_lakehouse_name": "lh_vule_sonle_medallion",
# META       "default_lakehouse_workspace_id": "174d659d-2d6e-4d9f-86df-19eba8ef09a7",
# META       "known_lakehouses": [
# META         {
# META           "id": "233f4c6f-57c9-4c54-bea6-60a9126b4070"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ============================================================
# GOLD NOTEBOOK — DIM_DATE
# Source: lh_vule_sonle_medallion.silver.Sales_Orders
#         (để xác định khoảng ngày thực tế cần tạo)
# Loại  : One-time hoặc refresh định kỳ (mode=overwrite)
# ============================================================

from pyspark.sql import functions as F
from datetime import date, timedelta

METADATA_DB  = "lh_vule_sonle_medallion"
GOLD_TABLE   = f"{METADATA_DB}.Gold.dim_date"

start_date = date(1999, 1, 1)
end_date   = date(2030, 12, 31)

print(f"[INFO] Khoảng ngày sẽ tạo: {start_date} → {end_date} ({(end_date - start_date).days + 1:,} ngày)")

# ── 2. Sinh danh sách ngày (Python driver, sau đó parallelise) ────────────────
rows = []
cur  = start_date
while cur <= end_date:
    rows.append((cur,))
    cur += timedelta(days=1)

df_raw = spark.createDataFrame(rows, ["full_date"])

# ── 3. Tạo tất cả các cột phân tích ──────────────────────────────────────────
df_dim_date = df_raw.select(
    # Khoá chính dạng số YYYYMMDD — dễ join với Fact
    F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
    F.col("full_date"),
    F.year("full_date").alias("year"),
    F.quarter("full_date").alias("quarter"),
    F.when(F.quarter("full_date") == 1, "Q1")
     .when(F.quarter("full_date") == 2, "Q2")
     .when(F.quarter("full_date") == 3, "Q3")
     .otherwise("Q4").alias("quarter_name"),
    F.month("full_date").alias("month"),
    F.date_format("full_date", "MMMM").alias("month_name"),
    F.date_format("full_date", "MMM").alias("month_short"),
    F.dayofmonth("full_date").alias("day"),
    F.dayofweek("full_date").alias("day_of_week"),       # 1=Chủ nhật … 7=Thứ 7
    F.date_format("full_date", "EEEE").alias("day_name"),
    F.date_format("full_date", "EEE").alias("day_short"),
    F.weekofyear("full_date").alias("week_of_year"),
    # Cờ phân tích
    F.when(F.dayofweek("full_date").isin([1, 7]), True)
     .otherwise(False).alias("is_weekend"),
    (F.dayofmonth("full_date") == 1).alias("is_month_start"),
    (F.last_day("full_date") == F.col("full_date")).alias("is_month_end"),
    # Năm-Tháng dạng số để nhóm trong BI
    F.date_format("full_date", "yyyyMM").cast("int").alias("year_month_key"),
    # Năm-Quý dạng chuỗi để hiển thị
    F.concat(F.year("full_date").cast("string"), F.lit("-"),
             F.when(F.quarter("full_date") == 1, "Q1")
              .when(F.quarter("full_date") == 2, "Q2")
              .when(F.quarter("full_date") == 3, "Q3")
              .otherwise("Q4")).alias("year_quarter"),
)

# ── 4. Ghi vào Gold (overwrite toàn bộ — date dim không có SCD) ──────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.Gold")

df_dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(GOLD_TABLE)

total = spark.table(GOLD_TABLE).count()
print(f"[SUCCESS] {GOLD_TABLE} sẵn sàng — {total:,} rows")
print(f"          date_key nhỏ nhất: {start_date.strftime('%Y%m%d')}")
print(f"          date_key lớn nhất: {end_date.strftime('%Y%m%d')}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
