-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "233f4c6f-57c9-4c54-bea6-60a9126b4070",
-- META       "default_lakehouse_name": "lh_vule_sonle_medallion",
-- META       "default_lakehouse_workspace_id": "174d659d-2d6e-4d9f-86df-19eba8ef09a7",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "233f4c6f-57c9-4c54-bea6-60a9126b4070"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC 
-- MAGIC CREATE SCHEMA IF NOT EXISTS etl;
-- MAGIC 
-- MAGIC CREATE TABLE etl.watermark (
-- MAGIC     timestamp TIMESTAMP NOT NULL,
-- MAGIC     layer STRING NOT NULL,
-- MAGIC     object_name STRING NOT NULL,
-- MAGIC     key_1 STRING NOT NULL,
-- MAGIC     key_1_desc STRING
-- MAGIC )
-- MAGIC USING DELTA;

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- Code xoá dòng wtm để run lại bảng

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql import functions as F
-- MAGIC 
-- MAGIC METADATA_DB = "lh_vule_sonle_medallion"
-- MAGIC WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"
-- MAGIC 
-- MAGIC # Đọc toàn bộ watermark
-- MAGIC df_watermark = spark.table(WATERMARK_TABLE)
-- MAGIC 
-- MAGIC # Giữ lại tất cả các dòng KHÔNG thuộc layer = 'bronze'
-- MAGIC df_keep = df_watermark.filter(F.col("layer") != "bronze")
-- MAGIC 
-- MAGIC # Số dòng đã bị xóa
-- MAGIC deleted_count = df_watermark.count() - df_keep.count()
-- MAGIC 
-- MAGIC if deleted_count > 0:
-- MAGIC     # Ghi đè bảng watermark bằng dữ liệu đã lọc
-- MAGIC     df_keep.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(WATERMARK_TABLE)
-- MAGIC     print(f"Đã xóa {deleted_count} dòng watermark của layer='bronze'")
-- MAGIC else:
-- MAGIC     print("Không có dòng watermark nào của layer='bronze' để xóa.")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
