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
# DEDICATED GOLD NOTEBOOK — DIM_PEOPLE (SCD1)
# Source: Silver.Application_People
# Target: Gold.Dim_People
#
# Logic:
#   - Read full Silver table
#   - Filter out soft-deleted records at read time:
#       deleted_audit_ts IS NULL
#   - Take latest active record per business key
#   - SCD1:
#       + New key    -> insert
#       + Changed row_hash / inferred row -> update in place
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================
# 1. Hardcode
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT = "Application_People"
TARGET_OBJECT = "Dim_People"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_tables"

TARGET_SCHEMA = "Gold"

SKEY_COL = "person_skey"

execution_start_time = datetime.now()

print("=" * 80)
print("[START] Gold SCD1 load started")
print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
print(f"[SKEY_COL] {SKEY_COL}")
print(f"[START_TIME] {execution_start_time}")
print("=" * 80)


# ============================================================
# 2. Helper functions
# ============================================================

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def get_row_value(row, col_name, default=None):
    row_dict = row.asDict()
    return row_dict.get(col_name, default)


def split_config_list(value):
    if value is None:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def create_hash_expr(cols):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                for c in cols
            ]
        ),
        256
    )


def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0

    result = (
        spark.table(target_table)
        .agg(F.max(F.col(skey_col)).alias("max_skey"))
        .collect()[0]["max_skey"]
    )

    return int(result or 0)


def add_surrogate_key(df, target_table: str, skey_col: str, business_keys: list):
    max_skey = get_max_skey(target_table, skey_col)

    order_cols = []

    for c in business_keys:
        if c in df.columns:
            order_cols.append(F.col(c).asc())

    order_cols += [
        F.col("audit_ts").asc(),
        F.col("hash_key").asc()
    ]

    w_skey = Window.orderBy(*order_cols)

    return df.withColumn(
        skey_col,
        F.lit(max_skey) + F.row_number().over(w_skey)
    )


def align_to_target_columns(df, target_table: str):
    if not table_exists(target_table):
        return df

    target_cols = spark.table(target_table).columns

    for c in target_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None))

    return df.select(*target_cols)


def make_empty_df_like(df):
    return spark.createDataFrame([], df.schema)


# ============================================================
# 3. Load Gold config
# ============================================================

try:
    config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("layer") == "Gold") &
            (F.col("source_object") == SOURCE_OBJECT) &
            (F.col("is_active") == 1)
        )
        .limit(1)
        .collect()
    )

    if not config_row:
        raise RuntimeError(
            f"[ERROR] Không tìm thấy config cho source_object = '{SOURCE_OBJECT}'"
        )

    metadata = config_row[0]

    source_schema = "Silver"
    source_object = get_row_value(metadata, "source_object", SOURCE_OBJECT)

    business_key = get_row_value(metadata, "business_key")
    column_list = get_row_value(metadata, "column_list")

    business_keys = split_config_list(business_key)
    configured_columns = split_config_list(column_list)

    if not business_keys:
        raise RuntimeError("[ERROR] business_key trong config_tables đang trống")

    if not configured_columns:
        raise RuntimeError("[ERROR] column_list trong config_tables đang trống")

    silver_table = f"{METADATA_DB}.{source_schema}.{source_object}"
    gold_table = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"

    print(f"[CONFIG] Source table : {silver_table}")
    print(f"[CONFIG] Target table : {gold_table}")
    print(f"[CONFIG] Business key : {business_keys}")
    print(f"[CONFIG] SKey column  : {SKEY_COL}")

except Exception as e:
    print(f"[FAILED] Load Gold config failed: {str(e)}")
    raise


# ============================================================
# 4. Read latest active records from Silver
#    Không dùng Gold watermark
#    Soft delete: filter deleted_audit_ts ra ngay lúc đọc
# ============================================================

try:
    if not table_exists(silver_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_table}")

    df_silver = spark.table(silver_table)

    if "audit_ts" not in df_silver.columns:
        raise RuntimeError("[ERROR] Silver table không có audit_ts")

    missing_bk = [c for c in business_keys if c not in df_silver.columns]
    if missing_bk:
        raise RuntimeError(f"[ERROR] Silver table thiếu business key columns: {missing_bk}")

    # Soft delete rule:
    # Chỉ lấy active records từ Silver. Tombstone/deleted records không đi tiếp xuống Gold.
    if "deleted_audit_ts" in df_silver.columns:
        df_silver = df_silver.filter(F.col("deleted_audit_ts").isNull())

    df_silver_with_hash = df_silver.withColumn(
        "hash_key",
        create_hash_expr(business_keys)
    )

    w_latest = Window.partitionBy("hash_key").orderBy(
        F.col("audit_ts").desc()
    )

    df_silver_latest = (
        df_silver_with_hash
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .cache()
    )

    latest_silver_count = df_silver_latest.count()

    if latest_silver_count == 0:
        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Gold SCD1 load completed - No active data in Silver")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

        notebookutils.notebook.exit("NO_ACTIVE_DATA_IN_SILVER")

    print(f"[INFO] Latest active records from Silver: {latest_silver_count}")

except Exception as e:
    print(f"[FAILED] Read latest active Silver records failed: {str(e)}")
    raise


# ============================================================
# 5. Prepare staged data
# ============================================================

try:
    selected_cols = list(dict.fromkeys(
        business_keys + configured_columns + ["audit_ts"]
    ))

    if "source_id" in df_silver_latest.columns and "source_id" not in selected_cols:
        selected_cols.append("source_id")

    if "hash_key" not in selected_cols:
        selected_cols.append("hash_key")

    missing_selected_cols = [
        c for c in selected_cols
        if c not in df_silver_latest.columns
    ]

    if missing_selected_cols:
        raise RuntimeError(f"[ERROR] Missing selected columns from Silver latest: {missing_selected_cols}")

    df_staged_raw = df_silver_latest.select(*selected_cols)

    exclude_for_row_hash = set(business_keys) | {
        "audit_ts",
        "source_id",
        "hash_key",
        "row_hash",
    }

    hash_columns = [
        c for c in configured_columns
        if c in df_staged_raw.columns and c not in exclude_for_row_hash
    ]

    if not hash_columns:
        raise RuntimeError("[ERROR] Không có business attribute nào để tạo row_hash")

    df_staged = (
        df_staged_raw
        .withColumn("row_hash", create_hash_expr(hash_columns))
        .cache()
    )

    staged_count = df_staged.count()

    print(f"[STAGED] Prepared staged latest active records: {staged_count}")
    print(f"[STAGED] Row hash columns: {hash_columns}")

except Exception as e:
    print(f"[FAILED] Prepare staged data failed: {str(e)}")
    raise


# ============================================================
# 6. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")
    print("[INFO] Gold schema checked")

except Exception as e:
    print(f"[FAILED] Create Gold schema failed: {str(e)}")
    raise


# ============================================================
# 7. Check new / changed records
#    Không xử lý deleted
#    SCD1: inferred records are handled as changed records
# ============================================================

try:
    if table_exists(gold_table):
        df_gold = spark.table(gold_table)

        required_cols = {
            "hash_key",
            SKEY_COL,
            "row_hash",
            "inferred_flag"
        }

        missing_cols = [c for c in required_cols if c not in df_gold.columns]

        if missing_cols:
            raise RuntimeError(
                f"[ERROR] Gold dimension thiếu required columns: {missing_cols}"
            )

        df_gold_current = (
            df_gold
            .select(
                "hash_key",
                SKEY_COL,
                F.col("row_hash").alias("_gold_row_hash"),
                F.col("inferred_flag").alias("_gold_inferred_flag")
            )
            .cache()
        )

        df_new = (
            df_staged
            .join(
                df_gold_current.select("hash_key"),
                on="hash_key",
                how="left_anti"
            )
            .cache()
        )

        df_changed = (
            df_staged
            .join(df_gold_current, on="hash_key", how="inner")
            .filter(
                (F.col("row_hash") != F.col("_gold_row_hash")) |
                (F.col("_gold_inferred_flag") == 1)
            )
            .drop(
                "_gold_row_hash",
                "_gold_inferred_flag"
            )
            .cache()
        )

    else:
        df_new = df_staged.cache()
        df_changed = make_empty_df_like(df_staged)

    new_rows = df_new.count()
    changed_rows = df_changed.count()

    print(f"[CHECK] New records      : {new_rows}")
    print(f"[CHECK] Changed records  : {changed_rows}")

except Exception as e:
    print(f"[FAILED] Check new / changed failed: {str(e)}")
    raise


# ============================================================
# 8. Build SCD1 insert dataframe
# ============================================================

try:
    df_new_insert = (
        df_new
        .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
        .withColumn("scd_active", F.lit(1).cast(IntegerType()))
        .withColumn("scd_start", F.current_timestamp())
        .withColumn("inferred_flag", F.lit(0).cast(IntegerType()))
    )

    print(f"[INSERT NEW] Prepared rows: {new_rows}")

except Exception as e:
    print(f"[FAILED] Build SCD1 insert dataframe failed: {str(e)}")
    raise


# ============================================================
# 9. Insert new records
# ============================================================

try:
    if new_rows > 0:
        df_new_insert = add_surrogate_key(
            df_new_insert,
            gold_table,
            SKEY_COL,
            business_keys
        )

        ordered_cols = [SKEY_COL] + [
            c for c in df_new_insert.columns
            if c != SKEY_COL
        ]

        df_new_insert = df_new_insert.select(*ordered_cols)

        if table_exists(gold_table):
            df_new_insert = align_to_target_columns(df_new_insert, gold_table)

            df_new_insert.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(gold_table)

        else:
            df_new_insert.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(gold_table)

        print(f"[INSERT SCD1] Inserted new rows: {new_rows}")

    else:
        print("[INSERT SCD1] No new records to insert")

except Exception as e:
    print(f"[FAILED] Insert SCD1 new records failed: {str(e)}")
    raise


# ============================================================
# 10. Update changed records directly
#     SCD1 overwrite:
#       - changed attribute
#       - inferred_flag = 1 record
# ============================================================

try:
    if changed_rows > 0 and table_exists(gold_table):
        df_changed_update = (
            df_changed
            .dropDuplicates(["hash_key"])
            .cache()
        )

        df_gold = spark.table(gold_table)

        update_set = {
            "audit_ts": "s.audit_ts",
            "updated_audit_ts": "current_timestamp()",
            "row_hash": "s.row_hash",
            "inferred_flag": "0",
            "scd_active": "1",
            "scd_version": "1"
        }

        # Giữ column này nếu Gold table cũ đã có, nhưng không dùng nó để xử lý delete.
        if "deleted_audit_ts" in df_gold.columns:
            update_set["deleted_audit_ts"] = "CAST(NULL AS TIMESTAMP)"

        if "source_id" in df_changed_update.columns and "source_id" in df_gold.columns:
            update_set["source_id"] = "s.source_id"

        for c in configured_columns:
            if c in df_changed_update.columns and c in df_gold.columns:
                update_set[c] = f"s.`{c}`"

        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(
                df_changed_update.alias("s"),
                "t.hash_key = s.hash_key"
            ) \
            .whenMatchedUpdate(set=update_set) \
            .execute()

        print(f"[UPDATE SCD1] Updated changed rows: {changed_rows}")

        df_changed_update.unpersist()

    else:
        print("[UPDATE SCD1] No changed records to update")

except Exception as e:
    print(f"[FAILED] Update SCD1 changed records failed: {str(e)}")
    raise


# ============================================================
# 11. End log and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()

    print("=" * 80)
    print("[SUCCESS] Gold SCD1 load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[SOURCE_OBJECT] {SOURCE_OBJECT}")
    print(f"[START_TIME] {execution_start_time}")
    print(f"[END_TIME] {execution_end_time}")
    print(f"[DURATION] {execution_end_time - execution_start_time}")
    print("-" * 80)
    print(f"[SUMMARY] Latest active Silver rows : {latest_silver_count}")
    print(f"[SUMMARY] New rows                  : {new_rows}")
    print(f"[SUMMARY] Changed rows              : {changed_rows}")
    print(f"[SUMMARY] Updated rows              : {changed_rows}")
    print("=" * 80)

finally:
    for df_name in [
        "df_silver_latest",
        "df_staged",
        "df_new",
        "df_changed",
        "df_gold_current"
    ]:
        if df_name in locals():
            try:
                locals()[df_name].unpersist()
            except:
                pass

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
