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
# DEDICATED GOLD NOTEBOOK — INFERRED DIM_PEOPLE
# Source fact staging: Silver.Sales_Orders + Silver.Sales_OrderLines
# Target dimension : Gold.Dim_People
# Purpose:
#   - Run before loading Fact_Sales
#   - Check PersonID from fact source
#   - If PersonID does not exist in Dim_People, insert inferred row
#   - Follow Velocity inferred template:
#       trim(coalesce(key, ''))
#       row_number over partition by business key
#       insert inferred_flag = 1
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime

# ============================================================
# 1. Hardcode
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

DIM_SOURCE_OBJECT = "Application_People"
TARGET_OBJECT = "Dim_People"

SOURCE_FACT_OBJECT_ORDERS = "Sales_Orders"
SOURCE_FACT_OBJECT_ORDERLINES = "Sales_OrderLines"

SOURCE_FACT_SCHEMA = "Silver"
TARGET_SCHEMA = "Gold"

SOURCE_ORDERS_TABLE = f"{METADATA_DB}.{SOURCE_FACT_SCHEMA}.{SOURCE_FACT_OBJECT_ORDERS}"
SOURCE_ORDERLINES_TABLE = f"{METADATA_DB}.{SOURCE_FACT_SCHEMA}.{SOURCE_FACT_OBJECT_ORDERLINES}"

GOLD_TABLE = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"

SKEY_COL = "person_skey"

DEFAULT_SCD_TO = "2999-12-31 00:00:00"

# ============================================================
# Map business key của dimension sang cột trong source fact
# ============================================================

DIM_TO_SOURCE_KEY_MAP = {
    "PersonID": "o.SalespersonPersonID"
}

execution_start_time = datetime.now()

# Flags để stop notebook thành công, không làm Fabric Activity failed
no_source_business_keys = False
no_inferred_rows = False

print("=" * 80)
print("[START] Inferred Dim_People load started")
print(f"[SOURCE_ORDERS_TABLE] {SOURCE_ORDERS_TABLE}")
print(f"[SOURCE_ORDERLINES_TABLE] {SOURCE_ORDERLINES_TABLE}")
print(f"[TARGET_TABLE] {GOLD_TABLE}")
print(f"[SKEY_COL] {SKEY_COL}")
print(f"[START_TIME] {execution_start_time}")
print("=" * 80)


# ============================================================
# 2. Helper functions
# ============================================================

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def get_row_value(row, col_name, default=None):
    return row.asDict().get(col_name, default)


def split_config_list(value):
    if value is None:
        return []
    return [x.strip() for x in value.split(",") if x.strip()]


def source_col(source_ref: str):
    alias_name, col_name = source_ref.split(".", 1)
    return F.col(f"{alias_name}.`{col_name}`")


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


def create_template_key_expr(source_ref: str, alias_name: str):
    # Follow SQL template:
    # trim(COALESCE(source_key, '')) AS dim_business_key
    return F.trim(
        F.coalesce(
            source_col(source_ref).cast("string"),
            F.lit("")
        )
    ).alias(alias_name)


def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0

    max_skey = (
        spark.table(target_table)
        .agg(F.max(F.col(skey_col)).alias("max_skey"))
        .collect()[0]["max_skey"]
    )

    return int(max_skey or 0)


def add_surrogate_key(df, target_table: str, skey_col: str, business_keys: list):
    max_skey = get_max_skey(target_table, skey_col)

    order_cols = [F.col(c).asc() for c in business_keys]
    order_cols.append(F.col("hash_key").asc())

    w_skey = Window.orderBy(*order_cols)

    return df.withColumn(
        skey_col,
        F.lit(max_skey) + F.row_number().over(w_skey)
    )


def align_to_target_columns(df, target_table: str):
    if not table_exists(target_table):
        return df

    target_df = spark.table(target_table)
    target_cols = target_df.columns
    target_schema = {f.name: f.dataType for f in target_df.schema.fields}

    for c in target_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(target_schema[c]))
        else:
            df = df.withColumn(c, F.col(c).cast(target_schema[c]))

    return df.select(*target_cols)


# ============================================================
# 3. Load dimension config
# ============================================================

try:
    config_row = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("source_object") == DIM_SOURCE_OBJECT) &
            (F.col("is_active") == 1)
        )
        .limit(1)
        .collect()
    )

    if not config_row:
        raise RuntimeError(
            f"[ERROR] Không tìm thấy config cho source_object = {DIM_SOURCE_OBJECT}"
        )

    metadata = config_row[0]

    business_keys = split_config_list(get_row_value(metadata, "business_key"))
    configured_columns = split_config_list(get_row_value(metadata, "column_list"))

    if not business_keys:
        raise RuntimeError("[ERROR] business_key trong config đang trống")

    if not configured_columns:
        raise RuntimeError("[ERROR] column_list trong config đang trống")

    print(f"[CONFIG] Business keys      : {business_keys}")
    print(f"[CONFIG] Key mapping        : {DIM_TO_SOURCE_KEY_MAP}")
    print(f"[CONFIG] Configured columns : {configured_columns}")

except Exception as e:
    print(f"[FAILED] Load dimension config failed: {str(e)}")
    raise


# ============================================================
# 4. Read business keys from source fact
# ============================================================

try:
    df_orders = spark.table(SOURCE_ORDERS_TABLE)
    df_orderlines = spark.table(SOURCE_ORDERLINES_TABLE)

    df_fact_source = (
        df_orderlines.alias("ol")
        .join(
            df_orders.alias("o"),
            F.col("ol.OrderID") == F.col("o.OrderID"),
            "inner"
        )
    )

    key_exprs = [
        create_template_key_expr(
            source_ref=DIM_TO_SOURCE_KEY_MAP[c],
            alias_name=c
        )
        for c in business_keys
    ]

    # Với Dim_People trong Fact_Sales, key lấy từ Orders,
    # nên audit_ts/source_id cũng lấy từ Orders để bám cùng source key.
    audit_ts_expr = F.to_timestamp(F.col("o.audit_ts")).alias("audit_ts")
    source_id_expr = F.col("o.source_id").alias("source_id")

    df_source_keys_raw = (
        df_fact_source
        .select(
            *key_exprs,
            audit_ts_expr,
            source_id_expr
        )
    )

    w_key = Window.partitionBy(*business_keys).orderBy(F.col("audit_ts").asc())

    df_source_keys = (
        df_source_keys_raw
        .withColumn("rnk", F.row_number().over(w_key))
        .filter(F.col("rnk") == 1)
        .drop("rnk")
        .withColumn("hash_key", create_hash_expr(business_keys))
        .cache()
    )

    source_key_count = df_source_keys.count()

    print(f"[SOURCE] Source business keys: {source_key_count}")

    if source_key_count == 0:
        no_source_business_keys = True

        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Inferred Dim_People load completed - No source business keys")
        print("[STOP] No source business keys")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[BUSINESS_KEYS] {business_keys}")
        print(f"[SOURCE_KEYS] {source_key_count}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

except Exception as e:
    print(f"[FAILED] Read source business keys failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if source has no business keys
# IMPORTANT:
#   - Không dùng raise SystemExit
#   - Không đặt notebookutils.notebook.exit trong try/except
# ============================================================

if no_source_business_keys:
    if "df_source_keys" in locals():
        df_source_keys.unpersist()
    notebookutils.notebook.exit("NO_SOURCE_BUSINESS_KEYS")


# ============================================================
# 5. Check key exists in dimension
#    Follow template:
#    WHERE NOT EXISTS (...)
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")

    if table_exists(GOLD_TABLE):
        df_existing_keys = (
            spark.table(GOLD_TABLE)
            .select("hash_key")
            .dropDuplicates(["hash_key"])
        )

        df_missing_keys = (
            df_source_keys
            .join(df_existing_keys, on="hash_key", how="left_anti")
            .cache()
        )
    else:
        df_missing_keys = df_source_keys.cache()

    missing_key_count = df_missing_keys.count()

    print(f"[CHECK] Missing dimension keys: {missing_key_count}")

    if missing_key_count == 0:
        no_inferred_rows = True

        execution_end_time = datetime.now()

        print("=" * 80)
        print("[SUCCESS] Inferred Dim_People load completed - No inferred rows to insert")
        print("[STOP] No inferred rows to insert")
        print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
        print(f"[BUSINESS_KEYS] {business_keys}")
        print(f"[SOURCE_KEYS] {source_key_count}")
        print(f"[MISSING_KEYS] {missing_key_count}")
        print(f"[START_TIME] {execution_start_time}")
        print(f"[END_TIME] {execution_end_time}")
        print(f"[DURATION] {execution_end_time - execution_start_time}")
        print("=" * 80)

except Exception as e:
    print(f"[FAILED] Check missing keys failed: {str(e)}")
    raise


# ============================================================
# Stop notebook successfully if no inferred rows
# IMPORTANT:
#   - Không dùng raise SystemExit
#   - Không đặt notebookutils.notebook.exit trong try/except
# ============================================================

if no_inferred_rows:
    if "df_source_keys" in locals():
        df_source_keys.unpersist()
    if "df_missing_keys" in locals():
        df_missing_keys.unpersist()
    notebookutils.notebook.exit("NO_INFERRED_ROWS")


# ============================================================
# 6. Insert inferred rows
#    Follow template:
#    - source_id
#    - scd_start
#    - business key
#    - audit_ts
#    - inferred_flag = 1
#    - row_hash = ''
# ============================================================

try:
    df_inferred = (
        df_missing_keys
        .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
        .withColumn("deleted_audit_ts", F.lit(None).cast(TimestampType()))
        .withColumn("scd_start", F.current_timestamp())
        .withColumn("scd_active", F.lit(1).cast(IntegerType()))
        .withColumn("scd_version", F.lit(1).cast(IntegerType()))
        .withColumn("inferred_flag", F.lit(1).cast(IntegerType()))
        .withColumn("row_hash", F.lit(""))
    )

    all_dim_cols = list(dict.fromkeys(business_keys + configured_columns))

    for c in all_dim_cols:
        if c not in df_inferred.columns:
            df_inferred = df_inferred.withColumn(c, F.lit(None))

    df_inferred = add_surrogate_key(
        df_inferred,
        GOLD_TABLE,
        SKEY_COL,
        business_keys
    )

    df_inferred = df_inferred.select(
        SKEY_COL,
        *[c for c in df_inferred.columns if c != SKEY_COL]
    )

    if table_exists(GOLD_TABLE):
        df_inferred = align_to_target_columns(df_inferred, GOLD_TABLE)

    # Count trước khi write để tránh log bị lệch sau khi append vào target
    inferred_rows = df_inferred.count()

    if table_exists(GOLD_TABLE):
        df_inferred.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(GOLD_TABLE)

    else:
        df_inferred.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(GOLD_TABLE)

    print(f"[INSERT] Inserted inferred rows: {inferred_rows}")

except Exception as e:
    print(f"[FAILED] Insert inferred rows failed: {str(e)}")
    raise


# ============================================================
# 7. End log and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()

    print("=" * 80)
    print("[SUCCESS] Inferred Dim_People load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[BUSINESS_KEYS] {business_keys}")
    print(f"[SOURCE_KEYS] {source_key_count}")
    print(f"[MISSING_KEYS] {missing_key_count}")
    print(f"[INSERTED_ROWS] {inferred_rows}")
    print(f"[START_TIME] {execution_start_time}")
    print(f"[END_TIME] {execution_end_time}")
    print(f"[DURATION] {execution_end_time - execution_start_time}")
    print("=" * 80)

finally:
    if "df_source_keys" in locals():
        df_source_keys.unpersist()
    if "df_missing_keys" in locals():
        df_missing_keys.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
