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
# GOLD NOTEBOOK — FACT_SALES (Tích hợp Inferred Dimensions)
# Source: Silver.Sales_Orders + Silver.Sales_OrderLines
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DecimalType
from delta.tables import DeltaTable
from datetime import datetime

# ============================================================
# 1. Hardcode parameters
# ============================================================

METADATA_DB = "lh_vule_sonle_medallion"

SOURCE_OBJECT_ORDERS = "Sales_Orders"
SOURCE_OBJECT_ORDERLINES = "Sales_OrderLines"
TARGET_OBJECT = "Fact_Sales"
TARGET_SCHEMA = "Gold"

CONFIG_TABLE = f"{METADATA_DB}.etl.config_tables"            # <-- đổi sang bảng unified
WATERMARK_TABLE = f"{METADATA_DB}.etl.watermark"

gold_table = f"{METADATA_DB}.{TARGET_SCHEMA}.{TARGET_OBJECT}"
silver_orders_table = f"{METADATA_DB}.Silver.{SOURCE_OBJECT_ORDERS}"
silver_orderlines_table = f"{METADATA_DB}.Silver.{SOURCE_OBJECT_ORDERLINES}"

# Fact grain: 1 row per sales order line
FACT_SKEY_COL = "fact_sales_skey"
FACT_BUSINESS_KEYS = ["OrderLineID"]

# Dimension table names
DIM_CUSTOMER_TABLE = f"{METADATA_DB}.Gold.Dim_Customer"
DIM_PRODUCT_TABLE = f"{METADATA_DB}.Gold.Dim_StockItems"
DIM_PERSON_TABLE = f"{METADATA_DB}.Gold.Dim_People"

# Source objects để lấy config dimension
DIM_CUSTOMER_SOURCE = "Sales_Customers"
DIM_STOCKITEMS_SOURCE = "Warehouse_StockItems"
DIM_PEOPLE_SOURCE = "Application_People"

DEFAULT_SCD_TO = "2999-12-31 00:00:00"

execution_start_time = datetime.now()
no_new_data = False

print("=" * 80)
print("[START] Gold fact load for FACT_SALES")
print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
print(f"[START_TIME]    {execution_start_time}")
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

def create_hash_expr(cols):
    return F.sha2(
        F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in cols]
        ),
        256
    )

def source_change_ts_expr():
    # Chỉ có audit_ts, không có deleted_audit_ts
    return F.to_timestamp(F.col("audit_ts"))

def get_max_skey(target_table: str, skey_col: str) -> int:
    if not table_exists(target_table):
        return 0
    if skey_col not in spark.table(target_table).columns:
        return 0
    result = (
        spark.table(target_table)
        .agg(F.max(F.col(skey_col)).alias("max_skey"))
        .collect()[0]["max_skey"]
    )
    return int(result or 0)

def add_surrogate_key(df, target_table: str, skey_col: str, order_cols: list):
    max_skey = get_max_skey(target_table, skey_col)
    sort_cols = []
    for c in order_cols:
        if c in df.columns:
            sort_cols.append(F.col(c).asc())
    sort_cols += [F.col("hash_key").asc()]
    w_skey = Window.orderBy(*sort_cols)
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

def lookup_latest_dimension(
    df,
    dim_table: str,
    source_key_col: str,
    dim_key_col: str,
    skey_col: str
):
    dim_latest = (
        spark.table(dim_table)
        .filter(F.col("scd_active") == 1)
        .select(
            F.col(dim_key_col).alias(f"dim_{dim_key_col}"),
            F.col(skey_col).alias(f"dim_{skey_col}")
        )
        .dropDuplicates([f"dim_{dim_key_col}"])
    )

    return (
        df.join(
            F.broadcast(dim_latest),
            df[source_key_col] == F.col(f"dim_{dim_key_col}"),
            how="left"
        )
        .withColumn(
            skey_col,
            F.coalesce(F.col(f"dim_{skey_col}"), F.lit(-1)).cast(IntegerType())
        )
        .drop(f"dim_{dim_key_col}", f"dim_{skey_col}")
    )

# NEW: Hàm chèn inferred members vào dimension
def insert_inferred_dim_rows(dim_table, missing_df, business_keys, skey_col, effective_ts_col):
    if missing_df.isEmpty():
        return 0

    now_ts = F.current_timestamp()

    inferred_rows = (
        missing_df
        .withColumn("audit_ts", now_ts)
        .withColumn("updated_audit_ts", F.lit(None).cast(TimestampType()))
        .withColumn("scd_active", F.lit(1).cast(IntegerType()))
        .withColumn("scd_start", now_ts)
        .withColumn("inferred_flag", F.lit(1).cast(IntegerType()))
        .withColumn("row_hash", F.lit(""))
    )

    inferred_rows = add_surrogate_key(
        inferred_rows,
        dim_table,
        skey_col,
        business_keys
    )

    if table_exists(dim_table):
        target_cols = spark.table(dim_table).columns
        # Lấy schema (tên cột -> DataType) của bảng đích
        target_schema = {f.name: f.dataType for f in spark.table(dim_table).schema.fields}

        # Đảm bảo có tất cả các cột, với kiểu chính xác
        for c in target_cols:
            if c in inferred_rows.columns:
                # Ép kiểu nếu cần (Delta yêu cầu khớp chính xác)
                inferred_rows = inferred_rows.withColumn(c, F.col(c).cast(target_schema[c]))
            else:
                inferred_rows = inferred_rows.withColumn(c, F.lit(None).cast(target_schema[c]))

        # Sắp xếp cột đúng thứ tự bảng đích
        inferred_rows = inferred_rows.select(*target_cols)

        inferred_rows.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(dim_table)

    else:
        # Lần đầu tạo bảng – tự do chọn kiểu
        ordered_cols = [skey_col] + [c for c in inferred_rows.columns if c != skey_col]
        inferred_rows.select(*ordered_cols) \
            .write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(dim_table)

    return inferred_rows.count()

# ============================================================
# 3. Load Gold config cho Fact và Dimensions
# ============================================================

try:
    config_sources = [
        SOURCE_OBJECT_ORDERS,
        SOURCE_OBJECT_ORDERLINES,
        DIM_CUSTOMER_SOURCE,
        DIM_STOCKITEMS_SOURCE,
        DIM_PEOPLE_SOURCE
    ]

    config_rows = (
        spark.table(CONFIG_TABLE)
        .filter(
            (F.col("layer") == "Gold") &  
            (F.col("source_object").isin(config_sources)) &
            (F.col("is_active") == 1)
        )
        .collect()
    )

    config_map = {
        get_row_value(r, "source_object"): r
        for r in config_rows
    }

    def get_config(source_obj):
        r = config_map[source_obj]
        return (
            split_config_list(get_row_value(r, "business_key")),
            split_config_list(get_row_value(r, "column_list"))
        )

    orders_business_keys, orders_configured_columns = get_config(SOURCE_OBJECT_ORDERS)
    orderlines_business_keys, orderlines_configured_columns = get_config(SOURCE_OBJECT_ORDERLINES)

    cust_bk, cust_cols = get_config(DIM_CUSTOMER_SOURCE)
    stock_bk, stock_cols = get_config(DIM_STOCKITEMS_SOURCE)
    people_bk, people_cols = get_config(DIM_PEOPLE_SOURCE)

    FACT_BUSINESS_KEYS = ["OrderLineID"]

    print(f"[CONFIG] Fact business key: {FACT_BUSINESS_KEYS}")

except Exception as e:
    print(f"[FAILED] Load Gold config failed: {str(e)}")
    raise


# ============================================================
# 4. Load Gold watermark
# ============================================================

try:
    watermark_row = (
        spark.table(WATERMARK_TABLE)
        .filter(
            (F.col("layer") == "gold") &
            (F.col("object_name") == TARGET_OBJECT)
        )
        .orderBy(F.col("timestamp").desc())
        .limit(1)
        .select(F.to_timestamp("key_1").alias("max_audit_ts"))
        .collect()
    )
    gold_watermark_ts = watermark_row[0]["max_audit_ts"] if watermark_row else None
    print(f"[WATERMARK] Current Gold watermark: {gold_watermark_ts}")

except Exception as e:
    print(f"[FAILED] Load Gold watermark failed: {str(e)}")
    raise


# ============================================================
# 5. Read records from Silver Orders and OrderLines
# ============================================================

try:
    if not table_exists(silver_orders_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_orders_table}")
    if not table_exists(silver_orderlines_table):
        raise RuntimeError(f"[ERROR] Source Silver table không tồn tại: {silver_orderlines_table}")

    df_orders = spark.table(silver_orders_table)
    df_orderlines = spark.table(silver_orderlines_table)

    if gold_watermark_ts is None:
        df_filterd_orders = df_orders
        df_filterd_orderlines = df_orderlines
    else:
        df_filterd_orders = df_orders.filter(source_change_ts_expr() > F.lit(gold_watermark_ts))
        df_filterd_orderlines = df_orderlines.filter(source_change_ts_expr() > F.lit(gold_watermark_ts))

    filterd_orders_count = df_filterd_orders.count()
    filterd_orderlines_count = df_filterd_orderlines.count()

    if filterd_orders_count == 0 and filterd_orderlines_count == 0:
        no_new_data = True
        print("[INFO] No new data, stopping...")
    else:
        max_orders_audit_ts = (
            df_filterd_orders.agg(F.max(F.to_timestamp(F.col("audit_ts")))).collect()[0][0]
        ) if filterd_orders_count > 0 else None
        max_orderlines_audit_ts = (
            df_filterd_orderlines.agg(F.max(F.to_timestamp(F.col("audit_ts")))).collect()[0][0]
        ) if filterd_orderlines_count > 0 else None
        max_source_audit_ts = max([ts for ts in [max_orders_audit_ts, max_orderlines_audit_ts] if ts is not None])
        print(f"[INFO] filterd Orders rows    : {filterd_orders_count}")
        print(f"[INFO] filterd OrderLines rows: {filterd_orderlines_count}")
        print(f"[INFO] Max source audit_ts    : {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Read changed Silver records failed: {str(e)}")
    raise

if no_new_data:
    notebookutils.notebook.exit("NO_NEW_DATA")


# ============================================================
# 6. Join changed Orders + changed OrderLines -> df_fact_base
# ============================================================

try:
    o = df_filterd_orders.alias("o")
    ol = df_filterd_orderlines.alias("ol")

    df_fact_base = (
        ol.join(o, on="OrderID", how="inner")
        .select(
            F.col("ol.*"),
            F.col("o.CustomerID").alias("CustomerID"),
            F.col("o.OrderDate").alias("OrderDate"),
            F.col("o.ExpectedDeliveryDate").alias("ExpectedDeliveryDate"),
            F.col("o.SalespersonPersonID").alias("SalespersonPersonID"),
            F.col("o.audit_ts").alias("orders_audit_ts"),
            F.col("o.source_id").alias("orders_source_id")
        )
        .withColumn(
            "audit_ts",
            F.greatest(
                F.to_timestamp(F.col("audit_ts")),
                F.to_timestamp(F.col("orders_audit_ts"))
            )
        )
        .withColumn("updated_audit_ts", F.current_timestamp())
        .withColumn(
            "source_id",
            F.coalesce(
                F.col("source_id"),
                F.col("orders_source_id"),
                F.lit(-1)
            ).cast(IntegerType())
        )
        .withColumn("OrderDate", F.to_date(F.col("OrderDate")))
        .withColumn("ExpectedDeliveryDate", F.to_date(F.col("ExpectedDeliveryDate")))
        .withColumn(
            "fact_effective_ts",
                F.to_timestamp(F.col("audit_ts"))
            )
        .drop("orders_audit_ts", "orders_source_id")
    )

    # Measures
    df_fact_base = (
        df_fact_base
        .withColumn("Quantity", F.coalesce(F.col("Quantity"), F.lit(0)).cast(DecimalType(18,4)))
        .withColumn("UnitPrice", F.coalesce(F.col("UnitPrice"), F.lit(0)).cast(DecimalType(18,4)))
        .withColumn("TaxRate", F.coalesce(F.col("TaxRate"), F.lit(0)).cast(DecimalType(18,4)))
        .withColumn("PickedQuantity", F.coalesce(F.col("PickedQuantity"), F.lit(0)).cast(DecimalType(18,4)))
        .withColumn("sales_amount_net", (F.col("Quantity") * F.col("UnitPrice")).cast(DecimalType(18,4)))
        .withColumn("tax_amount", (F.col("sales_amount_net") * F.col("TaxRate") / 100).cast(DecimalType(18,4)))
        .withColumn("sales_amount_gross", (F.col("sales_amount_net") + F.col("tax_amount")).cast(DecimalType(18,4)))
    )

    fact_base_count = df_fact_base.count()
    print(f"[FACT BASE] Prepared {fact_base_count} rows")

except Exception as e:
    print(f"[FAILED] Prepare fact base failed: {str(e)}")
    raise


# ============================================================
# 7. NEW: Ensure dimension keys exist (insert inferred if missing)
# ============================================================

try:
    print("[INFERRED] Checking dimension keys from fact data...")

    df_cust_keys = df_fact_base.select("CustomerID", "fact_effective_ts").distinct() \
        .withColumnRenamed("fact_effective_ts", "effective_ts")
    df_stock_keys = df_fact_base.select("StockItemID", "fact_effective_ts").distinct() \
        .withColumnRenamed("fact_effective_ts", "effective_ts")
    df_person_keys = df_fact_base.select("SalespersonPersonID", "fact_effective_ts").distinct() \
        .withColumnRenamed("fact_effective_ts", "effective_ts") \
        .withColumnRenamed("SalespersonPersonID", "PersonID")

    dim_targets = [
        {
            "table": DIM_CUSTOMER_TABLE,
            "source_col": "CustomerID",
            "keys": df_cust_keys,
            "business_keys": cust_bk,
            "skey_col": "customer_skey",
            "dim_columns": cust_cols
        },
        {
            "table": DIM_PRODUCT_TABLE,
            "source_col": "StockItemID",
            "keys": df_stock_keys,
            "business_keys": stock_bk,
            "skey_col": "stockitem_skey",
            "dim_columns": stock_cols
        },
        {
            "table": DIM_PERSON_TABLE,
            "source_col": "PersonID",
            "keys": df_person_keys,
            "business_keys": people_bk,
            "skey_col": "person_skey",
            "dim_columns": people_cols
        }
    ]

    inferred_counts = {}

    for dim in dim_targets:
        dim_table = dim["table"]
        keys_df = dim["keys"]
        biz_keys = dim["business_keys"]
        skey_col = dim["skey_col"]
        dim_cols = dim["dim_columns"]

        keys_with_hash = keys_df.withColumn("hash_key", create_hash_expr(biz_keys))

        if table_exists(dim_table):
            existing_hashes = spark.table(dim_table).select("hash_key").distinct()
            missing = keys_with_hash.join(existing_hashes, "hash_key", "left_anti")
        else:
            missing = keys_with_hash

        cnt = insert_inferred_dim_rows(
            dim_table=dim_table,
            missing_df=missing,
            business_keys=biz_keys,
            skey_col=skey_col,
            effective_ts_col="effective_ts"
        )
        inferred_counts[dim_table] = cnt
        print(f"[INFERRED] Inserted {cnt} rows into {dim_table}")

except Exception as e:
    print(f"[FAILED] Inferred dimension handling failed: {str(e)}")
    raise


# ============================================================
# 8. Lookup surrogate keys from latest/current Gold dimensions
# ============================================================

try:
    df_fact_dim = lookup_latest_dimension(
        df_fact_base,
        DIM_CUSTOMER_TABLE,
        "CustomerID",
        "CustomerID",
        "customer_skey"
    )

    df_fact_dim = lookup_latest_dimension(
        df_fact_dim,
        DIM_PRODUCT_TABLE,
        "StockItemID",
        "StockItemID",
        "stockitem_skey"
    )

    df_fact_dim = lookup_latest_dimension(
        df_fact_dim,
        DIM_PERSON_TABLE,
        "SalespersonPersonID",
        "PersonID",
        "person_skey"
    )

    print("[DIM LOOKUP] All latest dimension lookups completed")

except Exception as e:
    print(f"[FAILED] Dimension lookup failed: {str(e)}")
    raise


# ============================================================
# 9. Prepare staged fact rows (hash, select, dedup)
# ============================================================

try:

    row_hash_cols = [
        "OrderID", "OrderLineID", "CustomerID", "StockItemID", "SalespersonPersonID",
        "OrderDate", "ExpectedDeliveryDate", "Quantity", "UnitPrice", "TaxRate",
        "PickedQuantity", "customer_skey", "stockitem_skey", "person_skey",
        "sales_amount_net", "tax_amount", "sales_amount_gross"
    ]

    df_staged = (
        df_fact_dim
        .withColumn("hash_key", create_hash_expr(FACT_BUSINESS_KEYS))
        .withColumn("row_hash", create_hash_expr(row_hash_cols))
        .select(
            "hash_key", "row_hash", "audit_ts", "updated_audit_ts", "source_id",
            "OrderID",
            *(["OrderLineID"] if "OrderLineID" in df_fact_dim.columns else []),
            *(["CustomerID"] if "CustomerID" in df_fact_dim.columns else []),
            *(["StockItemID"] if "StockItemID" in df_fact_dim.columns else []),
            *(["SalespersonPersonID"] if "SalespersonPersonID" in df_fact_dim.columns else []),
            "customer_skey", "stockitem_skey", "person_skey",
            *(["OrderDate"] if "OrderDate" in df_fact_dim.columns else []),
            *(["ExpectedDeliveryDate"] if "ExpectedDeliveryDate" in df_fact_dim.columns else []),
            *(["Description"] if "Description" in df_fact_dim.columns else []),
            "Quantity", "UnitPrice", "TaxRate", "PickedQuantity",
            "sales_amount_net", "tax_amount", "sales_amount_gross",
            "fact_effective_ts"
        )
        .dropDuplicates(["hash_key"])
        .cache()
    )

    staged_count = df_staged.count()
    print(f"[STAGED] {staged_count} unique fact rows ready")

except Exception as e:
    print(f"[FAILED] Prepare staged fact rows failed: {str(e)}")
    raise


# ============================================================
# 10. Create Gold schema if needed
# ============================================================

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.{TARGET_SCHEMA}")
    print("[INFO] Gold schema checked")
except Exception as e:
    print(f"[FAILED] Create Gold schema failed: {str(e)}")
    raise


# ============================================================
# 11. Delete existing fact rows by hash_key
# ============================================================

try:
    deleted_rows = 0
    affected_fact_keys_df = df_staged.select("hash_key").dropDuplicates(["hash_key"]).cache()

    if table_exists(gold_table) and affected_fact_keys_df.count() > 0:
        df_existing_to_delete = spark.table(gold_table).join(
            F.broadcast(affected_fact_keys_df), on="hash_key", how="inner"
        )
        deleted_rows = df_existing_to_delete.count()
        DeltaTable.forName(spark, gold_table).alias("t") \
            .merge(affected_fact_keys_df.alias("s"), "t.hash_key = s.hash_key") \
            .whenMatchedDelete() \
            .execute()
        print(f"[DELETE] Deleted {deleted_rows} existing fact rows")
    else:
        print("[DELETE] No existing rows to delete")

except Exception as e:
    print(f"[FAILED] Delete existing fact rows failed: {str(e)}")
    raise


# ============================================================
# 12. Insert refreshed fact rows
# ============================================================

try:
    insert_rows = staged_count
    if insert_rows > 0:
        df_to_insert = add_surrogate_key(df_staged, gold_table, FACT_SKEY_COL, FACT_BUSINESS_KEYS)
        ordered_cols = [FACT_SKEY_COL] + [c for c in df_to_insert.columns if c != FACT_SKEY_COL]
        df_to_insert = df_to_insert.select(*ordered_cols)

        if table_exists(gold_table):
            df_to_insert = align_to_target_columns(df_to_insert, gold_table)
            df_to_insert.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(gold_table)
        else:
            df_to_insert.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table)

        print(f"[INSERT] Inserted {insert_rows} fact rows")
    else:
        print("[INSERT] No fact rows to insert")

except Exception as e:
    print(f"[FAILED] Insert fact rows failed: {str(e)}")
    raise


# ============================================================
# 13. Update Gold watermark
# ============================================================

try:
    watermark_new_df = (
        spark.createDataFrame(
            [("gold", TARGET_OBJECT, str(max_source_audit_ts), "datetime")],
            ["layer", "object_name", "key_1", "key_1_desc"]
        )
        .withColumn("timestamp", F.current_timestamp())
    )
    watermark_new_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(WATERMARK_TABLE)
    print(f"[WATERMARK] Updated Gold watermark to {max_source_audit_ts}")

except Exception as e:
    print(f"[FAILED] Update watermark failed: {str(e)}")
    raise


# ============================================================
# 14. Summary and cleanup
# ============================================================

try:
    execution_end_time = datetime.now()

    print("=" * 80)
    print("[SUCCESS] Gold fact load completed")
    print(f"[TARGET_OBJECT] {TARGET_OBJECT}")
    print(f"[START_TIME]    {execution_start_time}")
    print(f"[END_TIME]      {execution_end_time}")
    print(f"[DURATION]      {execution_end_time - execution_start_time}")
    print("-" * 80)
    print(f"[SUMMARY] Changed Orders rows    : {filterd_orders_count}")
    print(f"[SUMMARY] Changed OrderLines rows: {filterd_orderlines_count}")
    print(f"[SUMMARY] Deleted old fact rows  : {deleted_rows}")
    print(f"[SUMMARY] Inserted refreshed rows: {insert_rows}")
    for dim_tbl, cnt in inferred_counts.items():
        print(f"[SUMMARY] Inferred rows in {dim_tbl}: {cnt}")
    print(f"[SUMMARY] Watermark updated      : {max_source_audit_ts}")
    print("=" * 80)

finally:
    for df_name in ["df_staged", "affected_fact_keys_df"]:
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
