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
# Setup: etl.config_bronze_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_schema="Sales",
        source_table="Orders",
        source_object="Sales_Orders",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, "
            "DeliveryInstructions, InternalComments, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_schema="Sales",
        source_table="OrderLines",
        source_object="Sales_OrderLines",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_schema="Sales",
        source_table="Customers",
        source_object="Sales_Customers",
        load_type="full",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, "
            "RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_schema="Sales",
        source_table="CustomerCategories",
        source_object="Sales_CustomerCategories",
        load_type="full",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_schema="Warehouse",
        source_table="StockItems",
        source_object="Warehouse_StockItems",
        load_type="full",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_schema="Application",
        source_table="People",
        source_object="Application_People",
        load_type="full",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Setup: etl.config_bronze_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_silver_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_schema="Sales",
        source_table="Orders",
        source_object="Sales_Orders",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, "
            "DeliveryInstructions, InternalComments, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_schema="Sales",
        source_table="OrderLines",
        source_object="Sales_OrderLines",
        load_type="incremental",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_schema="Sales",
        source_table="Customers",
        source_object="Sales_Customers",
        load_type="full",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, "
            "RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_schema="Sales",
        source_table="CustomerCategories",
        source_object="Sales_CustomerCategories",
        load_type="full",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_schema="Warehouse",
        source_table="StockItems",
        source_object="Warehouse_StockItems",
        load_type="full",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_schema="Application",
        source_table="People",
        source_object="Application_People",
        load_type="full",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Setup: etl.config_gold_tables
# Run ONCE to create and seed the Bronze config table.
# ============================================================

from pyspark.sql import Row

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_gold_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

rows = [
    # ========================================================
    # Sales fact-related tables
    # Incremental load
    # ========================================================
    Row(
        config_id=1,
        source_system="WWI",
        source_object="Sales_Orders",
        watermark_column="LastEditedWhen",
        business_key="OrderID",
        column_list=(
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, "
            "PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    Row(
        config_id=2,
        source_system="WWI",
        source_object="Sales_OrderLines",
        watermark_column="LastEditedWhen",
        business_key="OrderLineID",
        column_list=(
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        ),
        is_active=True
    ),

    # ========================================================
    # Customer dimension tables
    # Full load
    # ========================================================
    Row(
        config_id=3,
        source_system="WWI",
        source_object="Sales_Customers",
        watermark_column=None,
        business_key="CustomerID",
        column_list=(
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, "
            "DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, DeliveryLocation, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    Row(
        config_id=4,
        source_system="WWI",
        source_object="Sales_CustomerCategories",
        watermark_column=None,
        business_key="CustomerCategoryID",
        column_list=(
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # Product / item dimension table
    # Full load
    # ========================================================
    Row(
        config_id=5,
        source_system="WWI",
        source_object="Warehouse_StockItems",
        watermark_column=None,
        business_key="StockItemID",
        column_list=(
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        ),
        is_active=True
    ),

    # ========================================================
    # People dimension table
    # Full load
    # ========================================================
    Row(
        config_id=6,
        source_system="WWI",
        source_object="Application_People",
        watermark_column=None,
        business_key="PersonID",
        column_list=(
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, LastEditedBy, "
            "ValidFrom, ValidTo"
        ),
        is_active=True
    ),
]

df = spark.createDataFrame(rows)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Setup: etl.config_tables (Unified config cho Bronze, Silver, Gold)
# Run ONCE to create and seed the unified config table.
# Updated: removed "DeliveryLocation" from Sales_Customers column_list
# ============================================================

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

METADATA_DB  = "lh_vule_sonle_medallion"
CONFIG_TABLE = f"{METADATA_DB}.etl.config_tables"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {METADATA_DB}.etl")

# Drop and recreate unified config table
spark.sql(f"DROP TABLE IF EXISTS {CONFIG_TABLE}")

schema = StructType([
    StructField("config_id", IntegerType(), False),
    StructField("layer", StringType(), False),
    StructField("source_system", StringType(), True),
    StructField("source_schema", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("source_object", StringType(), True),
    StructField("load_type", StringType(), True),
    StructField("watermark_column", StringType(), True),
    StructField("business_key", StringType(), True),
    StructField("column_list", StringType(), True),
    StructField("is_active", BooleanType(), True),
])

# ---- Định nghĩa dữ liệu gốc cho Silver (và Bronze) ----
base_rows = [
    {
        "source_system": "WWI",
        "source_schema": "Sales",
        "source_table": "Orders",
        "source_object": "Sales_Orders",
        "load_type": "incremental",
        "watermark_column": "LastEditedWhen",
        "business_key": "OrderID",
        "column_list": (
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, Comments, "
            "DeliveryInstructions, InternalComments, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        )
    },
    {
        "source_system": "WWI",
        "source_schema": "Sales",
        "source_table": "OrderLines",
        "source_object": "Sales_OrderLines",
        "load_type": "incremental",
        "watermark_column": "LastEditedWhen",
        "business_key": "OrderLineID",
        "column_list": (
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        )
    },
    {
        "source_system": "WWI",
        "source_schema": "Sales",
        "source_table": "Customers",
        "source_object": "Sales_Customers",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "CustomerID",
        # DeliveryLocation removed
        "column_list": (
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, DeliveryRun, "
            "RunPosition, WebsiteURL, DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_system": "WWI",
        "source_schema": "Sales",
        "source_table": "CustomerCategories",
        "source_object": "Sales_CustomerCategories",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "CustomerCategoryID",
        "column_list": (
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_system": "WWI",
        "source_schema": "Warehouse",
        "source_table": "StockItems",
        "source_object": "Warehouse_StockItems",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "StockItemID",
        "column_list": (
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, MarketingComments, InternalComments, Photo, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_system": "WWI",
        "source_schema": "Application",
        "source_table": "People",
        "source_object": "Application_People",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "PersonID",
        "column_list": (
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, HashedPassword, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, Photo, CustomFields, OtherLanguages, LastEditedBy, "
            "ValidFrom, ValidTo"
        )
    }
]

# ---- Tạo danh sách tất cả các dòng (Bronze, Silver, Gold) ----
all_rows = []
config_id = 1

# 1. Bronze rows (6 rows)
for item in base_rows:
    all_rows.append(Row(
        config_id=config_id,
        layer="Bronze",
        source_system=item["source_system"],
        source_schema=item["source_schema"],
        source_table=item["source_table"],
        source_object=item["source_object"],
        load_type=item["load_type"],
        watermark_column=item["watermark_column"],
        business_key=item["business_key"],
        column_list=item["column_list"],
        is_active=True
    ))
    config_id += 1

# 2. Silver rows (6 rows) - identical column_list
for item in base_rows:
    all_rows.append(Row(
        config_id=config_id,
        layer="Silver",
        source_system=item["source_system"],
        source_schema=item["source_schema"],
        source_table=item["source_table"],
        source_object=item["source_object"],
        load_type=item["load_type"],
        watermark_column=item["watermark_column"],
        business_key=item["business_key"],
        column_list=item["column_list"],
        is_active=True
    ))
    config_id += 1

# 3. Gold rows (6 rows) - different column_list, no source_schema/source_table
gold_configs = [
    {
        "source_object": "Sales_Orders",
        "load_type": "incremental",
        "watermark_column": "LastEditedWhen",
        "business_key": "OrderID",
        "column_list": (
            "OrderID, CustomerID, SalespersonPersonID, PickedByPersonID, "
            "ContactPersonID, BackorderOrderID, OrderDate, ExpectedDeliveryDate, "
            "CustomerPurchaseOrderNumber, IsUndersupplyBackordered, "
            "PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        )
    },
    {
        "source_object": "Sales_OrderLines",
        "load_type": "incremental",
        "watermark_column": "LastEditedWhen",
        "business_key": "OrderLineID",
        "column_list": (
            "OrderLineID, OrderID, StockItemID, Description, PackageTypeID, "
            "Quantity, UnitPrice, TaxRate, PickedQuantity, PickingCompletedWhen, "
            "LastEditedBy, LastEditedWhen"
        )
    },
    {
        "source_object": "Sales_Customers",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "CustomerID",
        # DeliveryLocation removed
        "column_list": (
            "CustomerID, CustomerName, BillToCustomerID, CustomerCategoryID, "
            "BuyingGroupID, PrimaryContactPersonID, AlternateContactPersonID, "
            "DeliveryMethodID, DeliveryCityID, PostalCityID, CreditLimit, "
            "AccountOpenedDate, StandardDiscountPercentage, IsStatementSent, "
            "IsOnCreditHold, PaymentDays, PhoneNumber, FaxNumber, "
            "DeliveryAddressLine1, DeliveryAddressLine2, "
            "DeliveryPostalCode, PostalAddressLine1, "
            "PostalAddressLine2, PostalPostalCode, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_object": "Sales_CustomerCategories",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "CustomerCategoryID",
        "column_list": (
            "CustomerCategoryID, CustomerCategoryName, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_object": "Warehouse_StockItems",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "StockItemID",
        "column_list": (
            "StockItemID, StockItemName, SupplierID, ColorID, UnitPackageID, "
            "OuterPackageID, Brand, Size, LeadTimeDays, QuantityPerOuter, "
            "IsChillerStock, Barcode, TaxRate, UnitPrice, RecommendedRetailPrice, "
            "TypicalWeightPerUnit, "
            "CustomFields, Tags, SearchDetails, LastEditedBy, ValidFrom, ValidTo"
        )
    },
    {
        "source_object": "Application_People",
        "load_type": "full",
        "watermark_column": None,
        "business_key": "PersonID",
        "column_list": (
            "PersonID, FullName, PreferredName, SearchName, IsPermittedToLogon, "
            "LogonName, IsExternalLogonProvider, IsSystemUser, "
            "IsEmployee, IsSalesperson, UserPreferences, PhoneNumber, FaxNumber, "
            "EmailAddress, LastEditedBy, "
            "ValidFrom, ValidTo"
        )
    }
]

for item in gold_configs:
    all_rows.append(Row(
        config_id=config_id,
        layer="Gold",
        source_system="WWI",          # vẫn giữ source system
        source_schema=None,
        source_table=None,
        source_object=item["source_object"],
        load_type=item["load_type"],
        watermark_column=item["watermark_column"],
        business_key=item["business_key"],
        column_list=item["column_list"],
        is_active=True
    ))
    config_id += 1

# ---- Tạo DataFrame và ghi bảng ----
df = spark.createDataFrame(all_rows, schema=schema)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(CONFIG_TABLE)

print(f"Created {CONFIG_TABLE} with {df.count()} rows.")
spark.table(CONFIG_TABLE).orderBy("layer", "config_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
