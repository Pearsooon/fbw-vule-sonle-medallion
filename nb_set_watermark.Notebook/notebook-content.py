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

# PARAMETERS CELL ********************

# parameters
object_name = ""
watermark_value = ""
layer_value=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from datetime import datetime

data = [
    Row(
        timestamp=datetime.utcnow(),
        layer=layer_value,
        object_name=object_name,
        key_1=str(watermark_value),
        key_1_desc="datetime"
    )
]

df = spark.createDataFrame(data)

df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("etl.watermark")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
