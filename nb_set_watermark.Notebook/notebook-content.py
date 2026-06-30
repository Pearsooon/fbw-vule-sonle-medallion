# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ad85bd8b-dd57-4344-a645-fffdbabe97ee",
# META       "default_lakehouse_name": "lh_vule_sonle_medallion",
# META       "default_lakehouse_workspace_id": "f6c5af39-3a0c-437b-a30c-1863bf3f1d7b",
# META       "known_lakehouses": [
# META         {
# META           "id": "ad85bd8b-dd57-4344-a645-fffdbabe97ee"
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
