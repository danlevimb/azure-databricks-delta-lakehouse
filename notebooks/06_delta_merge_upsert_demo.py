# Databricks notebook source
#%%

# DBTITLE 1,Base config
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import Row

gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_lakehouse"
gold_dim_product_path = f"{gold_base_path}/gold_dim_product"

print("Delta MERGE / upsert demo configuration loaded")
print(f"Target Delta path: {gold_dim_product_path}")

#%%
# COMMAND ----------

# DBTITLE 1,Check Products - actual state
gold_dim_product_before = spark.read.format("delta").load(gold_dim_product_path)

display(
    gold_dim_product_before
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "record_hash"
    )
    .orderBy("product_id")
)

#%%
# COMMAND ----------

# DBTITLE 1,Build Products - Incremental batch
product_updates = spark.createDataFrame([
    Row(
        product_id="PROD-002",
        product_name="Ice Bag 5kg",
        category="Ice",
        unit_price=38.00,
        is_active=True,
        update_reason="price_adjustment"
    ),
    Row(
        product_id="PROD-005",
        product_name="Ice Bag 10kg",
        category="Ice",
        unit_price=62.00,
        is_active=True,
        update_reason="new_product"
    )
])

product_attributes = [
    "product_name",
    "category",
    "unit_price",
    "is_active"
]

product_updates_prepared = (
    product_updates
    .withColumn("product_sk", F.sha2(F.col("product_id").cast("string"), 256))
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
                    for column_name in product_attributes
                ]
            ),
            256
        )
    )
    .withColumn("ingestion_batch_id", F.lit("merge_demo_batch_001"))
    .withColumn("source_file_path", F.lit("manual_delta_merge_demo"))
    .withColumn("raw_record_hash", F.col("record_hash"))
    .withColumn("silver_processed_ts", F.current_timestamp())
    .withColumn("gold_processed_ts", F.current_timestamp())
)

display(product_updates_prepared.orderBy("product_id"))

#%%
# COMMAND ----------

# DBTITLE 1,Exec - MERGE / upsert
target_delta_table = DeltaTable.forPath(spark, gold_dim_product_path)

(
    target_delta_table.alias("target")
    .merge(
        product_updates_prepared.alias("source"),
        "target.product_id = source.product_id"
    )
    .whenMatchedUpdate(
        condition="target.record_hash <> source.record_hash",
        set={
            "product_name": "source.product_name",
            "category": "source.category",
            "unit_price": "source.unit_price",
            "is_active": "source.is_active",
            "record_hash": "source.record_hash",
            "ingestion_batch_id": "source.ingestion_batch_id",
            "source_file_path": "source.source_file_path",
            "raw_record_hash": "source.raw_record_hash",
            "silver_processed_ts": "source.silver_processed_ts",
            "gold_processed_ts": "source.gold_processed_ts"
        }
    )
    .whenNotMatchedInsert(
        values={
            "product_sk": "source.product_sk",
            "product_id": "source.product_id",
            "product_name": "source.product_name",
            "category": "source.category",
            "unit_price": "source.unit_price",
            "is_active": "source.is_active",
            "record_hash": "source.record_hash",
            "ingestion_batch_id": "source.ingestion_batch_id",
            "source_file_path": "source.source_file_path",
            "raw_record_hash": "source.raw_record_hash",
            "silver_processed_ts": "source.silver_processed_ts",
            "gold_processed_ts": "source.gold_processed_ts"
        }
    )
    .execute()
)

print("Delta MERGE / upsert completed")

#%%
# COMMAND ----------

# DBTITLE 1,Validate MERGE result
gold_dim_product_after = spark.read.format("delta").load(gold_dim_product_path)

display(
    gold_dim_product_after
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id",
        "source_file_path",
        "gold_processed_ts"
    )
    .orderBy("product_id")
)

#%%
# COMMAND ----------

# DBTITLE 1,Validate count
display(
    gold_dim_product_after
    .agg(
        F.count("*").alias("total_products"),
        F.countDistinct("product_id").alias("distinct_products")
    )
)

#%%
# COMMAND ----------

# DBTITLE 1,MERGE - Delta history
delta_table = DeltaTable.forPath(spark, gold_dim_product_path)

display(
    delta_table.history()
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics",
        "userName"
    )
    .orderBy(F.col("version").desc())
)

#%%
# COMMAND ----------

# DBTITLE 1,Validate MERGE operation
display(
    delta_table.history()
    .filter(F.col("operation") == "MERGE")
    .select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
)