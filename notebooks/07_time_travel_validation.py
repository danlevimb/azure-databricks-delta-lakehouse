# Databricks notebook source
#%%

# DBTITLE 1,Base config
from delta.tables import DeltaTable
from pyspark.sql import functions as F

gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_lakehouse"
gold_dim_product_path = f"{gold_base_path}/gold_dim_product"

print("Delta time travel validation loaded")
print(f"Target Delta path: {gold_dim_product_path}")

#%%
# COMMAND ----------

# DBTITLE 1,Check Delta history
delta_table = DeltaTable.forPath(spark, gold_dim_product_path)

history_df = (
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

display(history_df)

#%%
# COMMAND ----------

# DBTITLE 1,Get available versions
history_versions = delta_table.history()

initial_version = (
    history_versions
    .agg(F.min("version").alias("initial_version"))
    .collect()[0]["initial_version"]
)

latest_version = (
    history_versions
    .agg(F.max("version").alias("latest_version"))
    .collect()[0]["latest_version"]
)

latest_merge_version = (
    history_versions
    .filter(F.col("operation") == "MERGE")
    .agg(F.max("version").alias("latest_merge_version"))
    .collect()[0]["latest_merge_version"]
)

print(f"Initial version: {initial_version}")
print(f"Latest version: {latest_version}")
print(f"Latest MERGE version: {latest_merge_version}")

#%%
# COMMAND ----------

# DBTITLE 1,Get initial version
product_version_initial = (
    spark.read
    .format("delta")
    .option("versionAsOf", initial_version)
    .load(gold_dim_product_path)
)

display(
    product_version_initial
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id"
    )
    .orderBy("product_id")
)

#%%
# COMMAND ----------

# DBTITLE 1,Get actual version
product_version_latest = (
    spark.read
    .format("delta")
    .option("versionAsOf", latest_version)
    .load(gold_dim_product_path)
)

display(
    product_version_latest
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id"
    )
    .orderBy("product_id")
)

#%%
# COMMAND ----------

# DBTITLE 1,View - Version count
version_counts = spark.createDataFrame([
    {
        "delta_version": int(initial_version),
        "version_label": "before_merge",
        "product_count": product_version_initial.count(),
        "distinct_product_count": product_version_initial.select("product_id").distinct().count()
    },
    {
        "delta_version": int(latest_version),
        "version_label": "after_merge",
        "product_count": product_version_latest.count(),
        "distinct_product_count": product_version_latest.select("product_id").distinct().count()
    }
])

display(version_counts.orderBy("delta_version"))

#%%
# COMMAND ----------

# DBTITLE 1,Validate - Product change
before_prod_002 = (
    product_version_initial
    .filter(F.col("product_id") == "PROD-002")
    .select(
        F.col("product_id"),
        F.col("product_name").alias("product_name_before"),
        F.col("unit_price").alias("unit_price_before"),
        F.col("ingestion_batch_id").alias("batch_before")
    )
)

after_prod_002 = (
    product_version_latest
    .filter(F.col("product_id") == "PROD-002")
    .select(
        F.col("product_id"),
        F.col("product_name").alias("product_name_after"),
        F.col("unit_price").alias("unit_price_after"),
        F.col("ingestion_batch_id").alias("batch_after")
    )
)

display(
    before_prod_002
    .join(after_prod_002, "product_id", "inner")
)

#%%
# COMMAND ----------

# DBTITLE 1,Validate - New product
prod_005_before_count = (
    product_version_initial
    .filter(F.col("product_id") == "PROD-005")
    .count()
)

prod_005_after_count = (
    product_version_latest
    .filter(F.col("product_id") == "PROD-005")
    .count()
)

product_insert_validation = spark.createDataFrame([
    {
        "product_id": "PROD-005",
        "exists_before_merge": prod_005_before_count > 0,
        "exists_after_merge": prod_005_after_count > 0,
        "before_count": prod_005_before_count,
        "after_count": prod_005_after_count
    }
])

display(product_insert_validation)

#%%
# COMMAND ----------

# DBTITLE 1,View - Affected products
affected_products_before = (
    product_version_initial
    .filter(F.col("product_id").isin("PROD-002", "PROD-005"))
    .select(
        F.lit("before_merge").alias("version_label"),
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id"
    )
)

affected_products_after = (
    product_version_latest
    .filter(F.col("product_id").isin("PROD-002", "PROD-005"))
    .select(
        F.lit("after_merge").alias("version_label"),
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id"
    )
)

affected_products_comparison = (
    affected_products_before
    .unionByName(affected_products_after)
    .withColumn(
        "version_sort",
        F.when(F.col("version_label") == "before_merge", 1)
         .when(F.col("version_label") == "after_merge", 2)
         .otherwise(99)
    )
)

display(
    affected_products_comparison
    .orderBy("product_id", "version_sort")
    .drop("version_sort")
)

#%%
# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Time Travel Validation
# MAGIC
# MAGIC This notebook validates Delta Lake time travel by reading `gold_dim_product` at different table versions.
# MAGIC
# MAGIC The initial version represents the table state before the MERGE operation.
# MAGIC The latest version represents the table state after the MERGE operation.
# MAGIC
# MAGIC Validated behavior:
# MAGIC
# MAGIC - `PROD-002` was updated through Delta MERGE.
# MAGIC - `PROD-005` was inserted through Delta MERGE.
# MAGIC - Delta history preserves the transaction log.
# MAGIC - Historical table states can be queried using `versionAsOf`.
# MAGIC
# MAGIC This supports reproducibility, auditability, and debugging for lakehouse workloads.