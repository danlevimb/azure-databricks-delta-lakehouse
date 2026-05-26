# Databricks notebook source
# %%
# COMMAND ----------

# DBTITLE 1,Base config
from pyspark.sql import functions as F
from functools import reduce
from delta.tables import DeltaTable

landing_base_path = f"abfss://landing@{storage_account}.dfs.core.windows.net"
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta_lakehouse"

source_base_path = f"{landing_base_path}/source_data"

batches = [
    "batch_001",
    "batch_002",
    "batch_003_schema_evolution"
]

entities = [
    "customers",
    "products",
    "orders",
    "order_items"
]

print("Bronze ingestion configuration loaded")
print(f"Source path: {source_base_path}")
print(f"Bronze path: {bronze_base_path}")

#%%
# COMMAND ----------

# DBTITLE 1,Helper's
def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

def union_by_name_allow_missing(dataframes):
    if not dataframes:
        return None

    result = dataframes[0]

    for df in dataframes[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result

def add_bronze_metadata(df, entity_name: str, batch_id: str, source_path: str):
    raw_columns = df.columns

    raw_record_hash = F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
                for column_name in raw_columns
            ]
        ),
        256
    )

    return (
        df
        .withColumn("ingestion_batch_id", F.lit(batch_id))
        .withColumn("source_entity", F.lit(entity_name))
        .withColumn("source_file_path", F.lit(source_path))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("bronze_load_date", F.current_date())
        .withColumn("raw_record_hash", raw_record_hash)
    )

#%%
# COMMAND ----------

# DBTITLE 1,Clean Bronze container
dbutils.fs.rm(bronze_base_path, recurse=True)
print(f"Cleaned Bronze project path: {bronze_base_path}")

#%%
# COMMAND ----------

# DBTITLE 1,Bronze ingestion
bronze_results = []

for entity in entities:
    entity_dfs = []

    for batch_id in batches:
        source_file_path = f"{source_base_path}/{batch_id}/{entity}.csv"

        if path_exists(source_file_path):
            df_source = (
                spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(source_file_path)
            )

            df_bronze = add_bronze_metadata(
                df=df_source,
                entity_name=entity,
                batch_id=batch_id,
                source_path=source_file_path
            )

            entity_dfs.append(df_bronze)

            bronze_results.append({
                "entity": entity,
                "batch_id": batch_id,
                "source_file_path": source_file_path,
                "rows_read": df_source.count()
            })

        else:
            print(f"Skipped missing file: {source_file_path}")

    final_entity_df = union_by_name_allow_missing(entity_dfs)

    if final_entity_df is not None:
        target_path = f"{bronze_base_path}/bronze_{entity}"

        (
            final_entity_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(target_path)
        )

        print(f"Bronze Delta written: {target_path}")
    else:
        print(f"No files found for entity: {entity}")

#%%
# COMMAND ----------

# DBTITLE 1,Ingestion resume
bronze_summary_df = spark.createDataFrame(bronze_results)

display(bronze_summary_df.orderBy("entity", "batch_id"))

#%%
# COMMAND ----------

# DBTITLE 1,Bronze tables validation
for entity in entities:
    target_path = f"{bronze_base_path}/bronze_{entity}"

    df = spark.read.format("delta").load(target_path)

    print(f"\nbronze_{entity}")
    print(f"Rows: {df.count()}")

    display(
        df
        .groupBy("ingestion_batch_id")
        .count()
        .orderBy("ingestion_batch_id")
    )

#%%
# COMMAND ----------

# DBTITLE 1,Customers schema validation bronze evolution
bronze_customers_path = f"{bronze_base_path}/bronze_customers"

df_bronze_customers = spark.read.format("delta").load(bronze_customers_path)

df_bronze_customers.printSchema()

display(
    df_bronze_customers
    .select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "loyalty_tier",
        "effective_update_ts",
        "ingestion_batch_id",
        "raw_record_hash"
    )
    .orderBy("customer_id", "effective_update_ts")
)

#%%
# COMMAND ----------

# DBTITLE 1,Delta history

for entity in entities:
    target_path = f"{bronze_base_path}/bronze_{entity}"

    print(f"\nDelta history for bronze_{entity}")
    
    delta_table = DeltaTable.forPath(spark, target_path)

    display(delta_table.history())