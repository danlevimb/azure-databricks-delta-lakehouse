# Databricks notebook source
# DBTITLE 1,Base config
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

storage_account = "stdanadblh4827"

silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta_lakehouse"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_lakehouse"

print("Gold dimensions configuration loaded")
print(f"Silver path: {silver_base_path}")
print(f"Gold path: {gold_base_path}")

# COMMAND ----------

# DBTITLE 1,Helpers
def read_silver(table_name: str):
    return spark.read.format("delta").load(f"{silver_base_path}/{table_name}")


def write_gold(df, table_name: str):
    target_path = f"{gold_base_path}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Gold Delta written: {target_path}")


def build_hash(columns):
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(F.col(column_name).cast("string"), F.lit("<NULL>"))
                for column_name in columns
            ]
        ),
        256
    )

# COMMAND ----------

# DBTITLE 1,Customer SCD2 staging
silver_customers = read_silver("silver_customers_clean")

customer_scd_attributes = [
    "customer_name",
    "email",
    "city",
    "state",
    "customer_segment",
    "loyalty_tier"
]

customer_staged = (
    silver_customers
    .select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "loyalty_tier",
        F.col("effective_update_ts").alias("effective_start_ts"),
        "ingestion_batch_id",
        "source_file_path",
        "raw_record_hash",
        "silver_processed_ts"
    )
    .withColumn("record_hash", build_hash(customer_scd_attributes))
)

display(
    customer_staged
    .orderBy("customer_id", "effective_start_ts")
)

# COMMAND ----------

# DBTITLE 1,Build gold_dim_customer_sdc2
"""
1. Ordenar cambios por customer_id y effective_start_ts.
2. Comparar hash actual contra hash anterior.
3. Mantener solo cambios reales.
4. Calcular effective_end_ts con LEAD.
5. Marcar is_current.
6. Crear surrogate key determinística.
"""

customer_window = Window.partitionBy("customer_id").orderBy("effective_start_ts", "record_hash")

customer_changes = (
    customer_staged
    .withColumn("previous_record_hash", F.lag("record_hash").over(customer_window))
    .filter(
        F.col("previous_record_hash").isNull() |
        (F.col("record_hash") != F.col("previous_record_hash"))
    )
    .drop("previous_record_hash")
)

customer_history_window = Window.partitionBy("customer_id").orderBy("effective_start_ts")

gold_dim_customer_scd2 = (
    customer_changes
    .withColumn("effective_end_ts", F.lead("effective_start_ts").over(customer_history_window))
    .withColumn("is_current", F.col("effective_end_ts").isNull())
    .withColumn(
        "customer_sk",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("customer_id"),
                F.col("effective_start_ts").cast("string"),
                F.col("record_hash")
            ),
            256
        )
    )
    .withColumn("gold_processed_ts", F.current_timestamp())
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "loyalty_tier",
        "effective_start_ts",
        "effective_end_ts",
        "is_current",
        "record_hash",
        "ingestion_batch_id",
        "source_file_path",
        "raw_record_hash",
        "silver_processed_ts",
        "gold_processed_ts"
    )
)

write_gold(gold_dim_customer_scd2, "gold_dim_customer_scd2")

display(
    gold_dim_customer_scd2
    .orderBy("customer_id", "effective_start_ts")
)

# COMMAND ----------

# DBTITLE 1,SCD2 Validations
display(
    gold_dim_customer_scd2
    .filter(F.col("customer_id").isin("CUST-001", "CUST-002", "CUST-003"))
    .select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "customer_segment",
        "loyalty_tier",
        "effective_start_ts",
        "effective_end_ts",
        "is_current"
    )
    .orderBy("customer_id", "effective_start_ts")
)

display(
    gold_dim_customer_scd2
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("scd_versions"),
        F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_versions")
    )
    .orderBy("customer_id")
)

# COMMAND ----------

# DBTITLE 1,Current customer dimension view
gold_dim_customer_current = (
    gold_dim_customer_scd2
    .filter(F.col("is_current") == True)
)

display(
    gold_dim_customer_current
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "city",
        "state",
        "customer_segment",
        "loyalty_tier",
        "effective_start_ts"
    )
    .orderBy("customer_id")
)

# COMMAND ----------

# DBTITLE 1,Product dimension
silver_products = read_silver("silver_products_clean")

product_attributes = [
    "product_name",
    "category",
    "unit_price",
    "is_active"
]

gold_dim_product = (
    silver_products
    .select(
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "ingestion_batch_id",
        "source_file_path",
        "raw_record_hash",
        "silver_processed_ts"
    )
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_sk",
        F.sha2(F.col("product_id").cast("string"), 256)
    )
    .withColumn("record_hash", build_hash(product_attributes))
    .withColumn("gold_processed_ts", F.current_timestamp())
    .select(
        "product_sk",
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "is_active",
        "record_hash",
        "ingestion_batch_id",
        "source_file_path",
        "raw_record_hash",
        "silver_processed_ts",
        "gold_processed_ts"
    )
)

write_gold(gold_dim_product, "gold_dim_product")

display(gold_dim_product.orderBy("product_id"))

# COMMAND ----------

# DBTITLE 1,Gold summary
gold_tables = {
    "gold_dim_customer_scd2": f"{gold_base_path}/gold_dim_customer_scd2",
    "gold_dim_product": f"{gold_base_path}/gold_dim_product"
}

summary_rows = []

for table_name, table_path in gold_tables.items():
    df = spark.read.format("delta").load(table_path)
    summary_rows.append({
        "table_name": table_name,
        "row_count": df.count(),
        "path": table_path
    })

gold_summary_df = spark.createDataFrame(summary_rows)

display(gold_summary_df.orderBy("table_name"))

# COMMAND ----------

# DBTITLE 1,Gold Delta history
for table_name, table_path in gold_tables.items():
    print(f"\nDelta history for {table_name}")

    delta_table = DeltaTable.forPath(spark, table_path)

    display(delta_table.history())
