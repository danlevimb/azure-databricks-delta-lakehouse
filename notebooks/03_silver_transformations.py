# Databricks notebook source
# DBTITLE 1,Base config
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from functools import reduce

storage_account = "stdanadblh4827"

bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta_lakehouse"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta_lakehouse"

print("Silver transformation configuration loaded")
print(f"Bronze path: {bronze_base_path}")
print(f"Silver path: {silver_base_path}")

#%%

# COMMAND ----------

# DBTITLE 1,Helpers
def read_bronze(entity_name: str):
    path = f"{bronze_base_path}/bronze_{entity_name}"
    return spark.read.format("delta").load(path)

def write_silver(df, table_name: str):
    target_path = f"{silver_base_path}/{table_name}"

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Silver Delta written: {target_path}")

def clean_string(column_name: str):
    return (
        F.when(F.trim(F.col(column_name).cast("string")) == "", F.lit(None))
        .otherwise(F.trim(F.col(column_name).cast("string")))
    )

def add_missing_column(df, column_name: str, data_type: str):
    if column_name not in df.columns:
        return df.withColumn(column_name, F.lit(None).cast(data_type))
    return df

def union_by_name_allow_missing(dataframes):
    if not dataframes:
        return None

    result = dataframes[0]

    for df in dataframes[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    return result

def build_rejected_records(df, entity_name: str):
    """
    Creates a normalized rejected-records dataframe from any entity dataframe
    that contains a reject_reason column.
    """
    record_columns = [column_name for column_name in df.columns if column_name != "reject_reason"]

    return (
        df
        .filter(F.col("reject_reason").isNotNull())
        .select(
            F.lit(entity_name).alias("entity_name"),
            F.col("reject_reason"),
            F.col("ingestion_batch_id"),
            F.col("source_file_path"),
            F.col("raw_record_hash"),
            F.current_timestamp().alias("rejected_at"),
            F.to_json(F.struct(*[F.col(column_name) for column_name in record_columns])).alias("record_json")
        )
    )

#%%

# COMMAND ----------

# DBTITLE 1,Clean previous Silver
dbutils.fs.rm(silver_base_path, recurse=True)
print(f"Cleaned Silver project path: {silver_base_path}")
#%%

# COMMAND ----------

# DBTITLE 1,Customers Silver
bronze_customers = read_bronze("customers")
bronze_customers = add_missing_column(bronze_customers, "loyalty_tier", "string")

customers_typed = (
    bronze_customers
    .select(
        clean_string("customer_id").alias("customer_id"),
        clean_string("customer_name").alias("customer_name"),
        clean_string("email").alias("email"),
        clean_string("city").alias("city"),
        clean_string("state").alias("state"),
        clean_string("customer_segment").alias("customer_segment"),
        clean_string("loyalty_tier").alias("loyalty_tier"),
        F.to_timestamp("effective_update_ts").alias("effective_update_ts"),
        F.col("ingestion_batch_id"),
        F.col("source_entity"),
        F.col("source_file_path"),
        F.col("ingestion_ts"),
        F.col("bronze_load_date"),
        F.col("raw_record_hash")
    )
    .withColumn("silver_processed_ts", F.current_timestamp())
)

customers_validated = (
    customers_typed
    .withColumn(
        "reject_reason",
        F.when(F.col("customer_id").isNull(), F.lit("customer_id_is_required"))
         .when(F.col("customer_name").isNull(), F.lit("customer_name_is_required"))
         .when(F.col("effective_update_ts").isNull(), F.lit("effective_update_ts_is_invalid"))
         .otherwise(F.lit(None).cast("string"))
    )
)

silver_customers_clean = customers_validated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected_customers = build_rejected_records(customers_validated, "customers")

write_silver(silver_customers_clean, "silver_customers_clean")

display(silver_customers_clean.orderBy("customer_id", "effective_update_ts"))

#%%

# COMMAND ----------

# DBTITLE 1,Products Silver
bronze_products = read_bronze("products")

products_typed = (
    bronze_products
    .select(
        clean_string("product_id").alias("product_id"),
        clean_string("product_name").alias("product_name"),
        clean_string("category").alias("category"),
        F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.col("is_active").cast("boolean").alias("is_active"),
        F.col("ingestion_batch_id"),
        F.col("source_entity"),
        F.col("source_file_path"),
        F.col("ingestion_ts"),
        F.col("bronze_load_date"),
        F.col("raw_record_hash")
    )
    .withColumn("silver_processed_ts", F.current_timestamp())
)

products_validated = (
    products_typed
    .withColumn(
        "reject_reason",
        F.when(F.col("product_id").isNull(), F.lit("product_id_is_required"))
         .when(F.col("product_name").isNull(), F.lit("product_name_is_required"))
         .when(F.col("unit_price").isNull(), F.lit("unit_price_is_invalid"))
         .when(F.col("unit_price") <= 0, F.lit("unit_price_must_be_positive"))
         .otherwise(F.lit(None).cast("string"))
    )
)

silver_products_clean = products_validated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected_products = build_rejected_records(products_validated, "products")

write_silver(silver_products_clean, "silver_products_clean")

display(silver_products_clean.orderBy("product_id"))

#%%

# COMMAND ----------

# DBTITLE 1,Orders Silver
allowed_order_status = ["CREATED", "PAID", "COMPLETED", "CANCELLED", "REFUNDED"]
allowed_currency_codes = ["MXN", "USD"]

bronze_orders = read_bronze("orders")

orders_typed = (
    bronze_orders
    .select(
        clean_string("order_id").alias("order_id"),
        clean_string("customer_id").alias("customer_id"),
        F.upper(clean_string("order_status")).alias("order_status"),
        F.to_timestamp("order_ts").alias("order_ts"),
        F.upper(clean_string("currency_code")).alias("currency_code"),
        F.upper(clean_string("payment_method")).alias("payment_method"),
        F.lower(clean_string("source_system")).alias("source_system"),
        F.col("ingestion_batch_id"),
        F.col("source_entity"),
        F.col("source_file_path"),
        F.col("ingestion_ts"),
        F.col("bronze_load_date"),
        F.col("raw_record_hash")
    )
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("silver_processed_ts", F.current_timestamp())
)

customer_lookup = (
    silver_customers_clean
    .select(F.col("customer_id").alias("lk_customer_id"))
    .distinct()
)

orders_enriched = (
    orders_typed
    .join(
        customer_lookup,
        orders_typed.customer_id == customer_lookup.lk_customer_id,
        "left"
    )
)

orders_validated = (
    orders_enriched
    .withColumn(
        "reject_reason",
        F.when(F.col("order_id").isNull(), F.lit("order_id_is_required"))
         .when(F.col("customer_id").isNull(), F.lit("customer_id_is_required"))
         .when(F.col("lk_customer_id").isNull(), F.lit("customer_id_not_found"))
         .when(F.col("order_ts").isNull(), F.lit("order_ts_is_invalid"))
         .when(~F.col("order_status").isin(allowed_order_status), F.lit("order_status_is_not_allowed"))
         .when(~F.col("currency_code").isin(allowed_currency_codes), F.lit("currency_code_is_not_allowed"))
         .otherwise(F.lit(None).cast("string"))
    )
    .drop("lk_customer_id")
)

silver_orders_clean = orders_validated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected_orders = build_rejected_records(orders_validated, "orders")

write_silver(silver_orders_clean, "silver_orders_clean")

display(silver_orders_clean.orderBy("order_id", "ingestion_batch_id"))

#%%

# COMMAND ----------

# DBTITLE 1,OrderItems Silver
bronze_order_items = read_bronze("order_items")

order_items_typed = (
    bronze_order_items
    .select(
        clean_string("order_id").alias("order_id"),
        clean_string("product_id").alias("product_id"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("decimal(18,2)").alias("unit_price"),
        F.col("discount_amount").cast("decimal(18,2)").alias("discount_amount"),
        F.col("ingestion_batch_id"),
        F.col("source_entity"),
        F.col("source_file_path"),
        F.col("ingestion_ts"),
        F.col("bronze_load_date"),
        F.col("raw_record_hash")
    )
    .withColumn(
        "line_total",
        (F.col("quantity") * F.col("unit_price")) - F.col("discount_amount")
    )
    .withColumn("silver_processed_ts", F.current_timestamp())
)

order_lookup = (
    silver_orders_clean
    .select(F.col("order_id").alias("lk_order_id"))
    .distinct()
)

product_lookup = (
    silver_products_clean
    .select(F.col("product_id").alias("lk_product_id"))
    .distinct()
)

order_items_enriched = (
    order_items_typed
    .join(
        order_lookup,
        order_items_typed.order_id == order_lookup.lk_order_id,
        "left"
    )
    .join(
        product_lookup,
        order_items_typed.product_id == product_lookup.lk_product_id,
        "left"
    )
)

order_items_validated = (
    order_items_enriched
    .withColumn(
        "reject_reason",
        F.when(F.col("order_id").isNull(), F.lit("order_id_is_required"))
         .when(F.col("lk_order_id").isNull(), F.lit("order_id_not_found_or_parent_order_rejected"))
         .when(F.col("product_id").isNull(), F.lit("product_id_is_required"))
         .when(F.col("lk_product_id").isNull(), F.lit("product_id_not_found"))
         .when(F.col("quantity").isNull(), F.lit("quantity_is_invalid"))
         .when(F.col("quantity") <= 0, F.lit("quantity_must_be_positive"))
         .when(F.col("unit_price").isNull(), F.lit("unit_price_is_invalid"))
         .when(F.col("unit_price") <= 0, F.lit("unit_price_must_be_positive"))
         .when(F.col("discount_amount").isNull(), F.lit("discount_amount_is_invalid"))
         .when(F.col("discount_amount") < 0, F.lit("discount_amount_cannot_be_negative"))
         .when(F.col("line_total") < 0, F.lit("line_total_cannot_be_negative"))
         .otherwise(F.lit(None).cast("string"))
    )
    .drop("lk_order_id", "lk_product_id")
)

silver_order_items_clean = order_items_validated.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected_order_items = build_rejected_records(order_items_validated, "order_items")

write_silver(silver_order_items_clean, "silver_order_items_clean")

display(silver_order_items_clean.orderBy("order_id", "product_id"))

#%%

# COMMAND ----------

# DBTITLE 1,Rejected records
rejected_records = union_by_name_allow_missing([
    rejected_customers,
    rejected_products,
    rejected_orders,
    rejected_order_items
])

write_silver(rejected_records, "silver_rejected_records")

display(
    rejected_records
    .groupBy("entity_name", "reject_reason")
    .count()
    .orderBy("entity_name", "reject_reason")
)

#%%

# COMMAND ----------

# DBTITLE 1,Silver summary
silver_tables = {
    "silver_customers_clean": f"{silver_base_path}/silver_customers_clean",
    "silver_products_clean": f"{silver_base_path}/silver_products_clean",
    "silver_orders_clean": f"{silver_base_path}/silver_orders_clean",
    "silver_order_items_clean": f"{silver_base_path}/silver_order_items_clean",
    "silver_rejected_records": f"{silver_base_path}/silver_rejected_records"
}

summary_rows = []

for table_name, table_path in silver_tables.items():
    df = spark.read.format("delta").load(table_path)
    summary_rows.append({
        "table_name": table_name,
        "row_count": df.count(),
        "path": table_path
    })

silver_summary_df = spark.createDataFrame(summary_rows)

display(silver_summary_df.orderBy("table_name"))

#%%

# COMMAND ----------

# DBTITLE 1,Validators
print("Rejected records by entity and reason")

display(
    spark.read.format("delta")
    .load(f"{silver_base_path}/silver_rejected_records")
    .select("entity_name", "reject_reason", "ingestion_batch_id", "record_json")
    .orderBy("entity_name", "reject_reason")
)

print("Valid Silver orders")

display(
    spark.read.format("delta")
    .load(f"{silver_base_path}/silver_orders_clean")
    .select("order_id", "customer_id", "order_status", "order_ts", "currency_code", "ingestion_batch_id")
    .orderBy("order_id", "ingestion_batch_id")
)

print("Valid Silver order items")

display(
    spark.read.format("delta")
    .load(f"{silver_base_path}/silver_order_items_clean")
    .select("order_id", "product_id", "quantity", "unit_price", "discount_amount", "line_total", "ingestion_batch_id")
    .orderBy("order_id", "product_id")
)

#%%

# COMMAND ----------

# DBTITLE 1,Silver - Delta History
for table_name, table_path in silver_tables.items():
    print(f"\nDelta history for {table_name}")

    delta_table = DeltaTable.forPath(spark, table_path)

    display(delta_table.history())
