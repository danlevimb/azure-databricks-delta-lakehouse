# Databricks notebook source
# DBTITLE 1,Base config

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

storage_account = "stdanadblh4827"

silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta_lakehouse"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_lakehouse"

print("Gold facts and aggregates configuration loaded")
print(f"Silver path: {silver_base_path}")
print(f"Gold path: {gold_base_path}")

# COMMAND ----------

# DBTITLE 1,Helpers
def read_silver(table_name: str):
    return spark.read.format("delta").load(f"{silver_base_path}/{table_name}")


def read_gold(table_name: str):
    return spark.read.format("delta").load(f"{gold_base_path}/{table_name}")


def write_gold(df, table_name: str, partition_columns=None):
    target_path = f"{gold_base_path}/{table_name}"

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    writer.save(target_path)

    print(f"Gold Delta written: {target_path}")


def add_batch_sequence(df):
    return df.withColumn(
        "batch_sequence",
        F.regexp_extract(F.col("ingestion_batch_id"), r"batch_(\d+)", 1).cast("int")
    )

#%%

# COMMAND ----------

# DBTITLE 1,Silver/Gold Read
silver_orders = read_silver("silver_orders_clean")
silver_order_items = read_silver("silver_order_items_clean")

gold_dim_customer_scd2 = read_gold("gold_dim_customer_scd2")
gold_dim_product = read_gold("gold_dim_product")

print("Input tables loaded")

print(f"silver_orders: {silver_orders.count()}")
print(f"silver_order_items: {silver_order_items.count()}")
print(f"gold_dim_customer_scd2: {gold_dim_customer_scd2.count()}")
print(f"gold_dim_product: {gold_dim_product.count()}")

#%%

# COMMAND ----------

# DBTITLE 1,Get latest order state
orders_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("batch_sequence").desc(),
        F.col("ingestion_ts").desc()
    )
)

latest_orders = (
    add_batch_sequence(silver_orders)
    .withColumn("rn", F.row_number().over(orders_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

display(
    latest_orders
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_ts",
        "order_date",
        "currency_code",
        "payment_method",
        "source_system",
        "ingestion_batch_id",
        "batch_sequence"
    )
    .orderBy("order_id")
)

#%%

# COMMAND ----------

# DBTITLE 1,Get latest item state
items_window = (
    Window
    .partitionBy("order_id", "product_id")
    .orderBy(
        F.col("batch_sequence").desc(),
        F.col("ingestion_ts").desc()
    )
)

latest_order_items = (
    add_batch_sequence(silver_order_items)
    .withColumn("rn", F.row_number().over(items_window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

display(
    latest_order_items
    .select(
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "line_total",
        "ingestion_batch_id",
        "batch_sequence"
    )
    .orderBy("order_id", "product_id")
)

#%%

# COMMAND ----------

# DBTITLE 1,Add order items
order_item_agg = (
    latest_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("order_line_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum(F.col("quantity") * F.col("unit_price")).cast("decimal(18,2)").alias("gross_order_amount"),
        F.sum("discount_amount").cast("decimal(18,2)").alias("total_discount_amount"),
        F.sum("line_total").cast("decimal(18,2)").alias("net_order_amount")
    )
)

display(order_item_agg.orderBy("order_id"))

#%%

# COMMAND ----------

# DBTITLE 1,Build gold_fact_orders
fact_orders_base = (
    latest_orders.alias("o")
    .join(
        order_item_agg.alias("i"),
        F.col("o.order_id") == F.col("i.order_id"),
        "left"
    )
    .join(
        gold_dim_customer_scd2.alias("c"),
        (
            (F.col("o.customer_id") == F.col("c.customer_id")) &
            (F.col("o.order_ts") >= F.col("c.effective_start_ts")) &
            (
                F.col("c.effective_end_ts").isNull() |
                (F.col("o.order_ts") < F.col("c.effective_end_ts"))
            )
        ),
        "left"
    )
)

gold_fact_orders = (
    fact_orders_base
    .withColumn(
        "is_revenue_order",
        F.col("o.order_status").isin("PAID", "COMPLETED")
    )
    .withColumn(
        "recognized_revenue_amount",
        F.when(
            F.col("is_revenue_order"),
            F.coalesce(F.col("i.net_order_amount"), F.lit(0).cast("decimal(18,2)"))
        ).otherwise(F.lit(0).cast("decimal(18,2)"))
    )
    .withColumn("gold_processed_ts", F.current_timestamp())
    .select(
        F.col("o.order_id"),
        F.col("c.customer_sk"),
        F.col("o.customer_id"),
        F.col("o.order_status"),
        F.col("o.order_ts"),
        F.col("o.order_date"),
        F.col("o.currency_code"),
        F.col("o.payment_method"),
        F.col("o.source_system"),
        F.coalesce(F.col("i.order_line_count"), F.lit(0)).alias("order_line_count"),
        F.coalesce(F.col("i.total_quantity"), F.lit(0)).alias("total_quantity"),
        F.coalesce(F.col("i.gross_order_amount"), F.lit(0).cast("decimal(18,2)")).alias("gross_order_amount"),
        F.coalesce(F.col("i.total_discount_amount"), F.lit(0).cast("decimal(18,2)")).alias("total_discount_amount"),
        F.coalesce(F.col("i.net_order_amount"), F.lit(0).cast("decimal(18,2)")).alias("net_order_amount"),
        F.col("is_revenue_order"),
        F.col("recognized_revenue_amount"),
        F.col("o.ingestion_batch_id"),
        F.col("o.source_file_path"),
        F.col("o.raw_record_hash"),
        F.col("o.silver_processed_ts"),
        F.col("gold_processed_ts")
    )
)

write_gold(
    gold_fact_orders,
    "gold_fact_orders",
    partition_columns=["order_date"]
)

display(
    gold_fact_orders
    .orderBy("order_date", "order_id")
)

#%%

# COMMAND ----------

# DBTITLE 1,Validate Fact duplicates
display(
    gold_fact_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

#%%

# COMMAND ----------

# DBTITLE 1,Validate fact -customer SCD2
display(
    gold_fact_orders.alias("f")
    .join(
        gold_dim_customer_scd2.alias("c"),
        F.col("f.customer_sk") == F.col("c.customer_sk"),
        "left"
    )
    .select(
        F.col("f.order_id"),
        F.col("f.order_ts"),
        F.col("f.customer_id"),
        F.col("c.customer_name"),
        F.col("c.city"),
        F.col("c.customer_segment"),
        F.col("c.effective_start_ts"),
        F.col("c.effective_end_ts"),
        F.col("c.is_current"),
        F.col("f.order_status"),
        F.col("f.net_order_amount"),
        F.col("f.recognized_revenue_amount")
    )
    .orderBy("f.order_id")
)

#%%

# COMMAND ----------

# DBTITLE 1,Build gold_daily_sales_summary
gold_daily_sales_summary = (
    gold_fact_orders
    .groupBy("order_date", "currency_code")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum(F.when(F.col("order_status") == "CREATED", 1).otherwise(0)).alias("created_orders"),
        F.sum(F.when(F.col("order_status") == "PAID", 1).otherwise(0)).alias("paid_orders"),
        F.sum(F.when(F.col("order_status") == "COMPLETED", 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("is_revenue_order"), 1).otherwise(0)).alias("revenue_orders"),
        F.sum("total_quantity").alias("total_quantity"),
        F.sum("gross_order_amount").cast("decimal(18,2)").alias("gross_sales_amount"),
        F.sum("total_discount_amount").cast("decimal(18,2)").alias("total_discount_amount"),
        F.sum("net_order_amount").cast("decimal(18,2)").alias("net_order_amount"),
        F.sum("recognized_revenue_amount").cast("decimal(18,2)").alias("recognized_revenue_amount")
    )
    .withColumn("gold_processed_ts", F.current_timestamp())
)

write_gold(
    gold_daily_sales_summary,
    "gold_daily_sales_summary",
    partition_columns=["order_date"]
)

display(
    gold_daily_sales_summary
    .orderBy("order_date", "currency_code")
)

#%%

# COMMAND ----------

# DBTITLE 1,Build gold_customer_sales_summary
gold_customer_sales_summary = (
    gold_fact_orders.alias("f")
    .join(
        gold_dim_customer_scd2.alias("c"),
        F.col("f.customer_sk") == F.col("c.customer_sk"),
        "left"
    )
    .groupBy(
        F.col("f.customer_sk"),
        F.col("f.customer_id"),
        F.col("c.customer_name"),
        F.col("c.city"),
        F.col("c.state"),
        F.col("c.customer_segment"),
        F.col("c.loyalty_tier"),
        F.col("f.currency_code")
    )
    .agg(
        F.countDistinct("f.order_id").alias("total_orders"),
        F.sum(F.when(F.col("f.is_revenue_order"), 1).otherwise(0)).alias("revenue_orders"),
        F.sum("f.total_quantity").alias("total_quantity"),
        F.sum("f.gross_order_amount").cast("decimal(18,2)").alias("gross_sales_amount"),
        F.sum("f.total_discount_amount").cast("decimal(18,2)").alias("total_discount_amount"),
        F.sum("f.net_order_amount").cast("decimal(18,2)").alias("net_order_amount"),
        F.sum("f.recognized_revenue_amount").cast("decimal(18,2)").alias("recognized_revenue_amount"),
        F.min("f.order_date").alias("first_order_date"),
        F.max("f.order_date").alias("last_order_date")
    )
    .withColumn("gold_processed_ts", F.current_timestamp())
)

write_gold(
    gold_customer_sales_summary,
    "gold_customer_sales_summary"
)

display(
    gold_customer_sales_summary
    .orderBy("customer_id", "first_order_date")
)

#%%

# COMMAND ----------

# DBTITLE 1,Final Gold-summary
gold_tables = {
    "gold_dim_customer_scd2": f"{gold_base_path}/gold_dim_customer_scd2",
    "gold_dim_product": f"{gold_base_path}/gold_dim_product",
    "gold_fact_orders": f"{gold_base_path}/gold_fact_orders",
    "gold_daily_sales_summary": f"{gold_base_path}/gold_daily_sales_summary",
    "gold_customer_sales_summary": f"{gold_base_path}/gold_customer_sales_summary"
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

#%%

# COMMAND ----------

# DBTITLE 1,Gold - Delta history
for table_name, table_path in gold_tables.items():
    print(f"\nDelta history for {table_name}")

    delta_table = DeltaTable.forPath(spark, table_path)

    display(delta_table.history())
