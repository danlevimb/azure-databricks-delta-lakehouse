# Databricks notebook source
#%%

# DBTITLE 1,Base config
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta_lakehouse"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/delta_lakehouse"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/delta_lakehouse"
metadata_base_path = f"abfss://metadata@{storage_account}.dfs.core.windows.net/delta_lakehouse"

validation_run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

print("Data quality validation notebook loaded")
print(f"Validation run id: {validation_run_id}")

#%%
# COMMAND ----------

# DBTITLE 1,Helpers
validation_results = []

def read_delta(path: str):
    return spark.read.format("delta").load(path)


def add_validation_result(
    validation_name: str,
    layer_name: str,
    expected_value,
    actual_value,
    status: str,
    details: str
):
    validation_results.append({
        "validation_run_id": validation_run_id,
        "validation_name": validation_name,
        "layer_name": layer_name,
        "expected_value": str(expected_value),
        "actual_value": str(actual_value),
        "status": status,
        "details": details,
        "validated_at": datetime.utcnow().isoformat()
    })


def pass_fail(condition: bool):
    return "PASS" if condition else "FAIL"


def table_count(path: str):
    return read_delta(path).count()

#%%
# COMMAND ----------

# DBTITLE 1,Project - Table definitions
tables = {
    "bronze_customers": f"{bronze_base_path}/bronze_customers",
    "bronze_products": f"{bronze_base_path}/bronze_products",
    "bronze_orders": f"{bronze_base_path}/bronze_orders",
    "bronze_order_items": f"{bronze_base_path}/bronze_order_items",

    "silver_customers_clean": f"{silver_base_path}/silver_customers_clean",
    "silver_products_clean": f"{silver_base_path}/silver_products_clean",
    "silver_orders_clean": f"{silver_base_path}/silver_orders_clean",
    "silver_order_items_clean": f"{silver_base_path}/silver_order_items_clean",
    "silver_rejected_records": f"{silver_base_path}/silver_rejected_records",

    "gold_dim_customer_scd2": f"{gold_base_path}/gold_dim_customer_scd2",
    "gold_dim_product": f"{gold_base_path}/gold_dim_product",
    "gold_fact_orders": f"{gold_base_path}/gold_fact_orders",
    "gold_daily_sales_summary": f"{gold_base_path}/gold_daily_sales_summary",
    "gold_customer_sales_summary": f"{gold_base_path}/gold_customer_sales_summary"
}

print("Project Delta tables configured")

#%%
# COMMAND ----------

# DBTITLE 1,Base count - Validation
expected_counts = {
    "bronze_customers": 9,
    "bronze_products": 4,
    "bronze_orders": 10,
    "bronze_order_items": 11,

    "silver_customers_clean": 9,
    "silver_products_clean": 4,
    "silver_orders_clean": 7,
    "silver_order_items_clean": 8,
    "silver_rejected_records": 6,

    "gold_dim_customer_scd2": 9,
    "gold_dim_product": 5,
    "gold_fact_orders": 6,
    "gold_daily_sales_summary": 4,
    "gold_customer_sales_summary": 6
}

count_rows = []

for table_name, expected_count in expected_counts.items():
    actual_count = table_count(tables[table_name])
    status = pass_fail(actual_count == expected_count)

    add_validation_result(
        validation_name=f"{table_name}_row_count",
        layer_name=table_name.split("_")[0],
        expected_value=expected_count,
        actual_value=actual_count,
        status=status,
        details=f"Validate expected row count for {table_name}"
    )

    count_rows.append({
        "table_name": table_name,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "status": status
    })

row_count_validation_df = spark.createDataFrame(count_rows)

display(row_count_validation_df.orderBy("table_name"))

#%%
# COMMAND ----------

# DBTITLE 1,Rejected records - Validation
rejected_records = read_delta(tables["silver_rejected_records"])

rejected_summary = (
    rejected_records
    .groupBy("entity_name", "reject_reason")
    .count()
    .orderBy("entity_name", "reject_reason")
)

display(rejected_summary)

expected_rejections = {
    ("orders", "currency_code_is_not_allowed"): 1,
    ("orders", "customer_id_not_found"): 1,
    ("orders", "order_status_is_not_allowed"): 1,
    ("order_items", "order_id_not_found_or_parent_order_rejected"): 3
}

actual_rejections = {
    (row["entity_name"], row["reject_reason"]): row["count"]
    for row in rejected_summary.collect()
}

rejection_rows = []

for rejection_key, expected_count in expected_rejections.items():
    actual_count = actual_rejections.get(rejection_key, 0)
    status = pass_fail(actual_count == expected_count)

    entity_name, reject_reason = rejection_key

    add_validation_result(
        validation_name=f"rejection_{entity_name}_{reject_reason}",
        layer_name="silver",
        expected_value=expected_count,
        actual_value=actual_count,
        status=status,
        details=f"Validate rejected records for {entity_name}: {reject_reason}"
    )

    rejection_rows.append({
        "entity_name": entity_name,
        "reject_reason": reject_reason,
        "expected_count": expected_count,
        "actual_count": actual_count,
        "status": status
    })

rejection_validation_df = spark.createDataFrame(rejection_rows)

display(rejection_validation_df.orderBy("entity_name", "reject_reason"))

#%%
# COMMAND ----------

# DBTITLE 1,Gold Fact - No duplicates
gold_fact_orders = read_delta(tables["gold_fact_orders"])

duplicate_orders = (
    gold_fact_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_order_count = duplicate_orders.count()

add_validation_result(
    validation_name="gold_fact_orders_no_duplicate_order_id",
    layer_name="gold",
    expected_value=0,
    actual_value=duplicate_order_count,
    status=pass_fail(duplicate_order_count == 0),
    details="Validate that gold_fact_orders has one row per order_id"
)

display(duplicate_orders)

#%%
# COMMAND ----------

# DBTITLE 1,SCD2 - One version per customer - Validation
gold_dim_customer_scd2 = read_delta(tables["gold_dim_customer_scd2"])

scd2_current_validation = (
    gold_dim_customer_scd2
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("scd_versions"),
        F.sum(F.when(F.col("is_current") == True, 1).otherwise(0)).alias("current_versions")
    )
    .withColumn(
        "status",
        F.when(F.col("current_versions") == 1, F.lit("PASS")).otherwise(F.lit("FAIL"))
    )
)

display(scd2_current_validation.orderBy("customer_id"))

invalid_current_count = (
    scd2_current_validation
    .filter(F.col("current_versions") != 1)
    .count()
)

add_validation_result(
    validation_name="gold_dim_customer_scd2_one_current_record_per_customer",
    layer_name="gold",
    expected_value=0,
    actual_value=invalid_current_count,
    status=pass_fail(invalid_current_count == 0),
    details="Validate each customer has exactly one current SCD2 record"
)

#%%
# COMMAND ----------

# DBTITLE 1,SCD2 - Historic dates - Validation
invalid_scd2_dates = (
    gold_dim_customer_scd2
    .filter(
        F.col("effective_end_ts").isNotNull() &
        (F.col("effective_end_ts") <= F.col("effective_start_ts"))
    )
)

invalid_scd2_date_count = invalid_scd2_dates.count()

add_validation_result(
    validation_name="gold_dim_customer_scd2_valid_effective_dates",
    layer_name="gold",
    expected_value=0,
    actual_value=invalid_scd2_date_count,
    status=pass_fail(invalid_scd2_date_count == 0),
    details="Validate SCD2 effective_end_ts is greater than effective_start_ts"
)

display(invalid_scd2_dates)

#%%
# COMMAND ----------

# DBTITLE 1,Duplicate products (after MERGE) - Validation
gold_dim_product = read_delta(tables["gold_dim_product"])

duplicate_products = (
    gold_dim_product
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_product_count = duplicate_products.count()

add_validation_result(
    validation_name="gold_dim_product_no_duplicate_product_id",
    layer_name="gold",
    expected_value=0,
    actual_value=duplicate_product_count,
    status=pass_fail(duplicate_product_count == 0),
    details="Validate product MERGE did not create duplicate product_id values"
)

display(duplicate_products)

#%%
# COMMAND ----------

# DBTITLE 1,PROD 2 & 5 before MERGE - Validation
merge_product_validation = (
    gold_dim_product
    .filter(F.col("product_id").isin("PROD-002", "PROD-005"))
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

display(merge_product_validation)

prod_002_price = (
    gold_dim_product
    .filter(F.col("product_id") == "PROD-002")
    .select("unit_price")
    .collect()[0]["unit_price"]
)

prod_005_count = (
    gold_dim_product
    .filter(F.col("product_id") == "PROD-005")
    .count()
)

add_validation_result(
    validation_name="delta_merge_updated_existing_product",
    layer_name="gold",
    expected_value="38.00",
    actual_value=prod_002_price,
    status=pass_fail(float(prod_002_price) == 38.0),
    details="Validate PROD-002 was updated by Delta MERGE"
)

add_validation_result(
    validation_name="delta_merge_inserted_new_product",
    layer_name="gold",
    expected_value=1,
    actual_value=prod_005_count,
    status=pass_fail(prod_005_count == 1),
    details="Validate PROD-005 was inserted by Delta MERGE"
)

#%%
# COMMAND ----------

# DBTITLE 1,Fact orders vs Customer SCD2 - Validation
fact_orders_missing_customer_sk = (
    gold_fact_orders
    .filter(F.col("customer_sk").isNull())
)

missing_customer_sk_count = fact_orders_missing_customer_sk.count()

add_validation_result(
    validation_name="gold_fact_orders_customer_sk_not_null",
    layer_name="gold",
    expected_value=0,
    actual_value=missing_customer_sk_count,
    status=pass_fail(missing_customer_sk_count == 0),
    details="Validate every fact order has a resolved customer surrogate key"
)

display(fact_orders_missing_customer_sk)

#%%
# COMMAND ----------

# DBTITLE 1,Consistency Revenue vs Fact - Validation
gold_daily_sales_summary = read_delta(tables["gold_daily_sales_summary"])

fact_revenue_total = (
    gold_fact_orders
    .agg(F.sum("recognized_revenue_amount").alias("recognized_revenue_amount"))
    .collect()[0]["recognized_revenue_amount"]
)

daily_revenue_total = (
    gold_daily_sales_summary
    .agg(F.sum("recognized_revenue_amount").alias("recognized_revenue_amount"))
    .collect()[0]["recognized_revenue_amount"]
)

add_validation_result(
    validation_name="gold_daily_summary_revenue_matches_fact_orders",
    layer_name="gold",
    expected_value=fact_revenue_total,
    actual_value=daily_revenue_total,
    status=pass_fail(float(fact_revenue_total) == float(daily_revenue_total)),
    details="Validate recognized revenue total matches between fact and daily summary"
)

revenue_validation_df = spark.createDataFrame([
    {
        "metric_name": "recognized_revenue_amount",
        "fact_orders_total": str(fact_revenue_total),
        "daily_summary_total": str(daily_revenue_total),
        "status": pass_fail(float(fact_revenue_total) == float(daily_revenue_total))
    }
])

display(revenue_validation_df)

#%%
# COMMAND ----------

# DBTITLE 1,Delta history contains MERGE - Validation
product_delta_table = DeltaTable.forPath(spark, tables["gold_dim_product"])

merge_history_count = (
    product_delta_table
    .history()
    .filter(F.col("operation") == "MERGE")
    .count()
)

add_validation_result(
    validation_name="gold_dim_product_delta_history_contains_merge",
    layer_name="gold",
    expected_value=">= 1",
    actual_value=merge_history_count,
    status=pass_fail(merge_history_count >= 1),
    details="Validate Delta history includes at least one MERGE operation"
)

display(
    product_delta_table
    .history()
    .filter(F.col("operation") == "MERGE")
    .select("version", "timestamp", "operation", "operationMetrics")
    .orderBy(F.col("version").desc())
)

#%%
# COMMAND ----------

# DBTITLE 1,Consolidate Report Consolidation
validation_report_df = spark.createDataFrame(validation_results)

display(
    validation_report_df
    .select(
        "validation_name",
        "layer_name",
        "expected_value",
        "actual_value",
        "status",
        "details"
    )
    .orderBy("layer_name", "validation_name")
)

#%%
# COMMAND ----------

# DBTITLE 1,PASS / FAIL Summary
validation_status_summary = (
    validation_report_df
    .groupBy("status")
    .count()
    .orderBy("status")
)

display(validation_status_summary)

#%%
# COMMAND ----------

# DBTITLE 1,Validation Report - Metadata save
validation_report_path = f"{metadata_base_path}/validation_reports/run_id={validation_run_id}"

(
    validation_report_df
    .write
    .format("delta")
    .mode("overwrite")
    .save(validation_report_path)
)

print(f"Validation report written to: {validation_report_path}")

#%%
# COMMAND ----------

# DBTITLE 1,Get - Validation report
display(
    spark.read
    .format("delta")
    .load(validation_report_path)
    .select(
        "validation_run_id",
        "validation_name",
        "layer_name",
        "expected_value",
        "actual_value",
        "status",
        "details",
        "validated_at"
    )
    .orderBy("layer_name", "validation_name")
)