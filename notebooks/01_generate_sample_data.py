# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Row
from datetime import datetime

storage_account = "stdanadblh4827"

landing_base_path = f"abfss://landing@{storage_account}.dfs.core.windows.net"
source_base_path = f"{landing_base_path}/source_data"

print("Project: azure-databricks-delta-lakehouse")
print(f"Landing base path: {landing_base_path}")
print(f"Source base path: {source_base_path}")

# %%

# COMMAND ----------

def write_single_csv(df, target_file_path: str):
    """
    Writes a Spark DataFrame as a single CSV file with a controlled file name.
    Spark writes to a temporary folder first, then the part file is renamed.
    """
    temp_path = target_file_path + "_tmp"

    dbutils.fs.rm(temp_path, recurse=True)
    dbutils.fs.rm(target_file_path, recurse=False)

    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(temp_path)
    )

    part_file = [
        file.path for file in dbutils.fs.ls(temp_path)
        if file.name.startswith("part-") and file.name.endswith(".csv")
    ][0]

    dbutils.fs.mv(part_file, target_file_path)
    dbutils.fs.rm(temp_path, recurse=True)

    print(f"Written: {target_file_path}")

#%%

# COMMAND ----------

dbutils.fs.rm(source_base_path, recurse=True)
print(f"Cleaned source path: {source_base_path}")

#%%

# COMMAND ----------

batch_001_path = f"{source_base_path}/batch_001"

customers_001 = spark.createDataFrame([
    Row(customer_id="CUST-001", customer_name="Northwind Cafe", email="contact@northwind.example", city="Saltillo", state="Coahuila", customer_segment="SMB", effective_update_ts="2026-05-01 08:00:00"),
    Row(customer_id="CUST-002", customer_name="Blue Mountain Restaurant", email="ops@bluemountain.example", city="Monterrey", state="Nuevo Leon", customer_segment="SMB", effective_update_ts="2026-05-01 08:05:00"),
    Row(customer_id="CUST-003", customer_name="Urban Coffee Lab", email="hello@urbancoffee.example", city="Torreon", state="Coahuila", customer_segment="Enterprise", effective_update_ts="2026-05-01 08:10:00"),
    Row(customer_id="CUST-004", customer_name="Fresh Market Express", email="admin@freshmarket.example", city="Guadalajara", state="Jalisco", customer_segment="SMB", effective_update_ts="2026-05-01 08:15:00"),
])

products_001 = spark.createDataFrame([
    Row(product_id="PROD-001", product_name="Purified Water 20L", category="Water", unit_price=48.00, is_active=True),
    Row(product_id="PROD-002", product_name="Ice Bag 5kg", category="Ice", unit_price=35.00, is_active=True),
    Row(product_id="PROD-003", product_name="Water Bottle 600ml", category="Water", unit_price=12.50, is_active=True),
    Row(product_id="PROD-004", product_name="Premium Mineral Water 1L", category="Water", unit_price=22.00, is_active=True),
])

orders_001 = spark.createDataFrame([
    Row(order_id="ORD-1001", customer_id="CUST-001", order_status="CREATED", order_ts="2026-05-01 09:00:00", currency_code="MXN", payment_method="CARD", source_system="pos"),
    Row(order_id="ORD-1002", customer_id="CUST-002", order_status="PAID", order_ts="2026-05-01 09:15:00", currency_code="MXN", payment_method="TRANSFER", source_system="web"),
    Row(order_id="ORD-1003", customer_id="CUST-003", order_status="PAID", order_ts="2026-05-01 10:00:00", currency_code="USD", payment_method="CARD", source_system="pos"),
    Row(order_id="ORD-1004", customer_id="CUST-004", order_status="CANCELLED", order_ts="2026-05-01 10:30:00", currency_code="MXN", payment_method="CASH", source_system="pos"),
])

order_items_001 = spark.createDataFrame([
    Row(order_id="ORD-1001", product_id="PROD-001", quantity=3, unit_price=48.00, discount_amount=0.00),
    Row(order_id="ORD-1001", product_id="PROD-002", quantity=2, unit_price=35.00, discount_amount=5.00),
    Row(order_id="ORD-1002", product_id="PROD-001", quantity=10, unit_price=48.00, discount_amount=20.00),
    Row(order_id="ORD-1003", product_id="PROD-004", quantity=4, unit_price=22.00, discount_amount=0.00),
    Row(order_id="ORD-1004", product_id="PROD-003", quantity=12, unit_price=12.50, discount_amount=0.00),
])

write_single_csv(customers_001, f"{batch_001_path}/customers.csv")
write_single_csv(products_001, f"{batch_001_path}/products.csv")
write_single_csv(orders_001, f"{batch_001_path}/orders.csv")
write_single_csv(order_items_001, f"{batch_001_path}/order_items.csv")

#%%

# COMMAND ----------

batch_002_path = f"{source_base_path}/batch_002"

customers_002 = spark.createDataFrame([
    Row(customer_id="CUST-002", customer_name="Blue Mountain Restaurant", email="ops@bluemountain.example", city="San Pedro Garza Garcia", state="Nuevo Leon", customer_segment="Enterprise", effective_update_ts="2026-05-02 08:30:00"),
    Row(customer_id="CUST-005", customer_name="Central Bakery Group", email="contact@centralbakery.example", city="Saltillo", state="Coahuila", customer_segment="SMB", effective_update_ts="2026-05-02 08:40:00"),
])

orders_002 = spark.createDataFrame([
    Row(order_id="ORD-1002", customer_id="CUST-002", order_status="COMPLETED", order_ts="2026-05-01 09:15:00", currency_code="MXN", payment_method="TRANSFER", source_system="web"),
    Row(order_id="ORD-1005", customer_id="CUST-005", order_status="PAID", order_ts="2026-05-02 11:00:00", currency_code="MXN", payment_method="CARD", source_system="web"),
    Row(order_id="ORD-1006", customer_id="CUST-999", order_status="PAID", order_ts="2026-05-02 11:30:00", currency_code="MXN", payment_method="CARD", source_system="web"),
])

order_items_002 = spark.createDataFrame([
    Row(order_id="ORD-1002", product_id="PROD-001", quantity=12, unit_price=48.00, discount_amount=20.00),
    Row(order_id="ORD-1005", product_id="PROD-002", quantity=6, unit_price=35.00, discount_amount=0.00),
    Row(order_id="ORD-1006", product_id="PROD-001", quantity=-1, unit_price=48.00, discount_amount=0.00),
])

write_single_csv(customers_002, f"{batch_002_path}/customers.csv")
write_single_csv(orders_002, f"{batch_002_path}/orders.csv")
write_single_csv(order_items_002, f"{batch_002_path}/order_items.csv")

#%%

# COMMAND ----------

batch_003_path = f"{source_base_path}/batch_003_schema_evolution"

customers_003 = spark.createDataFrame([
    Row(customer_id="CUST-001", customer_name="Northwind Cafe", email="newcontact@northwind.example", city="Saltillo", state="Coahuila", customer_segment="SMB", effective_update_ts="2026-05-03 08:00:00", loyalty_tier="Gold"),
    Row(customer_id="CUST-003", customer_name="Urban Coffee Lab", email="hello@urbancoffee.example", city="Torreon", state="Coahuila", customer_segment="Enterprise", effective_update_ts="2026-05-03 08:10:00", loyalty_tier="Platinum"),
    Row(customer_id="CUST-006", customer_name="Airport Bistro", email="admin@airportbistro.example", city="Monterrey", state="Nuevo Leon", customer_segment="SMB", effective_update_ts="2026-05-03 08:20:00", loyalty_tier="Silver"),
])

orders_003 = spark.createDataFrame([
    Row(order_id="ORD-1007", customer_id="CUST-001", order_status="PAID", order_ts="2026-05-03 12:00:00", currency_code="MXN", payment_method="CARD", source_system="mobile"),
    Row(order_id="ORD-1008", customer_id="CUST-003", order_status="CREATED", order_ts="2026-05-03 12:30:00", currency_code="EUR", payment_method="CARD", source_system="web"),
    Row(order_id="ORD-1009", customer_id="CUST-006", order_status="INVALID_STATUS", order_ts="2026-05-03 13:00:00", currency_code="MXN", payment_method="CARD", source_system="web"),
])

order_items_003 = spark.createDataFrame([
    Row(order_id="ORD-1007", product_id="PROD-001", quantity=5, unit_price=48.00, discount_amount=0.00),
    Row(order_id="ORD-1008", product_id="PROD-004", quantity=3, unit_price=22.00, discount_amount=0.00),
    Row(order_id="ORD-1009", product_id="PROD-002", quantity=2, unit_price=35.00, discount_amount=0.00),
])

write_single_csv(customers_003, f"{batch_003_path}/customers.csv")
write_single_csv(orders_003, f"{batch_003_path}/orders.csv")
write_single_csv(order_items_003, f"{batch_003_path}/order_items.csv")

#%%

# COMMAND ----------

for batch_dir in dbutils.fs.ls(source_base_path):
    print(f"\n{batch_dir.path}")
    for file in dbutils.fs.ls(batch_dir.path):
        print(f"  - {file.name}")
#%%

# COMMAND ----------

sample_customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{source_base_path}/batch_001/customers.csv")
)

display(sample_customers)
