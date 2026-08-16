# Databricks notebook source
# MAGIC %md
# MAGIC # E-commerce Incremental Data Engineering Pipeline
# MAGIC **Flow:** Raw CSV → Incremental Bronze Delta → Silver → Data Quality → Gold
# MAGIC
# MAGIC Put CSVs in:
# MAGIC ```text
# MAGIC ecommerce_raw/
# MAGIC ├── customers/
# MAGIC ├── orders/
# MAGIC └── order_items/
# MAGIC ```
# MAGIC Keep old files and add new batch files. The `processed_raw_files` Delta table prevents already processed files from being ingested again.
# MAGIC

# COMMAND ----------

# 1. CONFIGURATION
RAW_PATH = "/Workspace/Users/mohammedaliuddinhyder04@gmail.com/deproject/ecommerce_raw"
CATALOG = "workspace"
SCHEMA = "default"

BRONZE_CUSTOMERS = f"{CATALOG}.{SCHEMA}.bronze_customers"
BRONZE_ORDERS = f"{CATALOG}.{SCHEMA}.bronze_orders"
BRONZE_ITEMS = f"{CATALOG}.{SCHEMA}.bronze_order_items"

SILVER_CUSTOMERS = f"{CATALOG}.{SCHEMA}.silver_customers"
SILVER_ORDERS = f"{CATALOG}.{SCHEMA}.silver_orders"
SILVER_ITEMS = f"{CATALOG}.{SCHEMA}.silver_order_items"

QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.data_quality_results"

GOLD_ORDER_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_order_summary"
GOLD_CUSTOMER_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_customer_summary"

PROCESSED_FILES_TABLE = f"{CATALOG}.{SCHEMA}.processed_raw_files"

print("RAW_PATH:", RAW_PATH)


# COMMAND ----------

# 2. IMPORTS
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, to_date, current_date,
    when, countDistinct, sum as spark_sum, round as spark_round
)
from datetime import datetime

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PROCESSED_FILES_TABLE} (
    file_path STRING,
    dataset STRING,
    processed_at TIMESTAMP
)
USING DELTA
""")

print("Setup complete.")


# COMMAND ----------

# 3. RAW FOLDERS
CUSTOMERS_RAW_PATH = f"{RAW_PATH}/customers"
ORDERS_RAW_PATH = f"{RAW_PATH}/orders"
ITEMS_RAW_PATH = f"{RAW_PATH}/order_items"

def get_csv_files(path):
    return sorted([
        x.path for x in dbutils.fs.ls(path)
        if x.path.lower().endswith(".csv")
    ])

raw_files = {
    "customers": get_csv_files(CUSTOMERS_RAW_PATH),
    "orders": get_csv_files(ORDERS_RAW_PATH),
    "order_items": get_csv_files(ITEMS_RAW_PATH)
}

for dataset, files in raw_files.items():
    print(f"\n{dataset}:")
    if not files:
        raise FileNotFoundError(f"No CSV files found in {dataset}: {raw_files[dataset]}")
    for f in files:
        print(" -", f)


# COMMAND ----------

# 4. FIND ONLY NEW FILES
processed_files = {
    r["file_path"]
    for r in spark.table(PROCESSED_FILES_TABLE).select("file_path").collect()
}

new_customers_files = [f for f in raw_files["customers"] if f not in processed_files]
new_orders_files = [f for f in raw_files["orders"] if f not in processed_files]
new_items_files = [f for f in raw_files["order_items"] if f not in processed_files]

print("New customer files:", new_customers_files)
print("New order files:", new_orders_files)
print("New order-item files:", new_items_files)


# COMMAND ----------

# 5. READ ONLY NEW FILES
def read_new_csv_files(files):
    if not files:
        return None
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv(files)
    )

bronze_customers_new = read_new_csv_files(new_customers_files)
bronze_orders_new = read_new_csv_files(new_orders_files)
bronze_order_items_new = read_new_csv_files(new_items_files)

print("New customers:", 0 if bronze_customers_new is None else bronze_customers_new.count())
print("New orders:", 0 if bronze_orders_new is None else bronze_orders_new.count())
print("New order items:", 0 if bronze_order_items_new is None else bronze_order_items_new.count())


# COMMAND ----------

# 6. DISPLAY NEW DATA
if bronze_customers_new is not None: display(bronze_customers_new)
if bronze_orders_new is not None: display(bronze_orders_new)
if bronze_order_items_new is not None: display(bronze_order_items_new)


# COMMAND ----------

# 7. BRONZE CUSTOMERS — INITIAL LOAD OR MERGE
if bronze_customers_new is not None:
    if not spark.catalog.tableExists(BRONZE_CUSTOMERS):
        (bronze_customers_new.write.format("delta").mode("overwrite").saveAsTable(BRONZE_CUSTOMERS))
        print("Created", BRONZE_CUSTOMERS)
    else:
        target = DeltaTable.forName(spark, BRONZE_CUSTOMERS)
        (target.alias("t")
         .merge(bronze_customers_new.alias("s"), "t.customer_id = s.customer_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print("Merged", BRONZE_CUSTOMERS)
else:
    print("No new customer files.")


# COMMAND ----------

# 8. BRONZE ORDERS — INITIAL LOAD OR MERGE
if bronze_orders_new is not None:
    if not spark.catalog.tableExists(BRONZE_ORDERS):
        (bronze_orders_new.write.format("delta").mode("overwrite").saveAsTable(BRONZE_ORDERS))
        print("Created", BRONZE_ORDERS)
    else:
        target = DeltaTable.forName(spark, BRONZE_ORDERS)
        (target.alias("t")
         .merge(bronze_orders_new.alias("s"), "t.order_id = s.order_id")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print("Merged", BRONZE_ORDERS)
else:
    print("No new order files.")


# COMMAND ----------

# 9. BRONZE ORDER ITEMS — INITIAL LOAD OR MERGE
if bronze_order_items_new is not None:
    if not spark.catalog.tableExists(BRONZE_ITEMS):
        (bronze_order_items_new.write.format("delta").mode("overwrite").saveAsTable(BRONZE_ITEMS))
        print("Created", BRONZE_ITEMS)
    else:
        target = DeltaTable.forName(spark, BRONZE_ITEMS)
        (target.alias("t")
         .merge(
             bronze_order_items_new.alias("s"),
             "t.order_id = s.order_id AND t.product = s.product"
         )
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        print("Merged", BRONZE_ITEMS)
else:
    print("No new order-item files.")


# COMMAND ----------

# 10. MARK FILES PROCESSED — ONLY AFTER BRONZE SUCCEEDS
processed_now = (
    [(f, "customers", datetime.now()) for f in new_customers_files] +
    [(f, "orders", datetime.now()) for f in new_orders_files] +
    [(f, "order_items", datetime.now()) for f in new_items_files]
)

if processed_now:
    (spark.createDataFrame(
        processed_now, ["file_path", "dataset", "processed_at"]
    ).write.format("delta").mode("append").saveAsTable(PROCESSED_FILES_TABLE))
    print("Recorded", len(processed_now), "processed files.")
else:
    print("No new files to record.")


# COMMAND ----------

# 11. COMPLETE BRONZE
bronze_customers = spark.table(BRONZE_CUSTOMERS)
bronze_orders = spark.table(BRONZE_ORDERS)
bronze_order_items = spark.table(BRONZE_ITEMS)

print("Bronze customers:", bronze_customers.count())
print("Bronze orders:", bronze_orders.count())
print("Bronze order items:", bronze_order_items.count())


# COMMAND ----------

# 12. SILVER CUSTOMERS
silver_customers = (
    bronze_customers
    .withColumn("customer_id", trim(col("customer_id").cast("string")))
    .withColumn("name", trim(col("name").cast("string")))
    .withColumn("city", trim(col("city").cast("string")))
    .withColumn("name", regexp_replace(col("name"), r"\s+", " "))
    .withColumn("city", regexp_replace(col("city"), r"\s+", " "))
    .filter(col("customer_id").isNotNull())
    .filter(trim(col("customer_id")) != "")
    .filter(col("name").isNotNull())
    .filter(trim(col("name")) != "")
    .filter(col("city").isNotNull())
    .filter(trim(col("city")) != "")
    .dropDuplicates(["customer_id"])
)
display(silver_customers)


# COMMAND ----------

# 13. SILVER ORDERS

silver_orders = (
    bronze_orders

    # Clean IDs
    .withColumn(
        "order_id",
        trim(col("order_id").cast("string"))
    )
    .withColumn(
        "customer_id",
        trim(col("customer_id").cast("string"))
    )

    # Standardize status
    .withColumn(
        "status",
        upper(trim(col("status").cast("string")))
    )

    # Convert different date formats → DATE
    .withColumn(
        "order_date",
        F.coalesce(
            to_date(trim(col("order_date").cast("string")), "yyyy-MM-dd"),
            to_date(trim(col("order_date").cast("string")), "MM/dd/yyyy"),
            to_date(trim(col("order_date").cast("string")), "dd-MM-yyyy"),
            to_date(trim(col("order_date").cast("string")), "M/d/yy")
        )
    )

    # Remove invalid records
    .filter(col("order_id").isNotNull())
    .filter(trim(col("order_id")) != "")

    .filter(col("customer_id").isNotNull())
    .filter(trim(col("customer_id")) != "")

    .filter(col("order_date").isNotNull())

    .filter(
        col("status").isin(
            "DELIVERED",
            "CANCELLED"
        )
    )

    # Remove duplicate orders
    .dropDuplicates(["order_id"])
)

display(silver_orders)

# COMMAND ----------

# 14. SILVER ORDER ITEMS
silver_order_items = (
    bronze_order_items
    .withColumn("order_id", trim(col("order_id").cast("string")))
    .withColumn("product", trim(col("product").cast("string")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("price", col("price").cast("double"))
    .filter(col("order_id").isNotNull())
    .filter(trim(col("order_id")) != "")
    .filter(col("product").isNotNull())
    .filter(trim(col("product")) != "")
    .filter(col("quantity").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("price").isNotNull())
    .filter(col("price") >= 0)
)
display(silver_order_items)


# COMMAND ----------

# 15. SAVE SILVER
for df, table in [
    (silver_customers, SILVER_CUSTOMERS),
    (silver_orders, SILVER_ORDERS),
    (silver_order_items, SILVER_ITEMS)
]:
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(table))

print("Silver tables saved.")


# COMMAND ----------

# 16. DATA QUALITY — BRONZE + SILVER
checks = []

def check(dataset, name, count_value):
    checks.append((dataset, name, int(count_value)))

# Bronze customers
check("Bronze Customers", "NULL customer_id",
      bronze_customers.filter(col("customer_id").isNull()).count())
check("Bronze Customers", "Duplicate customer_id",
      bronze_customers.groupBy("customer_id").count().filter(col("count") > 1).count())
check("Bronze Customers", "NULL name",
      bronze_customers.filter(col("name").isNull()).count())
check("Bronze Customers", "NULL city",
      bronze_customers.filter(col("city").isNull()).count())

# Bronze orders
check("Bronze Orders", "NULL order_id",
      bronze_orders.filter(col("order_id").isNull()).count())
check("Bronze Orders", "Duplicate order_id",
      bronze_orders.groupBy("order_id").count().filter(col("count") > 1).count())
check("Bronze Orders", "NULL customer_id",
      bronze_orders.filter(col("customer_id").isNull()).count())
check("Bronze Orders", "NULL order_date",
      bronze_orders.filter(col("order_date").isNull()).count())
check("Bronze Orders", "Invalid status",
      bronze_orders.filter(~upper(trim(col("status"))).isin("DELIVERED", "CANCELLED")).count())

# Bronze items
check("Bronze Order Items", "NULL order_id",
      bronze_order_items.filter(col("order_id").isNull()).count())
check("Bronze Order Items", "NULL product",
      bronze_order_items.filter(col("product").isNull()).count())
check("Bronze Order Items", "Invalid quantity",
      bronze_order_items.filter(col("quantity").cast("int").isNull() | (col("quantity").cast("int") <= 0)).count())
check("Bronze Order Items", "Invalid price",
      bronze_order_items.filter(col("price").cast("double").isNull() | (col("price").cast("double") < 0)).count())

# Silver
check("Silver Customers", "Duplicate customer_id",
      silver_customers.groupBy("customer_id").count().filter(col("count") > 1).count())
check("Silver Orders", "Duplicate order_id",
      silver_orders.groupBy("order_id").count().filter(col("count") > 1).count())
check("Silver Orders", "Future order date",
      silver_orders.filter(col("order_date") > current_date()).count())
check("Silver Orders", "Invalid status",
      silver_orders.filter(~col("status").isin("DELIVERED", "CANCELLED")).count())
check("Silver Order Items", "Invalid quantity",
      silver_order_items.filter(col("quantity").isNull() | (col("quantity") <= 0)).count())
check("Silver Order Items", "Invalid price",
      silver_order_items.filter(col("price").isNull() | (col("price") < 0)).count())

# Referential integrity
check("Referential Integrity", "Orders with unknown customer_id",
      silver_orders.select("customer_id").distinct()
      .join(silver_customers.select("customer_id").distinct(), "customer_id", "left_anti").count())
check("Referential Integrity", "Items with unknown order_id",
      silver_order_items.select("order_id").distinct()
      .join(silver_orders.select("order_id").distinct(), "order_id", "left_anti").count())

quality_df = (
    spark.createDataFrame(checks, ["dataset", "check", "failed_records"])
    .withColumn("status", when(col("failed_records") == 0, "PASS").otherwise("FAIL"))
    .withColumn("checked_at", F.current_timestamp())
)

display(quality_df.orderBy(col("status").desc(), col("dataset"), col("check")))


# COMMAND ----------

# 17. SAVE QUALITY RESULTS
(quality_df.write.format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(QUALITY_TABLE))

failed_checks = quality_df.filter(col("status") == "FAIL").count()
print("Failed quality checks:", failed_checks)

if failed_checks > 0:
    display(quality_df.filter(col("status") == "FAIL"))
    raise Exception("Data quality gate failed. Gold generation stopped.")

print("ALL QUALITY CHECKS PASSED.")


# COMMAND ----------

# 18. GOLD ORDER SUMMARY
order_items_with_orders = (
    silver_order_items
    .join(
        silver_orders.select("order_id", "customer_id", "order_date", "status"),
        "order_id",
        "inner"
    )
    .withColumn("line_amount", col("quantity") * col("price"))
)

gold_order_summary = (
    order_items_with_orders
    .groupBy("order_id", "customer_id", "order_date", "status")
    .agg(
        spark_sum("quantity").alias("total_items"),
        spark_round(spark_sum("line_amount"), 2).alias("order_value")
    )
)

display(gold_order_summary)


# COMMAND ----------

# 19. GOLD CUSTOMER SUMMARY
customer_orders = (
    silver_orders.groupBy("customer_id").agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum(when(col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_orders"),
        spark_sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders")
    )
)

customer_revenue = (
    order_items_with_orders
    .filter(col("status") == "DELIVERED")
    .groupBy("customer_id")
    .agg(spark_round(spark_sum("line_amount"), 2).alias("total_revenue"))
)

gold_customer_summary = (
    silver_customers
    .join(customer_orders, "customer_id", "left")
    .join(customer_revenue, "customer_id", "left")
    .fillna({
        "total_orders": 0,
        "delivered_orders": 0,
        "cancelled_orders": 0,
        "total_revenue": 0.0
    })
)

display(gold_customer_summary)


# COMMAND ----------

# 20. SAVE GOLD
(gold_order_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_ORDER_SUMMARY))
(gold_customer_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_CUSTOMER_SUMMARY))

print("Gold tables saved successfully.")


# COMMAND ----------

# 21. FINAL VALIDATION
print("========== PIPELINE SUMMARY ==========")
print("Bronze Customers:", spark.table(BRONZE_CUSTOMERS).count())
print("Bronze Orders:", spark.table(BRONZE_ORDERS).count())
print("Bronze Items:", spark.table(BRONZE_ITEMS).count())
print("Silver Customers:", spark.table(SILVER_CUSTOMERS).count())
print("Silver Orders:", spark.table(SILVER_ORDERS).count())
print("Silver Items:", spark.table(SILVER_ITEMS).count())
print("Gold Orders:", spark.table(GOLD_ORDER_SUMMARY).count())
print("Gold Customers:", spark.table(GOLD_CUSTOMER_SUMMARY).count())
print("Processed Files:", spark.table(PROCESSED_FILES_TABLE).count())
print("======================================")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental test
# MAGIC
# MAGIC After the first successful run, add a second batch without deleting the first batch:
# MAGIC
# MAGIC ```text
# MAGIC customers/customers_20260815.csv
# MAGIC orders/orders_20260815.csv
# MAGIC order_items/order_items_20260815.csv
# MAGIC ```
# MAGIC
# MAGIC Run the notebook again. The file-detection cell should show only the new files.
# MAGIC
# MAGIC **Important:** the order-items MERGE uses `(order_id, product)` because this dataset has no separate `order_item_id`. If your real dataset has an item ID, use that as the MERGE key.
# MAGIC