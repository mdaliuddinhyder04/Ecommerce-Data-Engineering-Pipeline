# 🛒 E-Commerce Incremental ETL Data Pipeline

<p align="center">
  <b>Incremental E-Commerce Data Engineering Pipeline using PySpark, Databricks, Delta Lake & Apache Airflow</b>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-PySpark-blue">
  <img src="https://img.shields.io/badge/SQL-Databricks-orange">
  <img src="https://img.shields.io/badge/Databricks-Delta%20Lake-red">
  <img src="https://img.shields.io/badge/ETL-Medallion%20Architecture-purple">
  <img src="https://img.shields.io/badge/Orchestration-Apache%20Airflow-green">
</p>

---

## 📖 Overview

This project implements an end-to-end **incremental e-commerce ETL pipeline** using **PySpark and Databricks**.

The pipeline ingests customer, order, and order-item CSV files from a raw landing area, processes only newly added files, stores the data in Delta Lake, performs data cleansing and validation, runs data-quality checks, and produces analytics-ready Gold tables.

The project follows the **Medallion Architecture**:

```text
Raw CSV
   ↓
Incremental Bronze
   ↓
Silver
   ↓
Data Quality
   ↓
Gold
   ↓
Analytics / BI
```

The pipeline is designed to work with repeated daily or batch file arrivals without reprocessing files that have already been ingested.

---

# ✨ Key Features

### 📥 Incremental File Ingestion
- Reads CSV files from separate customer, order, and order-item folders
- Detects newly added files
- Maintains a `processed_raw_files` Delta table
- Prevents previously processed files from being ingested again
- Supports keeping historical raw files in the landing area

### 🥉 Bronze Layer
- Stores incrementally ingested data in Delta tables
- Uses Delta Lake `MERGE` operations for customer, order, and order-item data
- Maintains persistent Bronze datasets across pipeline runs

### 🥈 Silver Layer
- Cleans and standardizes raw data
- Trims whitespace
- Standardizes customer and city text
- Standardizes order status
- Converts dates into a proper Spark `DATE` type
- Supports multiple incoming date formats
- Removes invalid records
- Removes duplicate customer and order IDs
- Validates quantity and price values

### 🧪 Data Quality
The pipeline validates:

- NULL IDs
- Duplicate IDs
- NULL customer references
- NULL dates
- Invalid order statuses
- Invalid quantities
- Invalid prices
- Future order dates
- Referential integrity between customers and orders
- Referential integrity between orders and order items

A **quality gate** prevents Gold generation when quality checks fail.

### 🥇 Gold Layer
Creates analytics-ready Delta tables:

- Gold Order Summary
- Gold Customer Summary

Gold includes metrics such as:

- Total items
- Order value
- Total orders
- Delivered orders
- Cancelled orders
- Customer revenue

### ⚙️ Orchestration
The Databricks notebook is designed to be triggered by an **Apache Airflow DAG / Databricks Job**.

Airflow is responsible for orchestration and scheduling, while Databricks performs the actual ETL processing.

---

# 🏗️ Architecture

```text
                    E-COMMERCE CSV BATCH
                             │
                             ▼
                    Raw Landing Area
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
         Customers        Orders       Order Items
              │              │              │
              └──────────────┼──────────────┘
                             ▼
                    File Detection
                             │
                    New Files Only
                             ▼
                    ┌────────────────┐
                    │ Bronze Delta   │
                    │ Incremental    │
                    │ Ingestion      │
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │ Silver Delta   │
                    │ Clean +        │
                    │ Transform      │
                    └───────┬────────┘
                            ▼
                    ┌────────────────┐
                    │ Data Quality   │
                    │ Validation     │
                    └───────┬────────┘
                            │
                       Quality PASS
                            │
                            ▼
                    ┌────────────────┐
                    │ Gold Delta     │
                    │ Analytics      │
                    └───────┬────────┘
                            ▼
                       SQL / BI
                            ▲
                            │
                       Apache Airflow
```

---

# 🔄 Incremental Processing

The pipeline does not reprocess every CSV on every run.

A Delta table called:

```text
workspace.default.processed_raw_files
```

stores:

```text
file_path
dataset
processed_at
```

When the pipeline runs:

```text
Raw Folder
    │
    ├── customers_20260814.csv  → Already processed → SKIP
    ├── customers_20260815.csv  → New              → PROCESS
    └── customers_20260816.csv  → New              → PROCESS
```

Only new files are read into the incremental Bronze ingestion step.

After ingestion, the complete Bronze tables are used to rebuild Silver and Gold so that the final analytical datasets contain both historical and newly ingested data.

### Example

First run:

```text
O0001
O0002
O0003
```

Second run with a new file:

```text
O0004
O0005
```

Final Gold:

```text
O0001
O0002
O0003
O0004
O0005
```

---

# 🥉 Bronze Tables

The pipeline creates:

```text
workspace.default.bronze_customers
workspace.default.bronze_orders
workspace.default.bronze_order_items
```

Bronze is the persistent Delta ingestion layer.

---

# 🥈 Silver Tables

The pipeline creates:

```text
workspace.default.silver_customers
workspace.default.silver_orders
workspace.default.silver_order_items
```

### Customer transformations

- Trim IDs and text
- Normalize whitespace
- Remove NULL/blank customer IDs
- Remove NULL/blank names
- Remove NULL/blank cities
- Remove duplicate customer IDs

### Order transformations

- Trim IDs
- Standardize status to uppercase
- Convert multiple date formats into a Spark `DATE`
- Accept formats such as:

```text
2026-08-15
08/15/2026
15-08-2026
8/15/26
```

All valid dates become a proper date value displayed as:

```text
2026-08-15
```

- Remove invalid IDs
- Remove invalid dates
- Allow only:

```text
DELIVERED
CANCELLED
```

- Remove duplicate order IDs

### Order-item transformations

- Trim order IDs and product names
- Cast quantity to integer
- Cast price to double
- Remove invalid order IDs
- Remove invalid products
- Remove NULL/invalid quantities
- Remove NULL/negative prices

---

# 🧪 Data Quality Framework

The pipeline produces:

```text
workspace.default.data_quality_results
```

Each check contains:

```text
dataset
check
failed_records
status
checked_at
```

Example:

| Dataset | Check | Failed Records | Status |
|---|---|---:|---|
| Customers | Duplicate customer_id | 0 | PASS |
| Orders | Duplicate order_id | 0 | PASS |
| Orders | Invalid status | 0 | PASS |
| Orders | Future order date | 0 | PASS |
| Order Items | Invalid quantity | 0 | PASS |
| Order Items | Invalid price | 0 | PASS |
| Referential Integrity | Unknown customer_id | 0 | PASS |

### Quality Gate

```text
Data Quality
     │
     ├── PASS ──→ Gold Generation
     │
     └── FAIL ──→ Pipeline Stops
```

This prevents invalid data from being promoted into the Gold layer.

---

# 🥇 Gold Tables

## Gold Order Summary

```text
workspace.default.gold_order_summary
```

Contains:

- `order_id`
- `customer_id`
- `order_date`
- `status`
- `total_items`
- `order_value`

Order value is calculated as:

```text
quantity × price
```

---

## Gold Customer Summary

```text
workspace.default.gold_customer_summary
```

Contains:

- `customer_id`
- `name`
- `city`
- `total_orders`
- `delivered_orders`
- `cancelled_orders`
- `total_revenue`

Revenue is calculated from delivered order items.

---

# 📂 Project Structure

```text
E-Commerce-ETL-Data-Pipeline/
│
├── ecommerce_incremental_databricks_pipeline.py
│
├── ecommerce_raw/
│   ├── customers/
│   │   ├── customers_20260814.csv
│   │   └── customers_20260815.csv
│   │
│   ├── orders/
│   │   ├── orders_20260814.csv
│   │   └── orders_20260815.csv
│   │
│   └── order_items/
│       ├── order_items_20260814.csv
│       └── order_items_20260815.csv
│
├── airflow/
│   └── dags/
│       └── ecommerce_pipeline.py
│
├── screenshots/
│
└── README.md
```

> The current Databricks notebook and sample raw datasets are included in this project. Add the Airflow DAG under `airflow/dags/` when the orchestration layer is committed to the repository.

---

# ⚙️ Tech Stack

## Data Engineering
- Python
- PySpark
- SQL
- ETL
- Incremental Data Processing
- Data Quality

## Cloud / Data Platform
- Databricks
- Delta Lake
- Databricks Jobs

## Orchestration
- Apache Airflow

## Architecture
- Medallion Architecture
- Bronze / Silver / Gold

## Storage
- CSV
- Delta Tables

---

# 🚀 Databricks Setup

## 1. Create the Raw Folder Structure

Create:

```text
ecommerce_raw/
├── customers/
├── orders/
└── order_items/
```

Upload CSV batches into the appropriate folders.

Example:

```text
customers/customers_20260815.csv
orders/orders_20260815.csv
order_items/order_items_20260815.csv
```

---

## 2. Configure the Notebook

Update:

```python
RAW_PATH = "/Workspace/Users/<your-user>/deproject/ecommerce_raw"
```

The catalog and schema used by this project are:

```text
Catalog: workspace
Schema: default
```

If your Databricks environment uses a different catalog/schema, update the configuration accordingly.

---

## 3. Run the Notebook

Execute the Databricks notebook from top to bottom.

The notebook will:

1. Create required Delta tables
2. Discover raw CSV files
3. Identify new files
4. Read only new files
5. Load/merge Bronze
6. Build Silver
7. Run data-quality checks
8. Stop if quality checks fail
9. Generate Gold
10. Display final pipeline counts

---

# 🔁 Testing Incremental Loads

After the first successful run, **do not delete the old CSV files**.

Add a new batch:

```text
ecommerce_raw/
├── customers/
│   ├── customers_20260814.csv
│   └── customers_20260815.csv
│
├── orders/
│   ├── orders_20260814.csv
│   └── orders_20260815.csv
│
└── order_items/
    ├── order_items_20260814.csv
    └── order_items_20260815.csv
```

Run the notebook again.

The file detection step should identify only:

```text
customers_20260815.csv
orders_20260815.csv
order_items_20260815.csv
```

as new files.

The final Silver and Gold tables contain the combined historical + newly processed data.

---

# ⚙️ Airflow Orchestration

The recommended production-style flow is:

```text
Airflow DAG
    │
    ▼
Databricks Job
    │
    ▼
Databricks Notebook
    │
    ├── Incremental Raw Ingestion
    ├── Bronze
    ├── Silver
    ├── Data Quality
    └── Gold
```

Airflow can schedule the Databricks Job daily or according to the required batch frequency.

Example:

```text
Every day
    ↓
Airflow DAG
    ↓
Trigger Databricks Job
    ↓
Process new CSV files
    ↓
Update Delta tables
    ↓
Quality validation
    ↓
Gold tables
```

---

# 📊 Final Data Flow

```text
CSV Files
   │
   ▼
Raw Landing
   │
   ▼
New File Detection
   │
   ▼
Incremental Bronze Delta
   │
   ▼
Silver Delta
   │
   ├── Cleaning
   ├── Standardization
   ├── Type Conversion
   ├── Deduplication
   └── Validation
   │
   ▼
Data Quality
   │
   ├── PASS ──────────┐
   │                  ▼
   │                 Gold
   │                  │
   │                  ▼
   │              Analytics
   │
   └── FAIL → Pipeline Stops
```

---

# 🧠 Key Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- Incremental ingestion
- Medallion architecture
- PySpark transformations
- Delta Lake
- Delta MERGE
- ETL pipeline design
- Data cleansing
- Data type standardization
- Date normalization
- Deduplication
- Data-quality validation
- Referential integrity
- Quality gates
- Analytics-ready Gold datasets
- Databricks Jobs
- Airflow orchestration
- Batch processing
- Idempotent file detection

---

# ⚠️ Important Design Note

The current implementation records files in `processed_raw_files` immediately after successful Bronze ingestion.

For a more production-grade retry design, the processed-file record should be committed only after the complete downstream pipeline succeeds:

```text
Bronze
  ↓
Silver
  ↓
Quality
  ↓
Gold
  ↓
Mark file as processed
```

This prevents a failed downstream run from permanently marking a file as processed.

The current implementation is suitable for demonstrating incremental ingestion, while this improvement can be added as a next iteration.

---

# 🔮 Future Enhancements

- Add a dedicated Airflow DAG to the repository
- Trigger Databricks Jobs through Airflow
- Move raw data from Workspace storage to Databricks Volumes or cloud object storage
- Add ingestion metadata such as batch ID and source file name
- Add pipeline audit logging
- Add retry-safe processed-file tracking
- Add automated notifications for failed quality checks
- Add partitioning and performance optimization
- Add Databricks Workflows dependencies
- Add Power BI dashboards
- Add cloud storage integration with AWS S3 / Azure Data Lake
- Add CI/CD for the Databricks project

---

# 🎓 Learning Outcomes

This project demonstrates how to design and implement an end-to-end Data Engineering pipeline that:

- Ingests continuously arriving batch data
- Processes only new files
- Uses Delta Lake for reliable storage
- Applies Bronze/Silver/Gold architecture
- Cleans and validates data using PySpark
- Implements data-quality gates
- Produces analytics-ready datasets
- Can be orchestrated through Apache Airflow

---

# 👨‍💻 Author

**Mohammed Aliuddin Hyder**

📧 mohammedaliuddinhyder04@gmail.com

🌐 GitHub: https://github.com/mdaliuddinhyder04

💼 LinkedIn: https://www.linkedin.com/in/mdaliuddinhyder04

---

# ⭐ Support

If you found this project useful, consider giving the repository a ⭐ on GitHub.

This project is built as a practical demonstration of **Data Engineering, PySpark, Databricks, Delta Lake, ETL, Data Quality, Incremental Processing, and Airflow orchestration**.
