# Data Lakehouse Architecture using Apache Spark, Minio, PostgreSQL, and Power BI

This project illustrates a modern **Data Lakehouse** architecture implemented with **Docker**, following the **Medallion Architecture** pattern (Bronze → Silver → Gold).

## 📊 Overview

The system extracts raw data from multiple CSV sources (e.g., CRM and ERP systems), processes it using **Apache Spark**, and organizes it into three logical layers in a data lakehouse:

- **Bronze Layer**: Raw, unprocessed data
- **Silver Layer**: Cleaned, structured data in dimensional format (fact and dimension tables)
- **Gold Layer**: Business-level aggregated data, accessible via views for reporting

Data is then loaded into **Minio, PostgreSQL** and visualized using **Power BI**.

**Minio** contains **Bronze Layer** and **Silver Layer**.

**PostgreSQL** contains **Silver layer** and **Gold Layer**.


---
## 🗺️ System Architecture
![Data Pipeline](/image/system_architecture.png)

## 🧱 Architecture Layers

### 🥉 Bronze Layer
- Stores raw ingested data with minimal processing.
- Data from CRM and ERP sources is extracted as CSV files.
- Primary goal: Keep a backup of the original data in its raw form.

### 🥈 Silver Layer
- Contains **fact and dimension tables**, such as:
  - `dim_customer`, `dim_product`.
  - `fact_sales`.
- Data is cleaned, normalized, and modeled using Spark.
- Ready for business analysis and visualization.

### 🥇 Gold Layer
- Contains **business views and pre-aggregated datasets**.
- SQL-based transformations (e.g., monthly revenue, sales by region) are applied.
- Used directly by BI tools such as Power BI.

---

## ⚙️ Components

| Component      | Description |
|----------------|-------------|
| **Apache Spark** | Reads, transforms, and loads data across all layers. |
| **Minio**        | Stores Bronze and Silver layer. |
| **PostgreSQL**   | Stores Silver and Gold layer. |
| **Power BI**     | Connects to PostgreSQL for final reporting and dashboards. |
| **Docker**       | Manages containerized deployment for all services. |

---

## 📁 Data Flow

1. CSV files from CRM and ERP systems are read by Spark.
2. Spark extracts the raw data into the **Bronze Layer**.
3. Transformed and cleaned data (dim/fact tables) are written to the **Silver Layer**.
4. Business views and aggregations are generated in the **Gold Layer**.
5. All data of **Bronze Layer** and **Silver Layer** is stored in **Minio**.
6. Load data from **Silver Layer** and **Gold Layer** into **PostgreSQL**.
6. **Power BI** connects to PostgreSQL to create dashboards and reports.

---

## 📌 Notes

- All data processing is performed using **Apache Spark**.
- The **Silver Layer** is responsible for dimensional modeling and serves as the main source for most analytical queries.
- The **Gold Layer** contains only pre-defined views for business reporting.

---

## 📈 Example Views in the Gold Layer

- `monthly_active_customers.sql`
![Data Pipeline](/image/monthly_active_customer.png)
- `sales_by_region`
![Data Pipeline](/image/sales_by_region.png)
- `customer_segmentation_age`
![Data Pipeline](/image/customer_segmentation_age.png)
- `revenue_by_month_year`
![Data Pipeline](/image/revenue_by_month_year.png)


---

## 🚀 Getting Started

### 🔁 Clone the repository:
```sh 
git clone https://github.com/tiennguyen2k4/Data-Warehouse.git
```
This project runs in a **Dockerized environment**. To start the services:
```sh
cd docker
```
Build the Spark image:
```sh
docker build -t bitnami/spark:latest .
```
Start the containers:
```sh
docker-compose up -d
```
Connect to http://localhost:9001, create a bucket named **data**, generate an **access key** and **secret key**, and then write them to the minio.env file as **MINIO_ACCESS_KEY** and **MINIO_SECRET_KEY**.

🧠 Access the spark-master container:
```sh 
docker exec -it spark-master /bin/bash
```
---
### 📦 ETL Process

#### 1️⃣ Extract data to the Bronze Layer:
```sh
spark-submit ETL/extract/extract_data.py
```
---
#### 2️⃣ Transform data to the Silver Layer:
```sh 
spark-submit ETL/transform/customer_data.py
```
```sh
spark-submit ETL/transform/date_data.py
```
```sh 
spark-submit ETL/transform/product_data.py
```
```sh 
spark-submit ETL/transform/sales_data.py
```
---
#### 3️⃣ Create tables in PostgreSQL:
```sh
python ETL/load/create_table.py
```
---
#### 4️⃣ Load Silver Layer into PostgreSQL:
```sh 
spark-submit ETL/load/load_data_to_postgres.py
```
---
#### 5️⃣ Load Gold Layer views into PostgreSQL:

##### Access the postgresql container:
```sh
docker exec -it postgresql /bin/bash
```
##### Run each SQL file to create views:
```sh 
psql -U datawarehouse -d datawarehouse -f gold_layer/customer_segmentation_age.sql
```
```sh
psql -U datawarehouse -d datawarehouse -f gold_layer/monthly_active_customers.sql
```
```sh 
psql -U datawarehouse -d datawarehouse -f gold_layer/potential_customer.sql
```
```sh 
psql -U datawarehouse -d datawarehouse -f gold_layer/product_sales_ranking.sql
```
```sh
psql -U datawarehouse -d datawarehouse -f gold_layer/revenue_by_month_year.sql
```
```sh 
psql -U datawarehouse -d datawarehouse -f gold_layer/slaes_by_region.sql
```
---
## 📊 Reporting

Connect to Power BI to visualize and analyze the data from the Gold Layer.




