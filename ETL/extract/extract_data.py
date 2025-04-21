from ETL.spark_session import get_spark_session

spark = get_spark_session("extract_data")

df_crm_cust = spark.read.csv("/app/datasets/source_crm/cust_info.csv", header=True)
df_crm_prd = spark.read.csv("/app/datasets/source_crm/prd_info.csv", header=True)
df_crm_sales = spark.read.csv("/app/datasets/source_crm/sales_details.csv", header=True)
df_erp_cust = spark.read.csv("/app/datasets/source_erp/CUST_AZ12.csv", header=True)
df_erp_loc = spark.read.csv("/app/datasets/source_erp/LOC_A101.csv", header=True)
df_erp_prd_cat = spark.read.csv("/app/datasets/source_erp/PX_CAT_G1V2.csv", header=True)

df_crm_cust.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_crm/customers")

df_crm_prd.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_crm/products")

df_crm_sales.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_crm/sales")

df_erp_cust.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_erp/customers")

df_erp_loc.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_erp/locations")

df_erp_prd_cat.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/bronze/source_erp/product_categories")

print("Successfully")

spark.stop()