from pyspark.sql.functions import concat_ws, col, date_add, expr
from pyspark.sql.types import IntegerType
from ETL.spark_session import get_spark_session

spark = get_spark_session("sales_data")

# Read data from sliver and bronze layer
df_crm_sales = spark.read.parquet("s3a://data/bronze/source_crm/sales")
dim_customer = spark.read.parquet("s3a://data/silver/dim_customer")
dim_product = spark.read.parquet("s3a://data/silver/dim_product")
dim_date = spark.read.parquet("s3a://data/silver/dim_date")

# Clean data
df_crm_sales = df_crm_sales.withColumn("sls_cst_prd_id", concat_ws("_",df_crm_sales["sls_cust_id"], df_crm_sales["sls_prd_key"]))

# Join dim_cust and dim_product table
dim_cust_prd = dim_customer.crossJoin(dim_product) \
    .withColumn("cst_prd_id", concat_ws("_", col("cst_id"), col("prd_key"))) \
    .select("cst_prd_id", "prd_cost", "prd_id", "prd_start_dt", )

# Filter the rows do not include cust_id and prd_id in dim_cust_prd
df_crm_sales = df_crm_sales.join(dim_cust_prd, dim_cust_prd["cst_prd_id"] == df_crm_sales["sls_cst_prd_id"], "inner")

df_crm_sales = df_crm_sales.withColumn("sls_quantity", col("sls_quantity").cast(IntegerType()))
df_crm_sales = df_crm_sales.withColumn("sls_cust_id", col("sls_cust_id").cast(IntegerType()))


df_crm_sales = df_crm_sales.filter(col("sls_ord_num").isNotNull())
df_crm_sales = df_crm_sales.dropDuplicates(["sls_ord_num"])

#Create fact_sales table
fact_sales = df_crm_sales.select(
    col("sls_ord_num").alias("sls_ord_id"),
    col("prd_id"),
    col("sls_cust_id").alias("cst_id"),
    col("prd_cost").alias("sls_price"),
    col("sls_quantity"),
    expr("prd_cost * sls_quantity").alias("sls_sales"),
    date_add(col("prd_start_dt"), 3).alias("sls_order_dt")
)

# Join fact_sales table with dim_date
dim_date_prep = dim_date.select(col("full_date"), col("date_key"))
fact_sales = fact_sales.join(
    dim_date_prep, fact_sales["sls_order_dt"]==dim_date_prep["full_date"], "left"
).withColumnRenamed("date_key", "sls_date_id") \
 .drop("full_date")

# Check qualified data
assert fact_sales.count() > 0, "Error: Empty DataFrame"

# Write to sliver layer
fact_sales.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/silver/fact_sales")


print("Successfully")

spark.stop()
