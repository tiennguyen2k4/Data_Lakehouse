from ETL.transform.customer_data import cst_data
from ETL.transform.product_data import prd_data
from ETL.transform.sales_data import sls_data
from pyspark.sql.functions import col, datediff, floor, lit, concat_ws, current_date, expr
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

df_cust = cst_data()
df_prd = prd_data()
df_sls = sls_data()


spark = SparkSession.builder.appName("dim_fact_table").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    days = (end - start).days + 1
    return [(start + timedelta(days=i),) for i in range(days)]

date_list = generate_date_range("2017-01-01", "2024-12-31")
df_date = spark.createDataFrame(date_list, ["date"])

dim_date = df_date.select(
    col("date"),
    expr("date_format(date, 'yyyyMMdd')").cast("int").alias("date_key"),
    expr("day(date)").alias("day"),
    expr("month(date)").alias("month"),
    expr("year(date)").alias("year"),
    expr("date_format(date, 'E')").alias("weekday_name"),
    expr("date_format(date, 'u')").cast("int").alias("weekday_number"),
    expr("quarter(date)").alias("quarter"),
    expr("CASE WHEN dayofweek(date) IN (1, 7) THEN true ELSE false END").alias("is_weekend")
)


dim_cust = df_cust.select(
    col("cst_id"),
    col("cst_key").alias("crm_cst_key"),
    col("CID").alias("erp_cst_key"),
    concat_ws(" ",
              col("cst_firstname"),
              col("cst_lastname")
              ).alias("full_name"),
    col("cst_firstname").alias("first_name"),
    col("cst_lastname").alias("last_name"),
    col("cst_marital_status").alias("marital_status"),
    col("cst_gndr").alias("gender"),
    col("BDATE").alias("cst_birthday"),
    floor((datediff(current_date(), col("BDATE")) / 365)).alias("age"),
    col("CNTRY").alias("country"),
    col("cst_create_date").alias("create_date"),
    lit("CRM+ERP").alias("data_source"),
)

dim_prd = df_prd.select(
    col("prd_id"),
    col("ID").alias("cat_id"),
    col("CAT").alias("cat_name"),
    col("SUBCAT").alias("subcat_name"),
    col("prd_key"),
    col("prd_name"),
    col("color"),
    col("size"),
    col("prd_cost"),
    col("prd_line"),
    col("prd_start_dt"),
    col("prd_end_dt"),
    col("MAINTENANCE").alias("maintenance"),
    lit("CRM+ERP").alias("data_source"),
)

fact_sls = df_sls.select(
    col("sls_ord_num"),
    col("prd_id"),
    col("cst_id"),
    col("prd_cost").alias("sls_price"),
    col("sls_quantity"),
    (col("prd_cost")*col("sls_quantity")).alias("sls_sales_amount"),
    col("sls_order_dt")
)

fact_sls = fact_sls \
    .join(dim_date.withColumnRenamed("date", "order_date"), fact_sls.sls_order_dt == col("order_date")) \
    .withColumnRenamed("date_key", "order_date_key") \
    .drop("order_date") \
    .drop("sls_order_dt") \
    .drop(*[c for c in dim_date.columns if c not in ["date", "date_key"]])

# dim_date.show(5)
# dim_date.describe().show()
# fact_sls.show(5)
# fact_sls.describe().show()
print("Starting write")
dim_date.coalesce(1).write.option("header", True).mode("overwrite").csv("/app/Storage_Data/dim_date")
dim_cust.coalesce(1).write.option("header", True).mode("overwrite").csv("/app/Storage_Data/dim_customer")
dim_prd.coalesce(1).write.option("header", True).mode("overwrite").csv("/app/Storage_Data/dim_product")
fact_sls.coalesce(1).write.option("header", True).mode("overwrite").csv("/app/Storage_Data/fact_sales")
print("Successfully")

