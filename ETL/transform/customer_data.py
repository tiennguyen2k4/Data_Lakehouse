from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, regexp_replace, col, when, to_date, current_date, lit, concat_ws, floor, datediff
from pyspark.sql.types import IntegerType

# SparkSession configuration
spark = SparkSession.builder \
    .appName("customer_data") \
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
    .config("spark.sql.parquet.writeLegacyFormat", "false") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read data from bronze
df_crm_cust = spark.read.parquet("/app/data/bronze/source_crm/customers")
df_erp_cust = spark.read.parquet("/app/data/bronze/source_erp/customers")
df_erp_loc = spark.read.parquet("/app/data/bronze/source_erp/locations")

# Clean data ERP Location
df_erp_loc = df_erp_loc.withColumnRenamed("CID", "CID_loc")
df_erp_loc = df_erp_loc.withColumn(
    "CNTRY", 
    when(col("CNTRY") == "US", "China")
    .when(col("CNTRY") == "DE", "Vietnam")
    .when(col("CNTRY") == "USA", "United States")
    .when(col("CNTRY").isNull(), "China")
    .otherwise(col("CNTRY"))
)

# Clean data CRM Customer
df_crm_cust = df_crm_cust.withColumn("cst_lastname", trim(df_crm_cust["cst_lastname"]))
df_crm_cust = df_crm_cust.withColumn(
    "cst_gndr",
    when(col("cst_gndr") == "M", "Male")
    .when(col("cst_gndr") == "F", "Female")
    .when(col("cst_gndr").isNull(), "LBGT")
    .otherwise(col("cst_gndr"))
)
df_crm_cust = df_crm_cust.withColumn(
    "cst_marital_status",
    when(col("cst_marital_status") == "M", "Married")
    .when(col("cst_marital_status") == "S", "Single")
    .otherwise(col("cst_marital_status"))
)

# Join CRM and ERP
df_erp_cust = df_erp_cust.withColumn("cst_key", regexp_replace(col("CID"), "^NAS", ""))
df_erp_loc = df_erp_loc.withColumn("cst_key", regexp_replace(col("CID_loc"), "^([A-Z]+)-", "$1"))

df_cust_crm_erp = df_crm_cust.join(df_erp_cust, "cst_key", "inner")
df_cust = df_cust_crm_erp.join(df_erp_loc, "cst_key", "inner")

# Filter and check data
df_cust = df_cust.filter(df_cust["cst_key"].startswith("AW000"))
df_cust = df_cust.filter(col("cst_firstname").isNotNull() & col("cst_lastname").isNotNull())
df_cust = df_cust.dropDuplicates(["cst_id"])

# Handle date
df_cust = df_cust.withColumn(
    "cst_create_date", to_date(col("cst_create_date"), "MM/dd/yyyy")
)
df_cust = df_cust.withColumn(
    "BDATE", to_date(col("BDATE"), "MM/dd/yyyy")
)

df_cust = df_cust.withColumn("cst_id",col("cst_id").cast(IntegerType()))

# Create dim customer table
dim_cust = df_cust.select(
    col("cst_id"),
    col("cst_key").alias("crm_cst_key"),
    col("CID").alias("erp_cst_key"),
    concat_ws(" ", col("cst_firstname"), col("cst_lastname")).alias("full_name"),
    col("cst_firstname").alias("first_name"),
    col("cst_lastname").alias("last_name"),
    col("cst_marital_status").alias("marital_status"),
    col("cst_gndr").alias("gender"),
    col("BDATE").alias("cst_birthday"),
    floor(datediff(current_date(), col("BDATE")) / 365).alias("age"),
    col("CNTRY").alias("country"),
    col("cst_create_date").alias("create_date"),
    lit("CRM+ERP").alias("cst_data_source"),
)

# Check qualified data
assert dim_cust.filter(col("cst_id").isNull()).count() == 0, "Error: cst_id is null"
assert dim_cust.count() > 0, "Error: No data in dim_customer"

# Write to silver
dim_cust.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("/app/data/silver/dim_customer")

print("Successfully")

spark.stop()