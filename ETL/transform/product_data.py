from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import regexp_replace, col, when, regexp_extract, expr, lit
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("product_data") \
    .config("spark.sql.parquet.writeLegacyFormat", "false") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read data from Bronze Layer
df_crm_prd = spark.read.parquet("/app/data/bronze/source_crm/products")
df_erp_cat = spark.read.parquet("/app/data/bronze/source_erp/product_categories")

# Transform ID
df_crm_prd = df_crm_prd.withColumn("ID", regexp_extract("prd_key", r'(.{5})', 1))
df_erp_cat = df_erp_cat.withColumn("ID", regexp_replace("ID", "_", "-"))


# Join data
df_prd = df_crm_prd.join(df_erp_cat, "ID", "inner") \
    .filter(col("CAT").isNotNull() & col("prd_cost").isNotNull())

# Handle null values
df_prd = df_prd.withColumn("prd_line",
    when(col("prd_line").isNull(), lit(None)).otherwise(col("prd_line")))

# Create color column
df_prd = df_prd.withColumn(
    "color",
    when(
        regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$", 1) != "",
        regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$", 1)
    ).when(
        regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*$", 1) != "",
        regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*$", 1)
    ).otherwise(lit(None))
)

df_prd = df_prd.withColumn(
    "size",
    when(
        regexp_extract(col("prd_nm"), "-\\s*(\\d+|[A-Z]{1,3})\\s*$", 1) != "",
        regexp_extract(col("prd_nm"), "-\\s*(\\d+|[A-Z]{1,3})\\s*$", 1)
    ).otherwise(lit(None))
)

df_prd = df_prd.withColumn(
    "color",
    expr("""
        CASE
            WHEN color = size AND size IS NOT NULL THEN NULL
            ELSE color
        END
    """)
)

# Create prd_name column
df_prd = df_prd.withColumn(
    "prd_name",
    regexp_replace(
        col("prd_nm"),
        "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$|" +
        "-\\s*([A-Za-z]+)\\s*$|" +
        "-\\s*(\\d+|[A-Z]{1,3})\\s*$",
        ""
    )
).withColumn(
    "prd_key",
    expr("regexp_replace(prd_key, CONCAT('^', ID, '-'), '')")
)

df_prd = df_prd.withColumn("prd_id", col("prd_id").cast(IntegerType()))
df_prd = df_prd.withColumn("prd_cost", col("prd_cost").cast(IntegerType()))
df_prd = df_prd.withColumn("prd_start_dt", to_date(col("prd_start_dt"), "MM/dd/yyyy"))
df_prd = df_prd.dropDuplicates(["prd_key"])


# Create dim_prd table
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
    col("MAINTENANCE").alias("maintenance"),
    lit("CRM+ERP").alias("prd_data_source")
)

# Check qualified data
assert dim_prd.count() > 0, "Error: Empty DataFrame"
assert dim_prd.filter(col("prd_id").isNull()).count() == 0, "Error: prd_id contains null"

# Write to Silver Layer
dim_prd.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("/app/data/silver/dim_product")

print("Successfully")

spark.stop()