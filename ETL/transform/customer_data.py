from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, regexp_replace, col, when, to_date, current_date

def cst_data():
    spark = SparkSession.builder \
        .appName("customer_data") \
        .getOrCreate()

    df_crm_cust = spark.read.csv("../bronze/source_crm/cust_info.csv", header=True)
    df_erp_cust = spark.read.csv("../bronze/source_erp/CUST_AZ12.csv", header=True)
    df_erp_loc = spark.read.csv("../bronze/source_erp/LOC_A101.csv", header=True)

    df_erp_loc = df_erp_loc.withColumnRenamed("CID","CID_loc")
    df_erp_loc = df_erp_loc.withColumn("CNTRY", when(col("CNTRY").isNull(), "Unknown").otherwise(col("CNTRY")))
    df_crm_cust = df_crm_cust.withColumn("cst_lastname", trim(df_crm_cust["cst_lastname"]))
    df_crm_cust = df_crm_cust.withColumn(
        "cst_gndr",
        when(col("cst_gndr") == "M", "Male")
        .when(col("cst_gndr") == "F", "Female")
        .otherwise(col("cst_gndr"))
    )

    df_crm_cust = df_crm_cust.withColumn(
        "cst_marital_status",
        when(col("cst_marital_status") == "M", "Married")
        .when(col("cst_marital_status") == "S", "Single")
        .otherwise(col("cst_marital_status"))
    )

    df_erp_cust = df_erp_cust.withColumn("cst_key", regexp_replace(col("CID"),"^NAS",""))
    df_erp_loc = df_erp_loc.withColumn("cst_key", regexp_replace(col("CID_loc"), "^([A-Z]+)-", "$1"))
    df_cust_crm_erp = df_crm_cust.join(df_erp_cust, "cst_key","inner")

    df_cust = df_cust_crm_erp.join(df_erp_loc, "cst_key", "inner")
    df_cust = df_cust.filter(df_cust["cst_key"].startswith("AW000"))
    df_cust = df_cust.withColumn(
        "cst_gndr",
        when(col("cst_gndr").isNull(), col("GEN"))
        .otherwise(col("cst_gndr"))
    )

    df_cust = df_cust.withColumn("cst_create_date", to_date(col("cst_create_date"), "yyyy-MM-dd"))
    df_cust = df_cust.withColumn("BDATE", to_date(col("BDATE"), "yyyy-MM-dd"))

    df_cust = df_cust.filter(col("BDATE") <= current_date())

    df_cust = df_cust.filter(col("cst_firstname").isNotNull() & col("cst_lastname").isNotNull())

    df_cust = df_cust.withColumn("cst_gndr", when(col("cst_gndr").isNull(), "Unknown").otherwise(col("cst_gndr")))

    df_cust = df_cust.dropDuplicates(["cst_id"])

    return df_cust

if __name__ == "__main__":
    df = cst_data()
    df.filter(col("cst_id")=="29466").show()
