from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, col
from data.sliver.customer_data import cst_data
from data.sliver.product_data import prd_data

def sls_data():
    spark = SparkSession.builder \
        .appName("sales_data") \
        .getOrCreate()

    df_crm_sales = spark.read.csv("../bronze/source_crm/sales_details.csv", header=True)

    df_cst = cst_data()
    df_prd = prd_data()

    df_crm_sales = df_crm_sales.join(df_cst, df_cst["cst_id"]==df_crm_sales["sls_cust_id"], "inner")
    df_crm_sales = df_crm_sales.join(df_prd, df_crm_sales["sls_prd_key"]==df_prd["prd_key"], "inner")

    df_crm_sales = df_crm_sales.withColumn("sls_order_dt", date_add(col("prd_start_dt"), 10))
    # df_crm_sales = df_crm_sales.withColumn("sls_ship_dt", date_add(col("sls_order_dt"), 5))
    # df_crm_sales = df_crm_sales.withColumn("sls_due_dt", date_add(col("sls_ship_dt"), 15))

    return df_crm_sales

if __name__ == "__main__":
    df = sls_data()
    df.show(5)
    df.describe().show()

