from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, when, regexp_extract, expr, udf, lit
from pyspark.sql.types import DateType
import numpy as np
from datetime import datetime, timedelta


def change_date(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    days_between = (end - start).days

    def random_date():
        return start + timedelta(days=np.random.randint(days_between))

    return udf(random_date, DateType())


def prd_data():
    spark = SparkSession.builder \
        .appName("product_data") \
        .getOrCreate()

    # Đọc dữ liệu
    df_crm_prd = spark.read.csv("../bronze/source_crm/prd_info.csv", header=True)
    df_erp_cat = spark.read.csv("../bronze/source_erp/PX_CAT_G1V2.csv", header=True)

    # Chuẩn hóa ID
    df_crm_prd = df_crm_prd.withColumn("ID", regexp_extract("prd_key", r'(.{5})', 1))
    df_erp_cat = df_erp_cat.withColumn("ID", regexp_replace("ID", "_", "-"))

    # Join dữ liệu
    df_prd = df_crm_prd.join(df_erp_cat, "ID", "inner")
    df_prd = df_prd.filter(col("CAT").isNotNull() & col("prd_cost").isNotNull())

    # Xử lý giá trị null
    df_prd = df_prd.withColumn("prd_line", when(col("prd_line").isNull(), lit("null")).otherwise(col("prd_line")))
    df_prd = df_prd.withColumn("prd_end_dt", when(col("prd_end_dt").isNull(), lit("null")).otherwise(col("prd_end_dt")))

    # Tạo ngày ngẫu nhiên
    random_date_start = change_date("2018-01-01", "2020-01-01")
    random_date_end = change_date("2022-01-01", "2023-01-01")

    df_prd = df_prd.withColumn("prd_start_dt", random_date_start())
    df_prd = df_prd.withColumn("prd_end_dt",
                               when(col("prd_end_dt") != "null", random_date_end())
                               .otherwise(lit("null").cast(DateType())))

    # Trích xuất màu sắc và kích thước
    df_prd = df_prd.withColumn(
        "color",
        when(
            regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$", 1) != "",
            regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$", 1)
        ).when(
            regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*$", 1) != "",
            regexp_extract(col("prd_nm"), "-\\s*([A-Za-z]+)\\s*$", 1)
        ).otherwise(lit("null"))
    )

    df_prd = df_prd.withColumn(
        "size",
        when(
            regexp_extract(col("prd_nm"), "-\\s*(\\d+|[A-Z]{1,3})\\s*$", 1) != "",
            regexp_extract(col("prd_nm"), "-\\s*(\\d+|[A-Z]{1,3})\\s*$", 1)
        ).otherwise(lit("null"))
    )

    # Xử lý trường hợp color trùng size
    df_prd = df_prd.withColumn(
        "color",
        expr("""
            CASE
                WHEN color = size AND size != 'null' THEN 'null'
                ELSE color
            END
        """)
    )

    # Chuẩn hóa tên sản phẩm
    df_prd = df_prd.withColumn(
        "prd_name",
        regexp_replace(
            col("prd_nm"),
            "-\\s*([A-Za-z]+)\\s*-\\s*(\\d+|[A-Z]{1,3})$|" +
            "-\\s*([A-Za-z]+)\\s*$|" +
            "-\\s*(\\d+|[A-Z]{1,3})\\s*$",
            ""
        )
    )

    # Chuẩn hóa prd_key
    df_prd = df_prd.withColumn(
        "prd_key",
        expr("""
            CASE
                WHEN ID IS NOT NULL THEN regexp_replace(prd_key, CONCAT('^', ID, '-'), '')
                ELSE prd_key
            END
        """)
    )

    return df_prd

if __name__ == "__main__":
    df = prd_data()
    df.show(5)
    df.describe().show()


