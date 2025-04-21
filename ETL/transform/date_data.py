from ETL.spark_session import get_spark_session
from pyspark.sql.functions import col, expr, dayofmonth, month, year, quarter, to_date, date_format
from datetime import datetime, timedelta

def generate_date_range(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return [(start + timedelta(days=i),) for i in range((end - start).days + 1)]

# config SparkSession
spark = get_spark_session("date_data")

# Create date range from 2017-01-01 to 2024-12-31
date_list = generate_date_range("2017-01-01", "2024-12-31")
df_date = spark.createDataFrame(date_list, ["date"])

# Create date dimension
dim_date = df_date.select(
    expr("CAST(date_format(date, 'yyyyMMdd') AS INT)").alias("date_key"),
    to_date(col("date"), "yyyy-MM-dd").alias("full_date"),
    dayofmonth("date").alias("day"),
    month("date").alias("month"),
    year("date").alias("year"),
    date_format("date", "EEEE").alias("weekday_name"),  
    expr("DAYOFWEEK(date)").alias("weekday_number"),    
    quarter("date").alias("quarter"),
    expr("MONTH(date) || '-' || YEAR(date)").alias("month_year"),
    expr("DAYOFWEEK(date) IN (1, 7)").alias("is_weekend")  
)

# Check data
assert dim_date.filter(col("date_key").isNull()).count() == 0, "Lỗi: date_key bị null"
assert dim_date.count() == (datetime(2024, 12, 31) - datetime(2017, 1, 1)).days + 1, "Lỗi: Thiếu ngày"

# Write to silver layer
dim_date.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://data/silver/dim_date")
    
print("Successfully")

spark.stop()