from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

def get_spark_session(app_name):
    load_dotenv(dotenv_path="/app/minio.env")

    print("[DEBUG] MINIO_ACCESS_KEY =", os.getenv("MINIO_ACCESS_KEY"))  
    print("[DEBUG] MINIO_SECRET_KEY =", os.getenv("MINIO_SECRET_KEY"))

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
        .getOrCreate()
    return spark