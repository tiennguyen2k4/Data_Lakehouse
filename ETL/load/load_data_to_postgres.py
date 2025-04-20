from pyspark.sql import SparkSession

postgres_config = {
    "url": "jdbc:postgresql://postgresql:5432/datawarehouse",
    "user": "datawarehouse",
    "password": "datawarehouse",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
            .appName("load_data_to_postgres") \
            .getOrCreate()

def load_data(spark, table_name, silver_path):
    try:
        df = spark.read.parquet(f"{silver_path}/{table_name}")

        df.write \
            .format("jdbc") \
            .option("url", postgres_config["url"]) \
            .option("dbtable", table_name) \
            .option("user", postgres_config["user"]) \
            .option("password", postgres_config["password"]) \
            .option("driver", postgres_config["driver"]) \
            .mode("append") \
            .save()
        print(f"Successfully loaded {table_name}")
    except Exception as e:
        print(f"Error loading {table_name}: {e}")
        raise

def main():
    spark = create_spark_session()

    silver_path = "/app/data/silver"
    TABLES = ["dim_customer", "dim_date", "dim_product", "fact_sales"]

    for table_name in TABLES:
        load_data(spark, table_name, silver_path)

    spark.stop()

if __name__ == "__main__":
    main()

