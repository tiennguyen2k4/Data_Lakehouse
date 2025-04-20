import psycopg2

postgres_config = {
    "host": "postgresql",
    "user": "datawarehouse",
    "password": "datawarehouse",
    "dbname": "datawarehouse"
}

TABLES = {
    "dim_customer": """
     CREATE TABLE IF NOT EXISTS dim_customer(
        cst_id INT PRIMARY KEY,
        crm_cst_key VARCHAR(255),
        erp_cst_key VARCHAR(255),
        full_name VARCHAR(255),
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        marital_status VARCHAR(255),
        gender VARCHAR(255),
        cst_birthday DATE,
        age INT,
        country VARCHAR(255),
        create_date DATE,
        cst_data_source VARCHAR(255)
    )
    """,
    "dim_product": """
    CREATE TABLE IF NOT EXISTS dim_product(
        prd_id INT PRIMARY KEY,
        cat_id VARCHAR(255),
        cat_name VARCHAR(255),
        subcat_name VARCHAR(255),
        prd_key VARCHAR(255),
        prd_name VARCHAR(255),
        color VARCHAR(255),
        size VARCHAR(255),
        prd_cost INT,
        prd_line VARCHAR(255),
        prd_start_dt DATE,  
        maintenance VARCHAR(255),
        prd_data_source VARCHAR(255)
    )
    """,
    "dim_date": """
    CREATE TABLE IF NOT EXISTS dim_date(
        date_key INT PRIMARY KEY,
        full_date DATE,
        day INT,
        month INT,
        year INT,
        weekday_name VARCHAR(255),
        weekday_number INT,
        quarter INT,
        month_year VARCHAR(255),
        is_weekend BOOLEAN
    )
    """,
    "fact_sales": """
        CREATE TABLE IF NOT EXISTS fact_sales(
            sls_ord_id VARCHAR(255) PRIMARY KEY,
            prd_id INT REFERENCES dim_product(prd_id),
            cst_id INT REFERENCES dim_customer(cst_id),
            sls_price INT,
            sls_quantity INT,
            sls_sales INT,
            sls_order_dt DATE,
            sls_date_id INT REFERENCES dim_date(date_key)
        )
    """
}

def create_table():
    try:
        with psycopg2.connect(**postgres_config) as conn:
            with conn.cursor() as cur:
                for table_name, ddl in TABLES.items():
                    cur.execute(ddl)
                    print(f"Create table: {table_name}")
                print("All table are created")
    except Exception as e:
        print(f"Error creating table: {e}")
        raise

if __name__ == "__main__":
    create_table()

