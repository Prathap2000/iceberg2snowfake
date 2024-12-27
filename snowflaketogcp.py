import os
import tkinter as tk
from tkinter import messagebox, filedialog
from pyspark.sql import SparkSession
import snowflake.connector
from urllib.parse import urlparse, parse_qs
from tkinter import PhotoImage
from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, BooleanType, DecimalType, DateType, TimestampType, FloatType, StructType
from pyspark.sql.types import StructField
import threading
import time

# Function to get Snowflake credentials from UI
def get_snowflake_credentials(account, warehouse, database, schema, user, password):
    return account, warehouse, database, schema, user, password

# Function to get Snowflake schema dynamically using values from UI
def get_snowflake_schema(account, warehouse, database, schema, user, password, table_name):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE TABLE {table_name}")
    schema = cursor.fetchall()
    cursor.close()
    conn.close()

    # Define the schema based on Snowflake data types
    fields = []
    for column, data_type, *_ in schema:  # Ignore the additional metadata columns
        if data_type == 'STRING' or data_type == 'VARCHAR':
            spark_type = StringType()
        elif data_type == 'NUMBER' or data_type == 'DECIMAL':
            spark_type = DecimalType()
        elif data_type == 'INTEGER':
            spark_type = IntegerType()
        elif data_type == 'BIGINT':
            spark_type = LongType()
        elif data_type == 'BOOLEAN':
            spark_type = BooleanType()
        elif data_type == 'DATE':
            spark_type = DateType()
        elif data_type == 'TIMESTAMP':
            spark_type = TimestampType()
        elif data_type == 'FLOAT':
            spark_type = FloatType()
        else:
            spark_type = StringType()  # Default to StringType if no other mapping is found

        fields.append(StructField(column, spark_type))

    return StructType(fields)

# Function to get Snowflake tables dynamically using values from UI
def get_snowflake_tables(account, warehouse, database, schema, user, password):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    cursor.close()
    conn.close()

    return [table[1] for table in tables]

# Function to initialize Spark session
def initialize_spark(snowflake_url, gcs_bucket_url, gcp_access_key):
    iceberg_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "iceberg-spark-runtime-3.5_2.12-1.7.0.jar")
    snowflake_jdbc_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "snowflake-jdbc-3.14.4.jar")
    gcs_connector_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "gcs-connector-hadoop2-latest.jar")
    snowflake_connector_jar_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark-snowflake_2.12-2.14.0-spark_3.4.jar")  # Add Snowflake Spark connector

    spark = SparkSession.builder \
        .appName("Snowflake to Iceberg Converter") \
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.my_catalog.type", "hadoop") \
        .config("spark.sql.catalog.my_catalog.warehouse", f"gs://{gcs_bucket_url}") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.fs.gs.auth.service.account.json.keyfile", gcp_access_key) \
        .config("spark.jars", iceberg_jar_path + "," + snowflake_jdbc_jar_path + "," + gcs_connector_jar_path + "," + snowflake_connector_jar_path) \
        .getOrCreate()

    return spark

# Function to handle conversion
def convert_tables_to_iceberg(snowflake_url, gcs_bucket_url, gcp_access_key, log_widget, account, warehouse, database, schema, user, password):
    spark = initialize_spark(snowflake_url, gcs_bucket_url, gcp_access_key)
    
    tables = get_snowflake_tables(account, warehouse, database, schema, user, password)
    
    for table_name in tables:
        try:
            root.after(0, update_log_widget, f"Processing table: {table_name}\n")
            snowflake_schema = get_snowflake_schema(account, warehouse, database, schema, user, password, table_name)
            
            df = spark.read \
                .format("snowflake") \
                .options(
                    sfURL=snowflake_url,
                    sfDatabase=database,
                    sfSchema=schema,
                    sfWarehouse=warehouse,
                    sfRole="PUBLIC",
                    sfUser=user,
                    sfPassword=password
                ) \
                .option("dbtable", table_name) \
                .load()
            
            df.printSchema()

            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS my_catalog.default.{table_name} (
                    {', '.join([f"{col.name} {col.dataType.simpleString()}" for col in snowflake_schema.fields])}
                )
                USING iceberg
            """
            spark.sql(create_table_sql)

            df.write \
                .format("iceberg") \
                .mode("overwrite") \
                .save(f"my_catalog.default.{table_name}")

            root.after(0, update_log_widget, f"Table {table_name} successfully converted to Iceberg format!\n")
        
        except Exception as e:
            root.after(0, update_log_widget, f"Error processing table {table_name}: {str(e)}\n")
    
    spark.stop()
    messagebox.showinfo("Conversion Complete", "All tables have been successfully converted to Iceberg format!")

# Function to handle button click for conversion
def on_convert_button_click():
    # Extract values from the UI dynamically
    snowflake_account = entry_snowflake_account.get()
    snowflake_user = entry_snowflake_user.get()
    snowflake_password = entry_snowflake_password.get()
    snowflake_database = entry_snowflake_database.get()
    snowflake_schema = entry_snowflake_schema.get()
    snowflake_warehouse = entry_snowflake_warehouse.get()
    gcs_bucket_name = entry_gcs_bucket_name.get()
    json_key_file = entry_json_key_file.get()

    if not (snowflake_account and snowflake_user and snowflake_password and snowflake_database and snowflake_schema and snowflake_warehouse and gcs_bucket_name and json_key_file):
        messagebox.showerror("Input Error", "Please fill in all fields.")
        return
    
    # Construct Snowflake URL dynamically
    snowflake_url = f"{snowflake_account}.snowflakecomputing.com"
    
    log_widget.delete(1.0, tk.END)  # Clear previous log messages
    log_widget.insert(tk.END, "Starting conversion...\n")
    log_widget.yview(tk.END)
    
    try:
        # Run the conversion function in a separate thread to avoid blocking the UI
        conversion_thread = threading.Thread(target=convert_tables_to_iceberg, args=(snowflake_url, gcs_bucket_name, json_key_file, log_widget, snowflake_account, snowflake_warehouse, snowflake_database, snowflake_schema, snowflake_user, snowflake_password))
        conversion_thread.start()
    except Exception as e:
        messagebox.showerror("Error", f"Error occurred while starting conversion: {str(e)}")

# Function to update log widget in thread-safe way
def update_log_widget(message):
    log_widget.insert(tk.END, message)
    log_widget.yview(tk.END)
    log_widget.update_idletasks()

# UI Setup
root = tk.Tk()
root.title("Snowflake to Iceberg Converter")

# Set fixed window size and disable resizing
root.geometry("1000x650")  # Adjusted window size for better layout
root.resizable(False, False)

# Window grid configuration
root.grid_rowconfigure(0, weight=1)
root.grid_rowconfigure(1, weight=2)
root.grid_rowconfigure(2, weight=1)
root.grid_rowconfigure(3, weight=1)
root.grid_rowconfigure(4, weight=0)  # Footer row with weight 0
root.grid_columnconfigure(0, weight=1)

# Snowflake Credentials UI
label_snowflake_account = tk.Label(root, text="Snowflake Account")
label_snowflake_account.grid(row=0, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_account = tk.Entry(root, width=50)
entry_snowflake_account.grid(row=0, column=1, padx=10, pady=5)

label_snowflake_user = tk.Label(root, text="Snowflake User")
label_snowflake_user.grid(row=1, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_user = tk.Entry(root, width=50)
entry_snowflake_user.grid(row=1, column=1, padx=10, pady=5)

label_snowflake_password = tk.Label(root, text="Snowflake Password")
label_snowflake_password.grid(row=2, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_password = tk.Entry(root, show="*", width=50)
entry_snowflake_password.grid(row=2, column=1, padx=10, pady=5)

label_snowflake_database = tk.Label(root, text="Snowflake Database")
label_snowflake_database.grid(row=3, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_database = tk.Entry(root, width=50)
entry_snowflake_database.grid(row=3, column=1, padx=10, pady=5)

label_snowflake_schema = tk.Label(root, text="Snowflake Schema")
label_snowflake_schema.grid(row=4, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_schema = tk.Entry(root, width=50)
entry_snowflake_schema.grid(row=4, column=1, padx=10, pady=5)

label_snowflake_warehouse = tk.Label(root, text="Snowflake Warehouse")
label_snowflake_warehouse.grid(row=5, column=0, sticky=tk.W, padx=10, pady=5)
entry_snowflake_warehouse = tk.Entry(root, width=50)
entry_snowflake_warehouse.grid(row=5, column=1, padx=10, pady=5)

# Google Cloud Storage Credentials UI
label_gcs_bucket_name = tk.Label(root, text="GCS Bucket Name")
label_gcs_bucket_name.grid(row=6, column=0, sticky=tk.W, padx=10, pady=5)
entry_gcs_bucket_name = tk.Entry(root, width=50)
entry_gcs_bucket_name.grid(row=6, column=1, padx=10, pady=5)

label_json_key_file = tk.Label(root, text="Google Cloud Key File")
label_json_key_file.grid(row=7, column=0, sticky=tk.W, padx=10, pady=5)
entry_json_key_file = tk.Entry(root, width=50)
entry_json_key_file.grid(row=7, column=1, padx=10, pady=5)

# Log UI
label_log = tk.Label(root, text="Conversion Log")
label_log.grid(row=8, column=0, sticky=tk.W, padx=10, pady=5)
log_widget = tk.Text(root, height=10, width=100)
log_widget.grid(row=9, column=0, columnspan=2, padx=10, pady=5)

# Convert Button
convert_button = tk.Button(root, text="Start Conversion", command=on_convert_button_click, height=2, width=20)
convert_button.grid(row=10, column=0, columnspan=2, padx=10, pady=10)

root.mainloop()
