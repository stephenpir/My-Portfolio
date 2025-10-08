import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import logging

# --- Configuration ---

# Load environment variables from .env file located in the parent directory
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# --- File Paths ---
INPUT_DIR = os.path.dirname(os.path.abspath(__file__))
JSON_INPUT_PATH = os.path.join(INPUT_DIR, "euromillions_draw_history_from_oracle.json")

# --- Constants ---
SQL_SERVER_TABLE_NAME = "euromillions_draw_history_sqlserver"

def load_to_sql_server():
    """Loads the JSON data into a SQL Server table using PySpark."""
    logging.info("Starting process to load data into SQL Server...")

    if not os.path.exists(JSON_INPUT_PATH):
        logging.error(f"Input file not found: {JSON_INPUT_PATH}")
        logging.error("Please run the export script first to generate the JSON file.")
        sys.exit(1)

    # --- SQL Server Connection Properties ---
    sql_server_jdbc_url = os.getenv("SQLSERVER_JDBC_URL")
    sql_server_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    sql_server_jar_path = os.getenv("SQLSERVER_JAR_PATH")

    if not all([sql_server_jdbc_url, sql_server_jar_path]):
        logging.error("One or more SQL Server environment variables are not set. (SQLSERVER_JDBC_URL, SQLSERVER_JAR_PATH)")
        sys.exit(1)

    connection_properties = {
        "user": os.getenv("SQLSERVER_USER"),
        "password": os.getenv("SQLSERVER_PASSWORD"),
        "driver": sql_server_driver
    }

    # Initialize Spark session with the SQL Server JDBC driver
    logging.info("Initializing Spark session...")
    spark = SparkSession.builder \
        .appName("JsonToSQLServerLoader") \
        .config("spark.jars", sql_server_jar_path) \
        .getOrCreate()

    try:
        # Read the JSON file into a Spark DataFrame
        logging.info(f"Reading JSON data from {JSON_INPUT_PATH}")
        df_spark = spark.read.json(JSON_INPUT_PATH)

        # Ensure column names are SQL Server compatible (e.g., lowercase)
        df_spark = df_spark.toDF(*[c.lower() for c in df_spark.columns])

        logging.info("Schema of the DataFrame to be loaded:")
        df_spark.printSchema()

        # --- Conditional Table Creation and Load ---
        # Check if the table exists. If it does, we'll append. If not, we'll create it by overwriting.
        try:
            # Attempt a read to see if the table exists. This is a common JDBC pattern.
            spark.read.jdbc(url=sql_server_jdbc_url, table=SQL_SERVER_TABLE_NAME, properties=connection_properties).limit(1).collect()
            logging.info(f"Table '{SQL_SERVER_TABLE_NAME}' already exists. Appending data.")
            write_mode = "append"
        except Exception as e:
            # The read fails if the table doesn't exist.
            logging.info(f"Table '{SQL_SERVER_TABLE_NAME}' does not exist or is not accessible. Attempting to create it.")
            logging.debug(f"Underlying error: {e}") # Log the actual error for debugging
            # 'overwrite' will create the table and write the data in one step.
            write_mode = "overwrite"

        # Write the DataFrame to the SQL Server table using the determined mode.
        if write_mode == "overwrite":
            logging.info(f"Creating table '{SQL_SERVER_TABLE_NAME}' and writing {df_spark.count()} rows...")
        else:
            logging.info(f"Appending {df_spark.count()} rows to table '{SQL_SERVER_TABLE_NAME}'...")

        df_spark.write.jdbc(
            url=sql_server_jdbc_url,
            table=SQL_SERVER_TABLE_NAME,
            mode=write_mode,
            properties=connection_properties
        )
        logging.info("Data loaded successfully into SQL Server.")

    except Exception as e:
        logging.error(f"An error occurred during the Spark job: {e}")
    finally:
        # Stop Spark session
        logging.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    load_to_sql_server()
    logging.info("--- SQL Server Load Process Complete ---")