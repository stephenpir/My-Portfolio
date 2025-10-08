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
        df_new = spark.read.json(JSON_INPUT_PATH)

        # Ensure column names are SQL Server compatible (e.g., lowercase)
        df_new = df_new.toDF(*[c.lower() for c in df_new.columns])

        logging.info("Schema of the new DataFrame to be loaded:")
        df_new.printSchema()

        # --- Spark-Side Upsert Logic (No Temp Table) ---

        # Step 1: Read existing data from SQL Server.
        # If the table doesn't exist, this will raise an exception,
        # and we'll handle it by creating an empty DataFrame.
        try:
            logging.info(f"Reading existing data from SQL Server table '{SQL_SERVER_TABLE_NAME}'...")
            df_existing = spark.read.jdbc(
                url=sql_server_jdbc_url,
                table=SQL_SERVER_TABLE_NAME,
                properties=connection_properties
            )
            logging.info(f"Found {df_existing.count()} existing rows.")
        except Exception as e:
            logging.warning(f"Could not read from '{SQL_SERVER_TABLE_NAME}'. Assuming it's the first run. Details: {e}")
            # Create an empty DataFrame with the same schema as the new data
            df_existing = spark.createDataFrame([], df_new.schema)

        # Step 2: Perform a full outer join on the primary key (draw_date).
        # We alias the dataframes to avoid column ambiguity issues.
        join_condition = df_new.alias("n").draw_date == df_existing.alias("e").draw_date
        df_joined = df_new.alias("n").join(df_existing.alias("e"), join_condition, "full_outer")

        # Step 3: Merge the columns using coalesce.
        # coalesce(col1, col2) returns the first non-null value.
        # This ensures that new/updated data from df_new is prioritized.
        from pyspark.sql.functions import col, coalesce
        select_expr = [
            coalesce(col(f"n.{c}"), col(f"e.{c}")).alias(c) for c in df_new.columns
        ]
        df_final = df_joined.select(select_expr)

        # Step 4: Overwrite the target table with the final merged data.
        # This operation is not atomic and will drop/recreate the table.
        logging.info(f"Writing {df_final.count()} rows to '{SQL_SERVER_TABLE_NAME}' with mode 'overwrite'...")
        df_final.write.jdbc(
            url=sql_server_jdbc_url,
            table=SQL_SERVER_TABLE_NAME,
            mode="overwrite",
            properties=connection_properties
        )
        logging.info("Data write operation completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the Spark job: {e}")
    finally:
        # Stop Spark session
        logging.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    load_to_sql_server()
    logging.info("--- SQL Server Load Process Complete ---")