import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import logging

# --- Configuration ---
# Define the project root directory to build absolute paths
# Use environment variable if set (for Docker/Airflow), otherwise calculate from file location
PROJECT_ROOT = os.getenv('PROJECT_ROOT') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
logging.info(f"Project root directory set to: {PROJECT_ROOT}")

# Load environment variables from .env file
dotenv_path = os.path.join(PROJECT_ROOT, '.env')
load_dotenv(dotenv_path=dotenv_path)

# Setup basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

# --- File Paths ---
INPUT_DIR = os.path.join(PROJECT_ROOT, "Scenario_4")
JSON_INPUT_PATH = os.path.join(INPUT_DIR, "euromillions_draw_history_from_oracle.json")

SQL_SERVER_TABLE_NAME = "euromillions_draw_history_sqlserver"
SQL_SERVER_STAGING_TABLE_NAME = "euromillions_draw_history_sqlserver_staging"

def load_to_sql_server():
    """Loads the JSON data into a SQL Server table using PySpark."""
    logging.info("Starting process to load data into SQL Server...")

    # Pre-flight check: Ensure JAVA_HOME is set, as PySpark requires it.
    if not os.getenv('JAVA_HOME'):
        logging.error("JAVA_HOME environment variable is not set. PySpark requires Java to be installed and JAVA_HOME to be configured.")
        sys.exit(1)

    if not os.path.exists(JSON_INPUT_PATH):
        logging.error(f"Input file not found: {JSON_INPUT_PATH}")
        logging.error("Please run the export script first to generate the JSON file.")
        sys.exit(1)

    # --- SQL Server Connection Properties ---
    sql_server_jdbc_url = os.getenv("SQLSERVER_JDBC_URL")
    sql_server_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # Get the relative path from the environment variable and remove any leading slashes
    relative_jar_path = os.getenv("SQLSERVER_JAR_PATH", "").lstrip("/")
    # Construct the absolute path by joining with the project root
    sql_server_jar_path = os.path.join(PROJECT_ROOT, relative_jar_path)

    if not all([sql_server_jdbc_url, sql_server_jar_path]):
        logging.error("One or more SQL Server environment variables are not set. (SQLSERVER_JDBC_URL, SQLSERVER_JAR_PATH)")
        sys.exit(1)

    # Pre-flight check: Ensure the JDBC driver JAR exists before starting Spark.
    if not os.path.exists(sql_server_jar_path):
        logging.error(f"SQL Server JDBC driver not found at: {sql_server_jar_path}")
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

        # --- Upsert Logic using a Staging Table and MERGE statement ---

        # Step 1: Write the new data to a temporary staging table, always overwriting it.
        logging.info(f"Writing {df_new.count()} new/updated rows to staging table '{SQL_SERVER_STAGING_TABLE_NAME}'...")
        df_new.write.jdbc(
            url=sql_server_jdbc_url,
            table=SQL_SERVER_STAGING_TABLE_NAME,
            mode="overwrite",
            properties=connection_properties
        )

        # Step 2: Use a MERGE statement to upsert data from staging to the final table.
        # This is executed on the SQL Server side via a JDBC connection.
        merge_sql = f"""
        MERGE {SQL_SERVER_TABLE_NAME} AS Target
        USING {SQL_SERVER_STAGING_TABLE_NAME} AS Source
        ON Target.draw_date = Source.draw_date
        -- When records match, update the target table
        WHEN MATCHED THEN
            UPDATE SET
                Target.ball_1 = Source.ball_1,
                Target.ball_2 = Source.ball_2,
                Target.ball_3 = Source.ball_3,
                Target.ball_4 = Source.ball_4,
                Target.ball_5 = Source.ball_5,
                Target.lucky_star_1 = Source.lucky_star_1,
                Target.lucky_star_2 = Source.lucky_star_2,
                Target.jackpot = Source.jackpot,
                Target.winners = Source.winners
        -- When records don't match, insert the new record from the source
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (draw_date, ball_1, ball_2, ball_3, ball_4, ball_5, lucky_star_1, lucky_star_2, jackpot, winners)
            VALUES (Source.draw_date, Source.ball_1, Source.ball_2, Source.ball_3, Source.ball_4, Source.ball_5, Source.lucky_star_1, Source.lucky_star_2, Source.jackpot, Source.winners);
        """

        logging.info("Executing MERGE statement on SQL Server to perform upsert...")
        try:
            # We need a connection to execute a raw SQL statement.
            # PySpark's `write.jdbc` doesn't support executing arbitrary DML.
            from java.sql import DriverManager
            conn = DriverManager.getConnection(sql_server_jdbc_url, connection_properties['user'], connection_properties['password'])
            conn.createStatement().execute(merge_sql)
            conn.close()
            logging.info("MERGE operation completed successfully.")
        except Exception as e:
            logging.error(f"Failed to execute MERGE statement. This can happen on the first run if the target table '{SQL_SERVER_TABLE_NAME}' does not exist.")
            logging.error(f"Error details: {e}")
            logging.info("Attempting to write directly to the target table as a fallback for the first run.")
            # Fallback for the first run: if the target table doesn't exist, MERGE fails.
            # In this case, we just copy the staging data to the final table.
            df_new.write.jdbc(url=sql_server_jdbc_url, table=SQL_SERVER_TABLE_NAME, mode="append", properties=connection_properties)
            logging.info("Fallback write completed. The target table has been created.")

    except Exception as e:
        logging.error(f"An error occurred during the Spark job: {e}")
        sys.exit(1) # Ensure the script exits with a non-zero status on critical errors
    finally:
        # Stop Spark session
        logging.info("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    load_to_sql_server()
    logging.info("--- SQL Server Load Process Complete ---")