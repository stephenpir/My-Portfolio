from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from dotenv import load_dotenv
import os
import logging

def main():
    """
    Main function to initialize Spark, read lottery data from CSV files,
    and write it to a PostgreSQL database.
    """
    
    # --- Configuration ---
    # Define the project root directory to build absolute paths
    # Use environment variable if set (for Docker/Airflow), otherwise calculate from file location
    PROJECT_ROOT = os.getenv('PROJECT_ROOT') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    logging.info(f"Project root directory set to: {PROJECT_ROOT}")

    # Load environment variables from .env file
    dotenv_path = os.path.join(PROJECT_ROOT, '.env')
    load_dotenv(dotenv_path=dotenv_path)

    # Get the relative path from the environment variable and remove any leading slashes
    relative_jar_path = os.getenv("PG_JAR_PATH", "").lstrip("/")
    # Construct the absolute path by joining with the project root
    pg_jar_path = os.path.join(PROJECT_ROOT, relative_jar_path)


    # Initialize Spark session with the PostgreSQL JDBC driver
    spark = SparkSession.builder \
        .appName("PostgresLotteryDataLoad") \
        .config("spark.jars", pg_jar_path) \
        .getOrCreate()

    # --- Connection Properties ---
    # Load connection details from environment variables
    jdbc_url = os.getenv("PG_JDBC_URL")
    connection_properties = {
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "driver": "org.postgresql.Driver",
        "sslmode": "require"
    }

    # --- File Paths ---
    INPUT_DIR = os.path.join(PROJECT_ROOT, "Scenario_1/Data")

    # --- Process EuroMillions Data ---
    #euromillions_csv_path_1 = "/Users/stephenpir/Desktop/Code/My Portfolio/Scenario_1/Data/euromillions-draw-history_20210416-20211012.csv"
    #euromillions_csv_path_2 = "/Users/stephenpir/Desktop/Code/My Portfolio/Scenario_1/Data/euromillions-draw-history_20221223-20230616.csv"
    euromillions_csv_path_1 = os.path.join(INPUT_DIR, "euromillions-draw-history_20210416-20211012.csv")
    euromillions_csv_path_2 = os.path.join(INPUT_DIR, "euromillions-draw-history_20221223-20230616.csv")
    euromillions_table = "public.euromillions_draw_history_pg"

    # Read the CSVs, infer schema, union the data into one df and correct the date format
    euromillions_df1 = spark.read.csv(euromillions_csv_path_1, header=True, inferSchema=True)
    euromillions_df2 = spark.read.csv(euromillions_csv_path_2, header=True, inferSchema=True)
    euromillions_df2 = euromillions_df2.drop("European Millionaire Maker")  # Drop the extra column not in the first CSV
    euromillions_df = euromillions_df1.union(euromillions_df2)
    euromillions_df = euromillions_df.withColumn(
        "DrawDate",
        to_date(col("DrawDate"), "dd-MMM-yyyy")
    )

    # Rename columns to match the database table schema (lowercase with underscores)
    euromillions_df_renamed = euromillions_df.withColumnRenamed("DrawDate", "draw_date") \
        .withColumnRenamed("Ball 1", "ball_1") \
        .withColumnRenamed("Ball 2", "ball_2") \
        .withColumnRenamed("Ball 3", "ball_3") \
        .withColumnRenamed("Ball 4", "ball_4") \
        .withColumnRenamed("Ball 5", "ball_5") \
        .withColumnRenamed("Lucky Star 1", "lucky_star_1") \
        .withColumnRenamed("Lucky Star 2", "lucky_star_2") \
        .withColumnRenamed("UK Millionaire Maker", "uk_millionaire_maker") \
        .withColumnRenamed("DrawNumber", "draw_number")

    # Write data to the PostgreSQL table
    euromillions_df_renamed.write.jdbc(url=jdbc_url, table=euromillions_table, mode="append", properties=connection_properties)
    print(f"Successfully loaded data into {euromillions_table}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()