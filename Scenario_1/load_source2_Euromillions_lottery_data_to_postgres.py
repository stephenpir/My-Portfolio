from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
from dotenv import load_dotenv
import os

def main():
    """
    Main function to initialize Spark, read lottery data from CSV files,
    and write it to a PostgreSQL database.
    """
    # Initialize Spark session with the PostgreSQL JDBC driver
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

    spark = SparkSession.builder \
        .appName("PostgresLotteryDataLoad") \
        .config("spark.jars", "/Applications/Drivers/postgresql-42.7.8.jar") \
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

    # --- File Paths and Table Name ---
    historical_csv_path = "/Users/stephenpir/Desktop/Code/My Portfolio/Scenario_1/Data/EuroMillions_numbers_20040110-20211201.csv"  
    euromillions_table = "public.euromillions_draw_history_pg"

    # Read the CSV with semicolon delimiter, infer schema, and correct the date format
    df = spark.read.csv(historical_csv_path, header=True, inferSchema=True, sep=";")
    df = df.drop("Winner", "Gain")  # Drop the extra columns not in the first CSV load
    df = df.withColumn(
        "Date",
        to_date(col("Date"), "yyyy-MM-dd")
    )
    # Add a source_id column to identify the data source
    df = df.withColumn("load_number", lit(2))

    # Rename columns to match the database table schema
    df_renamed = df.select(
        col("Date").alias("draw_date"),
        col("N1").alias("ball_1"), col("N2").alias("ball_2"), col("N3").alias("ball_3"),
        col("N4").alias("ball_4"), col("N5").alias("ball_5"),
        col("E1").alias("lucky_star_1"), col("E2").alias("lucky_star_2"),
        col("load_number")
    )

    # Write data to the PostgreSQL table
    df_renamed.write.jdbc(url=jdbc_url, table=euromillions_table, mode="append", properties=connection_properties)
    print(f"Successfully loaded historical data into {euromillions_table}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()