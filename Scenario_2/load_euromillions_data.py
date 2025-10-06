import oracledb
import pandas as pd
import os
import sys
from dotenv import load_dotenv

# Load environment variables from .env file located in the parent directory
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

class Config:
    """Centralized configuration for database and file paths."""
    # --- Database Credentials ---
    DB_USER = os.getenv("ORACLE_DB_USER")
    DB_PASSWORD = os.getenv("ORACLE_DB_PASSWORD")
    WALLET_PASSWORD = os.getenv("ORACLE_WALLET_PASSWORD")

    # --- Connection & Client Paths ---
    # TNS alias from tnsnames.ora in your wallet
    DB_DSN = os.getenv("ORACLE_DB_DSN")
    # Absolute path to the unzipped Oracle Instant Client directory
    INSTANT_CLIENT_PATH = os.getenv("ORACLE_INSTANT_CLIENT_PATH")
    # Absolute path to the unzipped Oracle Wallet directory
    WALLET_PATH = os.getenv("ORACLE_WALLET_PATH")

    # --- File Paths ---
    CSV_FILE_PATH = "/Users/stephenpir/Desktop/Code/My Portfolio/Scenario_2/euromillions_draw_history_scraped.csv"

def initialize_oracle_client():
    """
    Initializes the Oracle Client in Thick mode. Must be called once at startup.
    Returns True on success, False on failure.
    """
    try:
        oracledb.init_oracle_client(lib_dir=Config.INSTANT_CLIENT_PATH, config_dir=Config.WALLET_PATH)
        return True
    except oracledb.Error as e:
        print(f"Error initializing Oracle Client: {e}", file=sys.stderr)
        print("Please check the following:", file=sys.stderr)
        print(f"1. The Instant Client path is correct: '{Config.INSTANT_CLIENT_PATH}'", file=sys.stderr)
        print("2. You have downloaded the correct 'Intel (x86)' version for your Anaconda environment.", file=sys.stderr)
        print("3. You have run 'sudo xattr -r -d com.apple.quarantine ...' on the Instant Client directory.", file=sys.stderr)
        return False

def create_connection():
    """
    Creates and returns a connection to the Oracle database.
    Assumes the Oracle client has already been initialized.
    """
    try:
        connection = oracledb.connect(
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            dsn=Config.DB_DSN,
            wallet_password=Config.WALLET_PASSWORD,
            config_dir=Config.WALLET_PATH,
            wallet_location=Config.WALLET_PATH
        )
        print("Successfully connected to Oracle Database")
        return connection
    except oracledb.Error as e:
        print(f"Error connecting to Oracle Database: {e}", file=sys.stderr)
        return None

def process_and_load_data(connection):
    """
    Loads data from a CSV file into the EUROMILLIONS_DRAW_HISTORY table.
    """
    try:
        print(f"Reading data from {Config.CSV_FILE_PATH}...")
        df = pd.read_csv(Config.CSV_FILE_PATH, dtype=str, na_values=[''])

        print("Processing data...")
        df['draw_date'] = pd.to_datetime(df['draw_date'])
        df = df.sort_values(by='draw_date', ascending=True).reset_index(drop=True)
        df['draw_number'] = df.index + 1
        df = df.where(pd.notna(df), None)

        data_to_insert = [
            (
                row['draw_number'], row['draw_date'],
                row['ball_1'], row['ball_2'], row['ball_3'], row['ball_4'], row['ball_5'],
                row['lucky_star_1'], row['lucky_star_2'],
                row['jackpot'], row['winners']
            )
            for _, row in df.iterrows()
        ]

        with connection.cursor() as cursor:
            # Optional: Truncate the table to prevent duplicate data on re-runs
            # print("Truncating table EUROMILLIONS_DRAW_HISTORY...")
            # cursor.execute("TRUNCATE TABLE EUROMILLIONS_DRAW_HISTORY")

            sql = """
                INSERT INTO EUROMILLIONS_DRAW_HISTORY (
                    draw_number, draw_date, ball_1, ball_2, ball_3, ball_4, ball_5,
                    lucky_star_1, lucky_star_2, jackpot, winners
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
            """
            print(f"Inserting {len(data_to_insert)} rows into the database...")
            cursor.executemany(sql, data_to_insert, batcherrors=True)

            # Check for batch errors
            for error in cursor.getbatcherrors():
                print(f"Error inserting row {error.offset}: {error.message}", file=sys.stderr)

            connection.commit()
            print(f"Successfully committed {cursor.rowcount} rows to EUROMILLIONS_DRAW_HISTORY.")

    except FileNotFoundError:
        print(f"Error: The file was not found at {Config.CSV_FILE_PATH}", file=sys.stderr)
    except pd.errors.EmptyDataError:
        print(f"Error: The CSV file at {Config.CSV_FILE_PATH} is empty.", file=sys.stderr)
    except oracledb.Error as e:
        print(f"A database error occurred: {e}", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)

def main():
    """Main execution function."""
    # Step 1: Initialize the Oracle Client. Exit if it fails.
    if not initialize_oracle_client():
        sys.exit(1)

    # Step 2: Create a database connection.
    connection = None
    try:
        connection = create_connection()
        if connection:
            # Step 3: Process the CSV and load data into the database.
            process_and_load_data(connection)
    finally:
        # Step 4: Ensure the connection is closed.
        if connection:
            connection.close()
            print("Oracle connection closed.")

if __name__ == "__main__":
    main()