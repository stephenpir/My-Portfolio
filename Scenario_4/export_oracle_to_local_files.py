import oracledb
import pandas as pd
import os
import sys
from dotenv import load_dotenv
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
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "euromillions_draw_history_from_oracle.csv")
JSON_OUTPUT_PATH = os.path.join(OUTPUT_DIR, "euromillions_draw_history_from_oracle.json")

# --- Functions ---

def initialize_oracle_client():
    """Initializes the Oracle Client. Exits on failure."""
    lib_dir = os.getenv("ORACLE_INSTANT_CLIENT_PATH")
    config_dir = os.getenv("ORACLE_WALLET_PATH")
    if not lib_dir or not config_dir:
        logging.error("ORACLE_INSTANT_CLIENT_PATH or ORACLE_WALLET_PATH not set in .env file.")
        sys.exit(1)
    try:
        oracledb.init_oracle_client(lib_dir=lib_dir, config_dir=config_dir)
        logging.info("Oracle Client initialized successfully.")
        return True
    except oracledb.Error as e:
        logging.error(f"Error initializing Oracle Client: {e}")
        logging.error("Please check your Oracle Instant Client and Wallet paths in the .env file.")
        sys.exit(1)

def extract_from_oracle():
    """Fetches euromillions data from the Oracle database and returns a pandas DataFrame."""
    logging.info("Connecting to Oracle DB to extract data...")
    try:
        with oracledb.connect(
            user=os.getenv("ORACLE_DB_USER"),
            password=os.getenv("ORACLE_DB_PASSWORD"),
            dsn=os.getenv("ORACLE_DB_DSN"),
            wallet_password=os.getenv("ORACLE_WALLET_PASSWORD"),
            config_dir=os.getenv("ORACLE_WALLET_PATH"),
            wallet_location=os.getenv("ORACLE_WALLET_PATH")
        ) as connection:
            logging.info("Successfully connected to Oracle. Fetching data...")
            df = pd.read_sql("SELECT * FROM EUROMILLIONS_DRAW_HISTORY ORDER BY DRAW_DATE ASC", connection)
            logging.info(f"Successfully fetched {len(df)} rows from Oracle.")
            return df
    except oracledb.Error as e:
        logging.error(f"Error connecting to or fetching from Oracle Database: {e}")
        return None

def save_data_to_files(df):
    """Saves the DataFrame to both CSV and JSON file formats."""
    if df is None or df.empty:
        logging.warning("DataFrame is empty. No files will be saved.")
        return

    logging.info(f"Saving data to CSV file: {CSV_OUTPUT_PATH}")
    df.to_csv(CSV_OUTPUT_PATH, index=False, date_format='%Y-%m-%d %H:%M:%S')
    logging.info("CSV file saved successfully.")

    logging.info(f"Saving data to JSON file: {JSON_OUTPUT_PATH}")
    df.to_json(JSON_OUTPUT_PATH, orient='records', lines=True, date_format='iso')
    logging.info("JSON file saved successfully.")

def main():
    """Main execution function."""
    initialize_oracle_client()
    oracle_df = extract_from_oracle()
    if oracle_df is not None:
        save_data_to_files(oracle_df)
    else:
        logging.error("Failed to extract data from Oracle. Exiting.")
        sys.exit(1)
    logging.info("--- Data Export Process Complete ---")

if __name__ == "__main__":
    main()