import oracledb
import pandas as pd
import os
import sys
from dotenv import load_dotenv
import logging
import warnings

# --- Configuration ---
# Define the project root directory to build absolute paths
# Use environment variable if set (for Docker/Airflow), otherwise calculate from file location
PROJECT_ROOT = os.getenv('PROJECT_ROOT') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables from .env file located in the parent directory
dotenv_path = os.path.join(PROJECT_ROOT, '.env')
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
    try:
        client_dir_name = os.getenv("ORACLE_INSTANT_CLIENT_DIR_NAME")
        wallet_dir_name = os.getenv("ORACLE_WALLET_DIR_NAME")

        if not all([client_dir_name, wallet_dir_name]):
            logging.error("Oracle directory names are not set in the environment.")
            logging.error("Please ensure ORACLE_INSTANT_CLIENT_DIR_NAME and ORACLE_WALLET_DIR_NAME are in your .env file or environment variables.")
            sys.exit(1)

        client_path = os.path.join(PROJECT_ROOT, client_dir_name)
        wallet_path = os.path.join(PROJECT_ROOT, wallet_dir_name)

        oracledb.init_oracle_client(lib_dir=client_path, config_dir=wallet_path)
        logging.info("Oracle Client initialized successfully.")
    except oracledb.Error as e:
        logging.error(f"Error initializing Oracle Client: {e}")
        logging.error("Please check that ORACLE_INSTANT_CLIENT_DIR_NAME and ORACLE_WALLET_DIR_NAME in your .env file point to the correct directory names within your project.")
        sys.exit(1)

def extract_from_oracle():
    """Fetches euromillions data from the Oracle database and returns a pandas DataFrame."""
    logging.info("Connecting to Oracle DB to extract data...")
    try:
        wallet_path = os.path.join(PROJECT_ROOT, os.getenv("ORACLE_WALLET_DIR_NAME"))
        with oracledb.connect(
            user=os.getenv("ORACLE_DB_USER"),
            password=os.getenv("ORACLE_DB_PASSWORD"),
            dsn=os.getenv("ORACLE_DB_DSN"),
            wallet_password=os.getenv("ORACLE_WALLET_PASSWORD"),
            config_dir=wallet_path,
            wallet_location=wallet_path
        ) as connection:
            logging.info("Successfully connected to Oracle. Fetching data...")
            # Use a warnings context to specifically ignore the UserWarning from pandas
            # about using a non-SQLAlchemy connection object.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
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