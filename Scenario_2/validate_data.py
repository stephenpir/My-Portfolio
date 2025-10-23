import pandas as pd
import oracledb
import os
import sys
from dotenv import load_dotenv
import logging
from sqlalchemy import create_engine
import warnings
import argparse
from sqlalchemy import text

# Define the project root directory to build absolute paths
# Use environment variable if set (for Docker/Airflow), otherwise calculate from file location
PROJECT_ROOT = os.getenv('PROJECT_ROOT') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables from the .env file in the project root
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, '.env'))

class TerminalFilter(logging.Filter):
    """A custom filter to prevent 'file_only' records from reaching the terminal."""
    def filter(self, record):
        # If the 'file_only' attribute is present and True, don't log to terminal.
        return not getattr(record, 'file_only', False)

def setup_logging():
    """Configures logging to output to both console and a file."""
    log_file = 'validation_report.log'
    
    # Create handlers
    file_handler = logging.FileHandler(log_file, mode='w')
    stream_handler = logging.StreamHandler(sys.stdout)
    
    # Add our custom filter to the terminal handler
    stream_handler.addFilter(TerminalFilter())
    
    # Configure root logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[file_handler, stream_handler]
    )
    
    # Suppress the noisy UserWarning from pandas about non-SQLAlchemy DBAPI2 objects
    # This is expected when using the raw oracledb connection object
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

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

def get_oracle_data():
    """Fetches euromillions data from the Oracle database."""
    logging.info("Connecting to Oracle DB...")
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
            # Fetch all relevant columns and ensure data types are consistent on load
            # Use a warnings context to specifically ignore the UserWarning from pandas
            # about using a non-SQLAlchemy connection object.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                df = pd.read_sql("SELECT * FROM EUROMILLIONS_DRAW_HISTORY", connection)
            logging.info(f"Fetched {len(df)} rows from Oracle.")
            return df
    except oracledb.Error as e:
        logging.error(f"Error connecting to or fetching from Oracle Database: {e}")
        return None

def get_postgres_data():
    """Fetches euromillions data from the PostgreSQL database."""
    logging.info("Connecting to PostgreSQL DB...")
    try:
        # Construct the full JDBC URL for SQLAlchemy
        pg_url = os.getenv("PG_JDBC_URL").replace("jdbc:", "")
        user = os.getenv("PG_USER")
        password = os.getenv("PG_PASSWORD")
        
        # The URL needs to be in the format: postgresql://user:password@host:port/dbname
        # The Aiven URL is in the format: postgresql://user:password@host:port/dbname?sslmode=require
        db_url = f"{pg_url.split('?')[0]}?sslmode=require".replace(f"//{user}:{password}@", "//")
        
        engine = create_engine(db_url, connect_args={'user': user, 'password': password})

        with engine.connect() as connection:
            logging.info("Successfully connected to PostgreSQL. Fetching data...")
            # Assuming the main table is 'euromillions_draw_history_pg' from load_source1
            table_name = "public.euromillions_draw_history_pg" # Use the table with the _pg suffix
            # Correctly quote the schema and table name to avoid "UndefinedTable" errors.
            schema, table = table_name.split('.')
            sql_query = f'SELECT * FROM "{schema}"."{table}"'
            # Use sqlalchemy.text() to ensure the raw SQL string is executable
            df = pd.read_sql(text(sql_query), connection)
            logging.info(f"Fetched {len(df)} rows from PostgreSQL table '{table_name}'.")
            return df
    except Exception as e:
        logging.error(f"Error connecting to or fetching from PostgreSQL: {e}")
        return None

def standardize_dataframe(df, source_name):
    """Standardizes column names and data types for comparison."""
    # Convert all column names to lowercase for consistency
    df.columns = [col.lower() for col in df.columns]

    # Ensure draw_date is a datetime object (without time)
    if 'draw_date' in df.columns:
        df['draw_date'] = pd.to_datetime(df['draw_date']).dt.normalize()
    else:
        raise KeyError(f"'draw_date' column not found in {source_name} data.")

    # Define the core columns for comparison
    core_columns = [
        'draw_date', 'draw_number', 'ball_1', 'ball_2', 'ball_3', 'ball_4',
        'ball_5', 'lucky_star_1', 'lucky_star_2'
    ]
    
    # Ensure all core columns exist
    for col in core_columns:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' not found in {source_name} data.")

    # Convert ball and star columns to a consistent integer type to avoid comparison issues
    for col in core_columns[1:]: # Skip 'draw_date'
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df[core_columns]

def main():
    """Main function to run the data validation."""
    # Step 1: Configure logging
    setup_logging()
    logging.info("--- Starting Data Validation ---")

    # --- Argument Parsing for Non-Interactive Mode (Airflow) ---
    parser = argparse.ArgumentParser(description="Run data validation between Oracle and PostgreSQL.")
    parser.add_argument('--show-details', action='store_true', help="Show detailed line-by-line results in the terminal.")
    args, unknown = parser.parse_known_args()

    show_details_in_terminal = False
    if args.show_details:
        show_details_in_terminal = True
    # Only prompt for input if running in an interactive terminal and the flag wasn't passed.
    # This prevents the EOFError in non-interactive environments like Airflow.
    elif sys.stdout.isatty():
        show_details_in_terminal = input("Show detailed line-by-line results in the terminal? (y/n): ").lower().strip() == 'y'

    if show_details_in_terminal:
        logging.info("Detailed results will be shown in the terminal and saved to validation_report.log.")
    else:
        logging.info("Progress will NOT be shown in the terminal. Detailed results will be saved to validation_report.log.")
    
    # Step 2: Initialize Oracle Client
    initialize_oracle_client()

    # Step 3: Fetch data from both databases
    oracle_df = get_oracle_data()
    postgres_df = get_postgres_data()

    if oracle_df is None or postgres_df is None:
        logging.error("Could not retrieve data from one or both databases. Exiting.")
        sys.exit(1)

    # Step 4: Standardize dataframes
    logging.info("Standardizing data for comparison...")
    try:
        oracle_std_df = standardize_dataframe(oracle_df, "Oracle")
        postgres_std_df = standardize_dataframe(postgres_df, "PostgreSQL")
    except KeyError as e:
        logging.error(f"Column error: {e}")
        sys.exit(1)

    # Step 5: Merge the dataframes on draw_date to find matches and differences
    logging.info("Comparing datasets by merging on 'draw_date'...")
    comparison_df = pd.merge(
        postgres_std_df,
        oracle_std_df,
        on='draw_date',
        suffixes=('_pg', '_ora'),
        how='outer',
        indicator=True
    )

    # --- Step 6: Analysis and Reporting ---
    logging.info("--- Validation Report ---")

    # Find rows only in PostgreSQL
    pg_only = comparison_df[comparison_df['_merge'] == 'left_only']
    if not pg_only.empty:
        pg_only_details = pg_only[['draw_date']].to_string(index=False)
        logging.info(f"Found {len(pg_only)} draws that exist ONLY in PostgreSQL.")
        logging.info("\n" + pg_only_details, extra={'file_only': not show_details_in_terminal})

    # Find rows only in Oracle
    ora_only = comparison_df[comparison_df['_merge'] == 'right_only']
    if not ora_only.empty:
        ora_only_details = ora_only[['draw_date']].to_string(index=False)
        logging.info(f"Found {len(ora_only)} draws that exist ONLY in Oracle.")
        logging.info("\n" + ora_only_details, extra={'file_only': not show_details_in_terminal})

    # Find rows in both and check for mismatches
    both_df = comparison_df[comparison_df['_merge'] == 'both'].copy()
    # Initialize mismatches as an empty DataFrame to prevent UnboundLocalError
    mismatches = pd.DataFrame()

    if not both_df.empty:
        logging.info(f"Found {len(both_df)} common draws. Now checking for data mismatches...")
        both_df['draw_number_match'] = (both_df['draw_number_pg'] == both_df['draw_number_ora'])
        # Compare each ball and lucky star column
        for i in range(1, 6): # Balls 1-5
            both_df[f'ball_{i}_match'] = (both_df[f'ball_{i}_pg'] == both_df[f'ball_{i}_ora'])
        for i in range(1, 3): # Lucky Stars 1-2
            both_df[f'lucky_star_{i}_match'] = (both_df[f'lucky_star_{i}_pg'] == both_df[f'lucky_star_{i}_ora'])
        
        match_cols = [col for col in both_df.columns if '_match' in col]
        mismatches = both_df[~both_df[match_cols].all(axis=1)]

    if not mismatches.empty:
        mismatch_display_cols = [
            'draw_date', 'draw_number_pg', 'draw_number_ora', 'ball_1_pg', 'ball_1_ora'
        ]
        mismatch_details = mismatches[mismatch_display_cols].to_string()
        logging.warning(f"Found {len(mismatches)} draws with mismatched data.")
        logging.warning("\n" + mismatch_details, extra={'file_only': not show_details_in_terminal})

    elif not both_df.empty:
        logging.info("SUCCESS: All common records have matching data.")
    
    logging.info("--- Validation Complete. Full report saved to validation_report.log ---")

if __name__ == "__main__":
    main()