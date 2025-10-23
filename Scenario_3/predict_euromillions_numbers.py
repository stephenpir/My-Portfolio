import pandas as pd
import oracledb
import os
import sys
import random
from dotenv import load_dotenv
import argparse
import logging
import warnings

# Define the project root directory to build absolute paths
# Use environment variable if set (for Docker/Airflow), otherwise calculate from file location
PROJECT_ROOT = os.getenv('PROJECT_ROOT') or os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Load environment variables from the .env file in the project root
load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, '.env'))

def setup_logging():
    """Configures basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

def initialize_oracle_client():
    """Initializes the Oracle Client. Exits on failure."""
    try:
        # Construct absolute paths relative to the project root
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
    initialize_oracle_client()
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
            logging.info("Successfully connected. Fetching draw history...")
            # Use a warnings context to specifically ignore the UserWarning from pandas
            # about using a non-SQLAlchemy connection object.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                df = pd.read_sql("SELECT * FROM EUROMILLIONS_DRAW_HISTORY ORDER BY DRAW_DATE DESC", connection)
            logging.info(f"Fetched {len(df)} rows from Oracle.")
            return df
    except oracledb.Error as e:
        logging.error(f"Error connecting to or fetching from Oracle Database: {e}")
        return None

def standardize_dataframe(df):
    """Standardizes column names and data types for analysis."""
    df.columns = [col.lower() for col in df.columns]
    for col in ['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5', 'lucky_star_1', 'lucky_star_2']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['ball_1']) # Drop rows where ball data might be missing

def analyze_frequencies(df):
    """Calculates the frequency of each main ball and lucky star."""
    ball_cols = ['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5']
    star_cols = ['lucky_star_1', 'lucky_star_2']

    main_balls = pd.concat([df[col] for col in ball_cols]).value_counts().sort_index()
    lucky_stars = pd.concat([df[col] for col in star_cols]).value_counts().sort_index()

    return main_balls, lucky_stars

def analyze_recency(df):
    """Calculates how many draws ago each number was last seen."""
    # All possible numbers
    all_main_balls = set(range(1, 51))
    all_lucky_stars = set(range(1, 13))

    # Find the last draw index for each number
    last_seen_balls = {}
    last_seen_stars = {}

    for index, row in df.iterrows():
        for i in range(1, 6):
            ball = row[f'ball_{i}']
            if ball not in last_seen_balls:
                last_seen_balls[ball] = index
        for i in range(1, 3):
            star = row[f'lucky_star_{i}']
            if star not in last_seen_stars:
                last_seen_stars[star] = index

    # Fill in any numbers that have never been drawn (unlikely but possible with small dataset)
    for ball in all_main_balls:
        if ball not in last_seen_balls:
            last_seen_balls[ball] = len(df) # Mark as most overdue
    for star in all_lucky_stars:
        if star not in last_seen_stars:
            last_seen_stars[star] = len(df)

    # Convert to Series for easy sorting
    recency_balls = pd.Series(last_seen_balls).sort_values(ascending=False)
    recency_stars = pd.Series(last_seen_stars).sort_values(ascending=False)

    return recency_balls, recency_stars

def generate_prediction(strategy, main_ball_stats, lucky_star_stats):
    """Generates a set of predicted numbers based on the chosen strategy."""
    if strategy == 'most_frequent':
        main_balls = main_ball_stats.nlargest(5).index.tolist()
        lucky_stars = lucky_star_stats.nlargest(2).index.tolist()
        title = "Most Frequent Numbers"
    elif strategy == 'least_frequent':
        main_balls = main_ball_stats.nsmallest(5).index.tolist()
        lucky_stars = lucky_star_stats.nsmallest(2).index.tolist()
        title = "Least Frequent Numbers"
    elif strategy == 'most_overdue':
        main_balls = main_ball_stats.nlargest(5).index.tolist()
        lucky_stars = lucky_star_stats.nlargest(2).index.tolist()
        title = "Most Overdue (Hot) Numbers"
    elif strategy == 'random':
        main_balls = sorted(random.sample(range(1, 51), 5))
        lucky_stars = sorted(random.sample(range(1, 13), 2))
        title = "Random Selection"
    else:
        raise ValueError("Invalid strategy selected.")

    return title, sorted(main_balls), sorted(lucky_stars)

def main():
    """Main function to run the prediction generation."""
    setup_logging()
    logging.info("--- Starting EuroMillions Number 'Prediction' ---")
    print("\n" + "="*60)
    print("DISCLAIMER: This tool is for entertainment purposes only. Lottery")
    print("draws are random, and past performance is not an indicator of")
    print("future results. Please play responsibly.")
    print("="*60 + "\n")
    
    parser = argparse.ArgumentParser(description="Generate EuroMillions number predictions based on a chosen strategy.")
    strategies = {
        '1': 'most_frequent',
        '2': 'least_frequent',
        '3': 'most_overdue',
        '4': 'random'
    }
    parser.add_argument('--strategy', type=str, choices=strategies.values(), help="The prediction strategy to use.")
    args = parser.parse_args()

    if args.strategy:
        strategy = args.strategy
    else:
        # Interactive mode if no strategy is passed via command line
        print("Please choose a prediction strategy:")
        print("  1. Most Frequent:  Picks the numbers that have appeared most often.")
        print("  2. Least Frequent: Picks the numbers that have appeared least often.")
        print("  3. Most Overdue:   Picks the numbers that haven't been drawn for the longest time.")
        print("  4. Random:         Picks a random set of numbers.")
        
        choice = ''
        while choice not in strategies:
            choice = input("\nEnter the number of your chosen strategy (1-4): ").strip()
            if choice not in strategies:
                print("Invalid choice. Please enter a number between 1 and 4.")
        strategy = strategies[choice]
    
    df_raw = get_oracle_data()

    if df_raw is None or df_raw.empty:
        logging.error("Could not retrieve data from Oracle. Exiting.")
        sys.exit(1)

    df = standardize_dataframe(df_raw)

    # --- Analysis ---
    main_ball_freq, lucky_star_freq = analyze_frequencies(df)
    main_ball_recency, lucky_star_recency = analyze_recency(df)

    # --- Prediction ---
    if strategy in ['most_frequent', 'least_frequent']:
        title, main_balls, lucky_stars = generate_prediction(strategy, main_ball_freq, lucky_star_freq)
    elif strategy == 'most_overdue':
        title, main_balls, lucky_stars = generate_prediction(strategy, main_ball_recency, lucky_star_recency)
    elif strategy == 'random':
        # For random, stats are not needed for generation but can be shown for context
        title, main_balls, lucky_stars = generate_prediction(strategy, None, None)
    else:
        logging.error(f"Strategy '{strategy}' is not a valid choice.")
        return

    # --- Display Results ---
    print(f"--- Prediction based on: {title} ---\n")
    print(f"Suggested Main Balls: {main_balls}")
    print(f"Suggested Lucky Stars: {lucky_stars}\n")

    # --- Show some context for the choice ---
    if strategy == 'most_frequent':
        print("These are the 5 main balls and 2 lucky stars drawn most often in history.")
        print("\nMain Ball Frequencies:\n", main_ball_freq.loc[main_balls].to_string())
        print("\nLucky Star Frequencies:\n", lucky_star_freq.loc[lucky_stars].to_string())
    elif strategy == 'least_frequent':
        print("These are the 5 main balls and 2 lucky stars drawn least often in history.")
        print("\nMain Ball Frequencies:\n", main_ball_freq.loc[main_balls].to_string())
        print("\nLucky Star Frequencies:\n", lucky_star_freq.loc[lucky_stars].to_string())
    elif strategy == 'most_overdue':
        print("These are the numbers that have not been drawn for the longest time.")
        print("\nMain Balls (draws since last seen):\n", main_ball_recency.loc[main_balls].to_string())
        print("\nLucky Stars (draws since last seen):\n", lucky_star_recency.loc[lucky_stars].to_string())

    print("\n" + "="*60)
    logging.info("--- Prediction complete ---")


if __name__ == "__main__":
    main()