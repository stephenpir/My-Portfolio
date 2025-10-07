import pandas as pd
import oracledb
import os
import sys
import random
from dotenv import load_dotenv
import logging

from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import numpy as np

# Load environment variables from the .env file in the project root
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

def setup_logging():
    """Configures basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    # Suppress noisy warnings from pandas when using a raw DBAPI2 connection
    import warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

def initialize_oracle_client():
    """Initializes the Oracle Client. Exits on failure."""
    try:
        oracledb.init_oracle_client(
            lib_dir=os.getenv("ORACLE_INSTANT_CLIENT_PATH"),
            config_dir=os.getenv("ORACLE_WALLET_PATH")
        )
    except oracledb.Error as e:
        logging.error(f"Error initializing Oracle Client: {e}")
        logging.error("Please check ORACLE_INSTANT_CLIENT_PATH and ORACLE_WALLET_PATH in your .env file.")
        sys.exit(1)

def get_oracle_data():
    """Fetches euromillions data from the Oracle database."""
    logging.info("Connecting to Oracle DB...")
    try:
        with oracledb.connect(
            user=os.getenv("ORACLE_DB_USER"),
            password=os.getenv("ORACLE_DB_PASSWORD"),
            dsn=os.getenv("ORACLE_DB_DSN"),
            wallet_password=os.getenv("ORACLE_WALLET_PASSWORD"),
            config_dir=os.getenv("ORACLE_WALLET_PATH"),
            wallet_location=os.getenv("ORACLE_WALLET_PATH")
        ) as connection:
            logging.info("Successfully connected. Fetching draw history...")
            # Order by draw_date ascending for feature creation
            df = pd.read_sql("SELECT * FROM EUROMILLIONS_DRAW_HISTORY ORDER BY DRAW_DATE ASC", connection)
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

def create_ml_features_and_targets(df, lag_features=5, recent_freq_window=20):
    """
    Creates features (X) and targets (y) for machine learning.
    Features include lagged draw numbers, recent frequencies, and recency.
    Targets are binary vectors indicating drawn numbers.
    """
    # Ensure dataframe is sorted by date for correct lagging
    df = df.sort_values(by='draw_date').reset_index(drop=True)

    all_main_balls = range(1, 51)
    all_lucky_stars = range(1, 13)

    X = []
    y_main = []
    y_stars = []

    # Iterate through draws, starting after enough history for lagged features and frequency window
    # We need at least `lag_features` draws for lagged data, and `recent_freq_window` for frequency/recency.
    # So, start from the maximum of these two, plus one for the current draw.
    start_index = max(lag_features, recent_freq_window)

    for i in range(start_index, len(df)):
        current_draw = df.iloc[i]
        
        # --- Create Features for the current draw (based on previous draws up to i-1) ---
        features_row = []

        # Lagged numbers
        for lag in range(1, lag_features + 1):
            prev_draw = df.iloc[i - lag]
            features_row.extend([prev_draw[f'ball_{j}'] for j in range(1, 6)])
            features_row.extend([prev_draw[f'lucky_star_{j}'] for j in range(1, 3)])
        
        # Recent Frequencies and Recency (calculated from draws up to i-1)
        # This window should end at i-1 to avoid data leakage
        recent_history_df = df.iloc[i - recent_freq_window : i]
        
        # Main Balls
        main_ball_series = pd.concat([recent_history_df[f'ball_{j}'] for j in range(1, 6)])
        main_ball_counts = main_ball_series.value_counts()
        
        for ball_num in all_main_balls:
            features_row.append(main_ball_counts.get(ball_num, 0)) # Frequency
            
            # Recency: how many draws ago was it last seen in the recent_history_df?
            # Find the last index where this ball appeared in recent_history_df
            last_seen_draw_index = -1
            for k in range(len(recent_history_df) - 1, -1, -1):
                if ball_num in recent_history_df.iloc[k][['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5']].values:
                    last_seen_draw_index = k
                    break
            # If found, recency is (number of draws in recent_history_df - 1 - its index)
            # If not found, it's overdue beyond the window, so assign a large value
            features_row.append(len(recent_history_df) - 1 - last_seen_draw_index if last_seen_draw_index != -1 else recent_freq_window + 1)

        # Lucky Stars
        lucky_star_series = pd.concat([recent_history_df[f'lucky_star_{j}'] for j in range(1, 3)])
        lucky_star_counts = lucky_star_series.value_counts()

        for star_num in all_lucky_stars:
            features_row.append(lucky_star_counts.get(star_num, 0)) # Frequency
            
            last_seen_draw_index = -1
            for k in range(len(recent_history_df) - 1, -1, -1):
                if star_num in recent_history_df.iloc[k][['lucky_star_1', 'lucky_star_2']].values:
                    last_seen_draw_index = k
                    break
            features_row.append(len(recent_history_df) - 1 - last_seen_draw_index if last_seen_draw_index != -1 else recent_freq_window + 1)

        X.append(features_row)

        # --- Create Targets for the current draw ---
        current_main_balls = set([current_draw[f'ball_{j}'] for j in range(1, 6)])
        current_lucky_stars = set([current_draw[f'lucky_star_{j}'] for j in range(1, 3)])

        y_main_row = [1 if ball in current_main_balls else 0 for ball in all_main_balls]
        y_stars_row = [1 if star in current_lucky_stars else 0 for star in all_lucky_stars]

        y_main.append(y_main_row)
        y_stars.append(y_stars_row)

    return np.array(X), np.array(y_main), np.array(y_stars)

def train_and_predict_ml(X, y_main, y_stars):
    """
    Trains ML models for each number and predicts the next set of numbers.
    """
    logging.info("Training ML models and generating prediction...")

    # The last row of X contains features derived from the most recent historical draw(s)
    # and will be used to predict the *next* unseen draw.
    X_train = X[:-1]
    X_predict_next = X[-1].reshape(1, -1) # Reshape for single sample prediction

    y_main_train = y_main[:-1]
    y_stars_train = y_stars[:-1]

    predicted_main_balls = []
    predicted_lucky_stars = []

    # --- Predict Main Balls ---
    main_ball_probabilities = []
    for i in range(y_main_train.shape[1]): # Iterate through each main ball (0-49 for 1-50)
        # Using RandomForestClassifier as it handles non-linearities better than LogisticRegression
        # and can give probability estimates.
        # 'class_weight='balanced'' is crucial due to the high imbalance (many more 0s than 1s)
        model = Pipeline([
            ('scaler', StandardScaler()), # Scale features
            ('classifier', RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'))
        ])
        model.fit(X_train, y_main_train[:, i])
        prob = model.predict_proba(X_predict_next)[:, 1][0]
        main_ball_probabilities.append((i + 1, prob)) # Store (ball_number, probability)

    # Sort by probability and pick the top 5
    main_ball_probabilities.sort(key=lambda x: x[1], reverse=True)
    predicted_main_balls = sorted([ball for ball, prob in main_ball_probabilities[:5]])

    # --- Predict Lucky Stars ---
    lucky_star_probabilities = []
    for i in range(y_stars_train.shape[1]): # Iterate through each lucky star (0-11 for 1-12)
        model = Pipeline([
            ('scaler', StandardScaler()),
            ('classifier', RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced'))
        ])
        model.fit(X_train, y_stars_train[:, i])
        prob = model.predict_proba(X_predict_next)[:, 1][0]
        lucky_star_probabilities.append((i + 1, prob)) # Store (star_number, probability)

    # Sort by probability and pick the top 2
    lucky_star_probabilities.sort(key=lambda x: x[1], reverse=True)
    predicted_lucky_stars = sorted([star for star, prob in lucky_star_probabilities[:2]])

    return predicted_main_balls, predicted_lucky_stars

def main_ml():
    """Main function for ML prediction."""
    setup_logging()
    logging.info("--- Starting EuroMillions ML 'Prediction' ---")
    print("\n" + "="*60)
    print("DISCLAIMER: This tool uses Machine Learning to 'predict' lottery numbers.")
    print("Lottery draws are inherently random, and past results do NOT guarantee")
    print("future outcomes. This script is for educational and entertainment purposes")
    print("only and should NOT be used for actual gambling. Play responsibly.")
    print("="*60 + "\n")

    initialize_oracle_client()
    # Ensure data is ordered by date ascending for feature creation
    df_raw = get_oracle_data()

    if df_raw is None or df_raw.empty:
        logging.error("Could not retrieve data from Oracle. Exiting.")
        sys.exit(1)

    df = standardize_dataframe(df_raw)

    # Define parameters for feature creation
    lag_features = 5 # Look at the last 5 draws for lagged numbers
    recent_freq_window = 20 # Look at the last 20 draws for frequency/recency

    # Ensure enough data for feature creation
    min_draws_needed = max(lag_features, recent_freq_window) + 1 # +1 for the current draw itself
    if len(df) < min_draws_needed:
        logging.error(f"Not enough historical data ({len(df)} draws) to create ML features.")
        logging.error(f"Need at least {min_draws_needed} draws for current settings (lag={lag_features}, freq_window={recent_freq_window}). Exiting.")
        sys.exit(1)

    X, y_main, y_stars = create_ml_features_and_targets(df, lag_features, recent_freq_window)

    if X.shape[0] == 0:
        logging.error("No features could be generated from the historical data. This might happen if the dataset is too small after accounting for lags/windows. Exiting.")
        sys.exit(1)

    predicted_main_balls, predicted_lucky_stars = train_and_predict_ml(X, y_main, y_stars)

    print(f"--- ML Predicted Numbers for the Next Draw ---")
    print(f"Suggested Main Balls: {predicted_main_balls}")
    print(f"Suggested Lucky Stars: {predicted_lucky_stars}\n")

    print("\n" + "="*60)
    logging.info("--- ML Prediction complete ---")

if __name__ == "__main__":
    main_ml()