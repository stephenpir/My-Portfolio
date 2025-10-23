import pandas as pd
import oracledb
import os
import sys
from dotenv import load_dotenv
import logging
import argparse
import numpy as np
import warnings
import tempfile

# --- PyTorch Imports ---
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader

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
    return df.dropna(subset=['ball_1'])

def create_ml_features_and_targets(df, lag_features=5, recent_freq_window=20):
    """Creates features (X) and targets (y) for machine learning."""
    df = df.sort_values(by='draw_date').reset_index(drop=True)
    all_main_balls = range(1, 51)
    all_lucky_stars = range(1, 13)
    X, y_main, y_stars = [], [], []
    start_index = max(lag_features, recent_freq_window)

    for i in range(start_index, len(df)):
        features_row = []
        # Lagged numbers
        for lag in range(1, lag_features + 1):
            prev_draw = df.iloc[i - lag]
            features_row.extend([prev_draw[f'ball_{j}'] for j in range(1, 6)])
            features_row.extend([prev_draw[f'lucky_star_{j}'] for j in range(1, 3)])
        
        recent_history_df = df.iloc[i - recent_freq_window : i]
        
        # Main Balls: Frequency and Recency
        main_ball_series = pd.concat([recent_history_df[f'ball_{j}'] for j in range(1, 6)])
        main_ball_counts = main_ball_series.value_counts()
        for ball_num in all_main_balls:
            features_row.append(main_ball_counts.get(ball_num, 0))
            last_seen = -1
            for k in range(len(recent_history_df) - 1, -1, -1):
                if ball_num in recent_history_df.iloc[k][['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5']].values:
                    last_seen = k; break
            features_row.append(len(recent_history_df) - 1 - last_seen if last_seen != -1 else recent_freq_window + 1)

        # Lucky Stars: Frequency and Recency
        lucky_star_series = pd.concat([recent_history_df[f'lucky_star_{j}'] for j in range(1, 3)])
        lucky_star_counts = lucky_star_series.value_counts()
        for star_num in all_lucky_stars:
            features_row.append(lucky_star_counts.get(star_num, 0))
            last_seen = -1
            for k in range(len(recent_history_df) - 1, -1, -1):
                if star_num in recent_history_df.iloc[k][['lucky_star_1', 'lucky_star_2']].values:
                    last_seen = k; break
            features_row.append(len(recent_history_df) - 1 - last_seen if last_seen != -1 else recent_freq_window + 1)

        X.append(features_row)

        current_draw = df.iloc[i]
        current_main_balls = set(current_draw[f'ball_{j}'] for j in range(1, 6))
        current_lucky_stars = set(current_draw[f'lucky_star_{j}'] for j in range(1, 3))
        y_main.append([1 if ball in current_main_balls else 0 for ball in all_main_balls])
        y_stars.append([1 if star in current_lucky_stars else 0 for star in all_lucky_stars])

    return np.array(X, dtype=np.float32), np.array(y_main, dtype=np.float32), np.array(y_stars, dtype=np.float32)

# --- PyTorch Model and Data Components ---

class LotteryDataset(Dataset):
    """Custom PyTorch Dataset for lottery data."""
    def __init__(self, features, targets):
        self.features = torch.from_numpy(features)
        self.targets = torch.from_numpy(targets)

    def __len__(self):
        return len(self.features)

    def __getitem__(self, idx):
        return self.features[idx], self.targets[idx]

class LotteryPredictor(nn.Module):
    """A simple MLP for predicting lottery numbers."""
    def __init__(self, input_size, output_size):
        super(LotteryPredictor, self).__init__()
        self.network = nn.Sequential(
            nn.Linear(input_size, 256),
            nn.ReLU(),
            nn.Dropout(0.5),
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Dropout(0.5),
            nn.Linear(128, output_size)  # Output raw logits for BCEWithLogitsLoss
        )

    def forward(self, x):
        return self.network(x)

def train_model(model, dataloader, pos_weight, epochs=20, learning_rate=0.001):
    """Trains a PyTorch model."""
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight)
    optimizer = optim.Adam(model.parameters(), lr=learning_rate)
    model.train()

    for epoch in range(epochs):
        total_loss = 0
        for features, targets in dataloader:
            optimizer.zero_grad()
            outputs = model(features)
            loss = criterion(outputs, targets)
            loss.backward()
            optimizer.step()
            total_loss += loss.item()
        
        avg_loss = total_loss / len(dataloader)
        if (epoch + 1) % 5 == 0:
            logging.info(f'Epoch [{epoch+1}/{epochs}], Loss: {avg_loss:.4f}')

def train_and_predict_pytorch(X, y_main, y_stars):
    """
    Trains PyTorch models and predicts the next set of numbers.
    """
    logging.info("Training PyTorch models and generating prediction...")

    # Use all but the last sample for training, and the last for prediction
    X_train, X_predict_next = X[:-1], X[-1]
    y_main_train, y_stars_train = y_main[:-1], y_stars[:-1]

    # --- Predict Main Balls ---
    logging.info("--- Training Main Ball Model ---")
    # Calculate positive weight for imbalanced classes
    # pos_weight = (number of negatives) / (number of positives)
    pos_weight_main = torch.tensor((y_main_train.shape[0] * y_main_train.shape[1] - y_main_train.sum()) / y_main_train.sum())
    
    main_dataset = LotteryDataset(X_train, y_main_train)
    main_dataloader = DataLoader(main_dataset, batch_size=32, shuffle=True)
    
    main_model = LotteryPredictor(input_size=X_train.shape[1], output_size=y_main_train.shape[1])
    train_model(main_model, main_dataloader, pos_weight_main)

    main_model.eval()
    with torch.no_grad():
        prediction_input = torch.from_numpy(X_predict_next).unsqueeze(0)
        logits = main_model(prediction_input)
        probabilities = torch.sigmoid(logits).squeeze().numpy()

    main_ball_probs = sorted(enumerate(probabilities, 1), key=lambda x: x[1], reverse=True)
    predicted_main_balls = sorted([ball for ball, prob in main_ball_probs[:5]])

    # --- Predict Lucky Stars ---
    logging.info("--- Training Lucky Star Model ---")
    pos_weight_stars = torch.tensor((y_stars_train.shape[0] * y_stars_train.shape[1] - y_stars_train.sum()) / y_stars_train.sum())

    star_dataset = LotteryDataset(X_train, y_stars_train)
    star_dataloader = DataLoader(star_dataset, batch_size=32, shuffle=True)

    star_model = LotteryPredictor(input_size=X_train.shape[1], output_size=y_stars_train.shape[1])
    train_model(star_model, star_dataloader, pos_weight_stars)

    star_model.eval()
    with torch.no_grad():
        prediction_input = torch.from_numpy(X_predict_next).unsqueeze(0)
        logits = star_model(prediction_input)
        probabilities = torch.sigmoid(logits).squeeze().numpy()

    lucky_star_probs = sorted(enumerate(probabilities, 1), key=lambda x: x[1], reverse=True)
    predicted_lucky_stars = sorted([star for star, prob in lucky_star_probs[:2]])

    return predicted_main_balls, predicted_lucky_stars

def main_pytorch():
    """Main function for PyTorch prediction."""
    setup_logging()
    logging.info("--- Starting EuroMillions PyTorch 'Prediction' ---")
    print("\n" + "="*60)
    print("DISCLAIMER: This tool uses a PyTorch Neural Network to 'predict' numbers.")
    print("Lottery draws are random. This script is for educational purposes only")
    print("and should NOT be used for actual gambling. Play responsibly.")
    print("="*60 + "\n")

    initialize_oracle_client()
    df_raw = get_oracle_data()
    if df_raw is None:
        logging.error("Could not retrieve data from Oracle. Exiting.")
        sys.exit(1)

    df = standardize_dataframe(df_raw)

    # --- Argument Parsing for Data Filtering ---
    parser = argparse.ArgumentParser(description="Generate EuroMillions predictions using a PyTorch Neural Network.")
    parser.add_argument('--filter', type=str, choices=['all', 'tuesday', 'friday'], help="Filter dataset by draw day ('all', 'tuesday', 'friday').")
    args = parser.parse_args()

    if args.filter:
        filter_choice_map = {'all': '1', 'tuesday': '2', 'friday': '3'}
        filter_choice = filter_choice_map[args.filter]
    else:
        # Interactive mode if no filter is passed via command line
        print("Please choose the dataset to use for prediction:")
        print("  1. Entire History (All Draws)")
        print("  2. Tuesday Draws Only")
        print("  3. Friday Draws Only")

        filter_choice = ''
        while filter_choice not in ['1', '2', '3']:
            filter_choice = input("\nEnter your choice (1-3): ").strip()
            if filter_choice not in ['1', '2', '3']:
                print("Invalid choice. Please enter a number between 1 and 3.")

    df_filtered = df.copy() # Start with the full dataframe
    filter_desc = "Entire History"
    if filter_choice == '2':
        df_filtered = df[df['draw_date'].dt.day_name() == 'Tuesday'].copy()
        filter_desc = "Tuesday Draws Only"
        logging.info(f"Filtering dataset for Tuesday draws. Found {len(df_filtered)} records.")
    elif filter_choice == '3':
        df_filtered = df[df['draw_date'].dt.day_name() == 'Friday'].copy()
        filter_desc = "Friday Draws Only"
        logging.info(f"Filtering dataset for Friday draws. Found {len(df_filtered)} records.")

    # Define parameters for feature creation
    lag_features = 5
    recent_freq_window = 20

    min_draws_needed = max(lag_features, recent_freq_window) + 1
    if len(df_filtered) < min_draws_needed:
        logging.error(f"Not enough historical data in the selected subset ({len(df_filtered)} draws) to create ML features.")
        logging.error(f"Need at least {min_draws_needed} draws for current settings (lag={lag_features}, freq_window={recent_freq_window}). Exiting.")
        sys.exit(1)

    X, y_main, y_stars = create_ml_features_and_targets(df_filtered, lag_features, recent_freq_window)

    if X.shape[0] <= 1:
        logging.error("Not enough samples generated to train and predict. Need at least 2. Exiting.")
        sys.exit(1)

    predicted_main_balls, predicted_lucky_stars = train_and_predict_pytorch(X, y_main, y_stars)

    print(f"\n--- PyTorch Predicted Numbers (based on {filter_desc}) ---")
    print(f"Suggested Main Balls: {predicted_main_balls}")
    print(f"Suggested Lucky Stars: {predicted_lucky_stars}\n")

    print("\n" + "="*60)
    logging.info("--- PyTorch Prediction complete ---")

if __name__ == "__main__":
    # Check for PyTorch installation
    try:
        import torch
    except ImportError:
        print("PyTorch is not installed. Please install it to run this script.", file=sys.stderr)
        print("You can install it by running: pip install torch", file=sys.stderr)
        sys.exit(1)
    
    main_pytorch()