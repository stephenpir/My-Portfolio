import pandas as pd
import oracledb
import os
import sys
from dotenv import load_dotenv
import logging
import matplotlib.pyplot as plt
import argparse
import warnings
import seaborn as sns
import numpy as np
from collections import defaultdict

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
    log_file = 'analysis_report.log' # Changed log file name to avoid conflict with validation
    
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

def standardize_dataframe(df):
    """Standardizes column names and data types for analysis."""
    # Convert all column names to lowercase for consistency
    df.columns = [col.lower() for col in df.columns]

    # Ensure draw_date is a datetime object (without time)
    if 'draw_date' in df.columns:
        df['draw_date'] = pd.to_datetime(df['draw_date']).dt.normalize()
    else:
        raise KeyError(f"'draw_date' column not found in data.")

    # Define the core columns for analysis
    core_columns = [
        'draw_date', 'draw_number', 'ball_1', 'ball_2', 'ball_3', 'ball_4',
        'ball_5', 'lucky_star_1', 'lucky_star_2'
    ]
    
    # Ensure all core columns exist
    for col in core_columns:
        if col not in df.columns:
            raise KeyError(f"Required column '{col}' not found in data.")

    # Convert ball and star columns to a consistent integer type
    for col in core_columns[1:]: # Skip 'draw_date'
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    return df[core_columns].sort_values(by='draw_date').reset_index(drop=True)

def plot_pie_chart(data, title, filename, top_n=None):
    """Generates and saves a pie chart for number distribution."""
    if top_n and len(data) > top_n:
        # Group less frequent numbers into 'Others'
        top_data = data.nlargest(top_n)
        other_sum = data.nsmallest(len(data) - top_n).sum()
        top_data['Others'] = other_sum
        plot_data = top_data
        title = f"{title} (Top {top_n})"
    else:
        plot_data = data

    plt.figure(figsize=(12, 6))
    # Use a blue-themed color map, avoiding the lightest shades for better visibility
    colors = plt.cm.Blues_r(np.linspace(0.4, 1, len(plot_data)))
    plt.pie(plot_data, labels=plot_data.index, autopct='%1.1f%%', startangle=140, colors=colors)
    plt.title(title)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.tight_layout()
    plt.savefig(f"{filename}.png")
    plt.close()
    logging.info(f"Saved plot: {filename}.png")


def analyze_distributions(df, show_details_in_terminal):
    """Analyzes and visualizes the distribution of numbers."""
    logging.info("--- Analyzing Number Distributions ---")
    
    # Create directories for plots if they don't exist
    overall_plots_dir = "plots"
    yearly_plots_dir = "plots/yearly_distributions"
    os.makedirs(overall_plots_dir, exist_ok=True)
    os.makedirs(yearly_plots_dir, exist_ok=True)

    # --- 1. Overall Distributions (All Time) ---
    logging.info("--- Generating Overall Distribution Charts ---")
    ball_cols = ['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5']
    lucky_star_cols = ['lucky_star_1', 'lucky_star_2']

    # Overall Main Balls
    main_balls = pd.concat([df[col] for col in ball_cols])
    main_balls_summary = main_balls.value_counts()
    logging.info("\nMain Ball Distribution Summary:\n" + main_balls_summary.to_string(), extra={'file_only': not show_details_in_terminal})
    plot_pie_chart(main_balls_summary, "Overall Distribution of Main Ball Numbers", f"{overall_plots_dir}/main_balls_pie_distribution", top_n=15)

    # Overall Lucky Stars
    lucky_stars = pd.concat([df[col] for col in lucky_star_cols])
    lucky_stars_summary = lucky_stars.value_counts()
    logging.info("\nOverall Lucky Star Distribution Summary:\n" + lucky_stars_summary.to_string(), extra={'file_only': not show_details_in_terminal})
    plot_pie_chart(lucky_stars_summary, "Overall Distribution of Lucky Star Numbers", f"{overall_plots_dir}/lucky_stars_pie_distribution")

    # --- 2. Year-by-Year Distributions ---
    logging.info("--- Generating Year-by-Year Distribution Charts ---")
    df['year'] = df['draw_date'].dt.year
    for year in sorted(df['year'].unique()):
        logging.info(f"Analyzing distributions for the year {year}...")
        year_df = df[df['year'] == year]

        # Yearly Main Balls
        year_main_balls = pd.concat([year_df[col] for col in ball_cols])
        if not year_main_balls.empty:
            year_main_balls_summary = year_main_balls.value_counts()
            plot_pie_chart(year_main_balls_summary, f"Distribution of Main Ball Numbers for {year}", f"{yearly_plots_dir}/main_balls_pie_{year}", top_n=15)
        else:
            logging.warning(f"No main ball data to plot for year {year}.")

        # Yearly Lucky Stars
        year_lucky_stars = pd.concat([year_df[col] for col in lucky_star_cols])
        if not year_lucky_stars.empty:
            year_lucky_stars_summary = year_lucky_stars.value_counts()
            plot_pie_chart(year_lucky_stars_summary, f"Distribution of Lucky Star Numbers for {year}", f"{yearly_plots_dir}/lucky_stars_pie_{year}")
        else:
            logging.warning(f"No lucky star data to plot for year {year}.")

def analyze_time_series(df, show_details_in_terminal):
    """Analyzes and visualizes time series trends of number frequencies."""
    logging.info("--- Analyzing Time Series Distributions ---")
    
    os.makedirs("plots", exist_ok=True)

    # Sort by draw_date for time series analysis
    df = df.sort_values('draw_date').reset_index(drop=True)

    # --- Yearly Frequency Heatmap for Main Balls ---
    logging.info("Generating yearly frequency heatmap for main balls...")
    ball_cols = ['ball_1', 'ball_2', 'ball_3', 'ball_4', 'ball_5']
    if not df.empty:
        yearly_ball_counts = df.melt(id_vars=['draw_date'], value_vars=ball_cols, value_name='ball_number')
        yearly_ball_counts['year'] = yearly_ball_counts['draw_date'].dt.year
        yearly_ball_counts = yearly_ball_counts.groupby(['year', 'ball_number']).size().unstack(fill_value=0)

        if not yearly_ball_counts.empty:
            plt.figure(figsize=(16, 10))
            sns.heatmap(yearly_ball_counts.T, cmap='YlOrRd', linewidths=.5, annot=True, fmt='d')
            plt.title("Yearly Frequency Heatmap of Main Ball Numbers")
            plt.xlabel("Year")
            plt.ylabel("Ball Number")
            plt.tight_layout()
            plt.savefig("plots/yearly_ball_frequency_heatmap.png")
            plt.close()
            logging.info("Saved plot: plots/yearly_ball_frequency_heatmap.png")
        else:
            logging.warning("No data to plot for yearly main ball frequency heatmap.")
    else:
        logging.warning("DataFrame is empty, cannot generate main ball heatmap.")

    # --- Rolling Frequency for Lucky Stars ---
    logging.info("Calculating rolling frequency for lucky stars...")
    lucky_star_cols = ['lucky_star_1', 'lucky_star_2']
    rolling_window = 50  # Number of draws to include in the rolling average

    # Create a binary matrix: 1 if the number was drawn, 0 otherwise
    lucky_star_dummies = pd.get_dummies(df[lucky_star_cols].stack()).groupby(level=0).sum()
    # Reindex to include all possible lucky stars (1-12)
    all_stars = range(1, 13)
    lucky_star_dummies = lucky_star_dummies.reindex(columns=all_stars, fill_value=0)

    # Calculate the rolling sum (frequency) over the specified window
    rolling_freq = lucky_star_dummies.rolling(window=rolling_window).sum().dropna()
    rolling_freq['draw_date'] = df['draw_date'].iloc[rolling_freq.index]

    if not rolling_freq.empty:
        plt.figure(figsize=(18, 10))
        sns.lineplot(data=rolling_freq.melt(id_vars='draw_date', var_name='lucky_star', value_name='frequency'),
                     x='draw_date', y='frequency', hue='lucky_star', palette='tab10', legend='full')
        plt.title(f"Rolling Frequency of Lucky Stars (Window: {rolling_window} Draws)")
        plt.xlabel("Date")
        plt.ylabel(f"Frequency in last {rolling_window} draws")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("plots/rolling_lucky_star_frequency.png")
        plt.close()
        logging.info("Saved plot: plots/rolling_lucky_star_frequency.png")

    # --- Yearly Frequency Heatmap for Lucky Stars ---
    logging.info("Generating yearly frequency heatmap for lucky stars...")
    yearly_star_counts = df.melt(id_vars=['draw_date'], value_vars=lucky_star_cols, value_name='lucky_star_number')
    yearly_star_counts['year'] = yearly_star_counts['draw_date'].dt.year
    yearly_star_counts = yearly_star_counts.groupby(['year', 'lucky_star_number']).size().unstack(fill_value=0)

    if not yearly_star_counts.empty:
        plt.figure(figsize=(12, 8))
        sns.heatmap(yearly_star_counts.T, cmap='YlOrRd', linewidths=.5, annot=True, fmt='d')
        plt.title("Yearly Frequency Heatmap of Lucky Star Numbers")
        plt.xlabel("Year")
        plt.ylabel("Lucky Star Number")
        plt.tight_layout()
        plt.savefig("plots/yearly_lucky_star_frequency_heatmap.png")
        plt.close()
        logging.info("Saved plot: plots/yearly_lucky_star_frequency_heatmap.png")
    else:
        logging.warning("No data to plot for yearly lucky star frequency heatmap.")


def main():
    """Main function to run the data analysis."""
    setup_logging()
    logging.info("--- Starting EuroMillions Data Analysis ---")

    # --- Argument Parsing for Non-Interactive Mode (Airflow) ---
    parser = argparse.ArgumentParser(description="Run EuroMillions data analysis.")
    parser.add_argument('--show-details', action='store_true', help="Show detailed summary data in the terminal.")
    args, unknown = parser.parse_known_args() # Use parse_known_args to avoid errors with other potential args

    if args.show_details:
        show_details_in_terminal = True
    else:
        # --- Interactive Mode ---
        show_details_in_terminal = input("Show detailed summary data in the terminal? (y/n): ").lower().strip() == 'y'

    if show_details_in_terminal:
        logging.info("Detailed results will be shown in the terminal and saved to analysis_report.log.")
    else:
        logging.info("Progress will be shown in the terminal. Detailed summary data will be saved to analysis_report.log.")
    
    logging.info(f"PROJECT_ROOT resolved to: {PROJECT_ROOT}")
    logging.info(f"Attempting to load .env from: {os.path.join(PROJECT_ROOT, '.env')}")

    df = get_oracle_data()

    if df is None:
        logging.error("Could not retrieve data from Oracle database. Exiting.")
        sys.exit(1)

    logging.info("Standardizing data for analysis...")
    try:
        df_std = standardize_dataframe(df)
    except KeyError as e:
        logging.error(f"Column error during standardization: {e}")
        sys.exit(1)

    if df_std.empty:
        logging.warning("No data available after standardization. Exiting analysis.")
        sys.exit(0)

    # Perform distribution analysis
    analyze_distributions(df_std, show_details_in_terminal)

    # Perform time series analysis
    analyze_time_series(df_std, show_details_in_terminal)
    
    logging.info("--- Data Analysis Complete. Full report and plots saved. ---")

if __name__ == "__main__":
    main()
