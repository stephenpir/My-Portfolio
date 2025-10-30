import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import os

def scrape_euromillions_history():
    """
    Scrapes all EuroMillions draw history from beatlottery.co.uk.

    This function iterates through the paginated draw history, extracts the
    draw date, main balls, and lucky stars for each draw, and returns
    them as a pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the scraped draw history with
                      columns for date, balls, and lucky stars.
    """
    # Get the directory of the current script to save files in the correct location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_filename = "euromillions_draw_history_scraped.csv"

    base_url = "https://www.beatlottery.co.uk/euromillions/draw-history/year/"
    all_draws = []
    start_year = 2004  # The first year of EuroMillions draws
    end_year = 2025    # Scrape up to and including this year

    # Create a session object to persist headers and cookies, making requests more browser-like
    session = requests.Session()
    # Use a more comprehensive set of headers to better mimic a browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })

    # --- Add Retry Logic ---
    # Configure a retry strategy for handling transient network errors or server-side issues.
    retry_strategy = Retry(
        total=3,  # Total number of retries
        backoff_factor=1,  # A delay factor between retries: {backoff factor} * (2 ** ({number of total retries} - 1))
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry on
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    for year in range(start_year, end_year + 1):
        url = f"{base_url}{year}/"

        print(f"Scraping page: {url}")

        try:
            # Send a GET request to the URL
            response = session.get(url, timeout=15)

            # If a page for a future year doesn't exist, it will return a 404
            if response.status_code == 404:
                print(f"Page for year {year} not found. Skipping.")
                continue

            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

            # Parse the HTML content
            soup = BeautifulSoup(response.content, "html.parser")

            # Find the table containing the draw history
            table = soup.select_one("div.table-responsive table")
            if not table:
                debug_filepath = os.path.join(script_dir, f"debug_page_content_{year}.html")
                print(f"No results table found for year {year}. Saving received HTML to '{debug_filepath}' for inspection.")
                with open(debug_filepath, "w", encoding="utf-8") as f:
                    f.write(soup.prettify())
                continue

            # --- Flexible Parsing Logic ---
            # 1. Find header row and map header names to column indices
            header_row = table.select_one("thead tr")
            if not header_row:
                print(f"Could not find table header for year {year}. Skipping.")
                continue

            headers = [th.text.strip().lower() for th in header_row.find_all("th")]
            try:
                header_map = {
                    'draw date': headers.index('draw date'),
                    'jackpot': headers.index('jackpot'),
                    'winners': headers.index('winners'),
                }
            except ValueError as e:
                print(f"[ERROR] Could not find expected column header in {year}: {e}. Skipping year.")
                continue

            # 2. Find all data rows in the table body and process them using the header map
            rows = table.select("tbody tr")

            print(f"Found {len(rows)} draws for {year}.")

            for i, row in enumerate(rows):
                try:
                    cells = row.find_all("td")
                    if len(cells) > max(header_map.values()):
                        draw_date = cells[header_map['draw date']].text.strip()
                        jackpot = cells[header_map['jackpot']].text.strip()
                        winners = cells[header_map['winners']].text.strip()

                        balls = [span.text.strip() for span in row.select("span.ball-euromillions")]
                        lucky_stars = [span.text.strip() for span in row.select("span.ball-euromillions-lucky-star")]

                        if not balls or not lucky_stars:
                            print(f"\n[WARNING] Failed to extract numbers for row {i+1} in {year} (Date: {draw_date}). HTML:")
                            print(row.prettify())
                        else:
                            all_draws.append([draw_date] + balls + lucky_stars + [jackpot, winners])
                except Exception as e:
                    print(f"\n[ERROR] Could not process row {i+1} in {year} due to: {e}. HTML:")
                    print(row.prettify())

        except requests.exceptions.RequestException as e:
            print(f"An error occurred during the request: {e}")
            break

    if not all_draws:
        print("No data was scraped.")
        return None

    # Create a pandas DataFrame
    columns = ["draw_date", "ball_1", "ball_2", "ball_3", "ball_4", "ball_5", "lucky_star_1", "lucky_star_2", "jackpot", "winners"]
    df = pd.DataFrame(all_draws, columns=columns)

    # Convert date column to datetime objects
    df['draw_date'] = pd.to_datetime(df['draw_date'], format='%d %b %Y')

    return df

if __name__ == "__main__":
    scraped_df = scrape_euromillions_history()
    if scraped_df is not None and not scraped_df.empty:
        # Define the output path within the script's directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "euromillions_draw_history_scraped.csv")

        # Save the DataFrame to a CSV file, ensuring the directory exists
        scraped_df.to_csv(output_path, index=False, date_format='%Y-%m-%d')
        print(f"\nSuccessfully scraped {len(scraped_df)} draws and saved to {output_path}")
    else:
        print("\nScraping did not produce any data. No file was saved.")