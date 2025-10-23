from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Define the project's root directory
#PROJECT_ROOT = "/Users/stephenpir/Desktop/Code/My Portfolio"
PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="euromillions_ingestion_and_validation",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule="0 2 * * 3,6",  # Runs at 2 AM on Wednesday and Saturday
    catchup=False,
    tags=["euromillions", "ingestion", "validation"],
    doc_md="""
    ### EuroMillions Ingestion and Validation DAG

    This DAG performs the following steps:
    1.  **Scrapes** the latest draw history from the web.
    2.  **Loads** the scraped data into the Oracle database.
    3.  **Loads** supplementary source data into the PostgreSQL database.
    4.  **Validates** the data consistency between Oracle and PostgreSQL.
    """,
)
def euromillions_ingestion_dag():
    """
    Orchestrates the scraping, loading, and validation of EuroMillions data.
    """

    @task
    def run_validation():
        """Runs the data validation script."""
        # The --show-details flag can be added here if you want detailed logs in Airflow
        # For manual runs, you can trigger with config: `{"show_details": true}`
        # and use `{{ dag_run.conf.get('show_details', false) }}` to dynamically set it.
        # For simplicity, we run with default (non-detailed) terminal output.
        BashOperator(
            task_id="validate_data",
            bash_command=f"python '{PROJECT_ROOT}/Scenario_2/validate_data.py'",
        ).execute(context={})

    # Task to scrape new data
    scrape_data = BashOperator(
        task_id="scrape_euromillions_data",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_2/scrape_euromillions_data.py'",
    )

    # Task to load the newly scraped data into Oracle
    load_scraped_to_oracle = BashOperator(
        task_id="load_scraped_to_oracle",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_2/load_euromillions_data.py'",
    )

    # These can run in parallel with the Oracle load
    load_source1_to_pg = BashOperator(task_id="load_source1_to_pg", bash_command=f"python '{PROJECT_ROOT}/Scenario_1/load_source1_Euromillions_lottery_data_to_postgres.py'")
    # Source 2 loading is commented out due to poor data quality
    #load_source2_to_pg = BashOperator(task_id="load_source2_to_pg", bash_command=f"python '{PROJECT_ROOT}/Scenario_1/load_source2_Euromillions_lottery_data_to_postgres.py'")

    # Define task dependencies
    scrape_data >> load_scraped_to_oracle
    [load_scraped_to_oracle, load_source1_to_pg] >> run_validation()
    # Source 2 loading is commented out due to poor data quality
    #[load_scraped_to_oracle, load_source1_to_pg, load_source2_to_pg] >> run_validation()

euromillions_ingestion_dag()