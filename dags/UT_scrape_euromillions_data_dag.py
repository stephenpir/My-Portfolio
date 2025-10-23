from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_scrape_euromillions_data",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "ingestion", "scrape"],
    doc_md="""
    ### Unit Test DAG for `scrape_euromillions_data.py`

    This DAG is designed to run the `scrape_euromillions_data.py` script in isolation for unit testing purposes.
    """,
)
def ut_scrape_euromillions_data_dag():
    run_scrape_euromillions_data = BashOperator(
        task_id="run_scrape_euromillions_data",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_2/scrape_euromillions_data.py'",
    )

ut_scrape_euromillions_data_dag()