from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_load_source1_to_pg",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "ingestion", "postgres"],
    doc_md="""
    ### Unit Test DAG for `load_source1_Euromillions_lottery_data_to_postgres.py`

    This DAG is designed to run the `load_source1_Euromillions_lottery_data_to_postgres.py` script in isolation for unit testing purposes.
    """,
)
def ut_load_source1_to_pg_dag():
    run_load_source1_to_pg = BashOperator(
        task_id="run_load_source1_to_pg",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_1/load_source1_Euromillions_lottery_data_to_postgres.py'",
    )

ut_load_source1_to_pg_dag()