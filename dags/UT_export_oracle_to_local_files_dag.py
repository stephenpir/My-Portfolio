from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_export_oracle_to_local_files",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "export", "oracle"],
    doc_md="""
    ### Unit Test DAG for `export_oracle_to_local_files.py`

    This DAG is designed to run the `export_oracle_to_local_files.py` script in isolation for unit testing purposes.
    """,
)
def ut_export_oracle_to_local_files_dag():
    run_export_oracle_to_local_files = BashOperator(
        task_id="run_export_oracle_to_local_files",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/export_oracle_to_local_files.py'",
    )

ut_export_oracle_to_local_files_dag()