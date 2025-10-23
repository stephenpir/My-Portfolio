from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_load_json_to_sqlserver",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "export", "sqlserver"],
    doc_md="""
    ### Unit Test DAG for `load_json_to_sqlserver.py`

    This DAG is designed to run the `load_json_to_sqlserver.py` script in isolation for unit testing purposes.
    """,
)
def ut_load_json_to_sqlserver_dag():
    run_load_json_to_sqlserver = BashOperator(
        task_id="run_load_json_to_sqlserver",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/load_json_to_sqlserver.py'",
    )

ut_load_json_to_sqlserver_dag()