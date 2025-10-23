from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_analyze_euromillions_data",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "analysis"],
    params={
        "show_analysis_details": Param(False, type="boolean", title="Show Analysis Details", description="If true, the analysis script will output detailed logs."),
    },
    doc_md="""
    ### Unit Test DAG for `analyze_euromillions_data.py`

    This DAG is designed to run the `analyze_euromillions_data.py` script in isolation for unit testing purposes.
    It allows toggling detailed output via a DAG parameter.
    """,
)
def ut_analyze_euromillions_data_dag():
    run_analyze_euromillions_data = BashOperator(
        task_id="run_analyze_euromillions_data",
        bash_command=(
            f"python '{PROJECT_ROOT}/Scenario_3/analyze_euromillions_data.py' "
            "{% if params.show_analysis_details %}--show-details{% endif %}"
        ),
    )

ut_analyze_euromillions_data_dag()