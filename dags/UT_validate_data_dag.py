from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_validate_data",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "validation"],
    params={
        "show_details": Param(False, type="boolean", title="Show Validation Details", description="If true, the validation script will output detailed logs."),
    },
    doc_md="""
    ### Unit Test DAG for `validate_data.py`

    This DAG is designed to run the `validate_data.py` script in isolation for unit testing purposes.
    It allows toggling detailed output via a DAG parameter.
    """,
)
def ut_validate_data_dag():
    run_validate_data = BashOperator(
        task_id="run_validate_data",
        bash_command=(
            f"python '{PROJECT_ROOT}/Scenario_2/validate_data.py' "
            "{% if params.show_details %}--show-details{% endif %}"
        ),
    )

ut_validate_data_dag()