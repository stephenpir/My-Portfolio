from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_predict_ml_euromillions",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "prediction", "ml"],
    params={
        "ml_filter": Param("all", type="string", enum=["all", "tuesday", "friday"], title="ML Data Filter", description="Filter for the ML prediction data."),
    },
    doc_md="""
    ### Unit Test DAG for `predict_ml_euromillions.py`

    This DAG is designed to run the `predict_ml_euromillions.py` script in isolation for unit testing purposes.
    It allows selecting the data filter via a DAG parameter.
    """,
)
def ut_predict_ml_euromillions_dag():
    run_predict_ml_euromillions = BashOperator(
        task_id="run_predict_ml_euromillions",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_ml_euromillions.py' --filter '{{{{ params.ml_filter }}}}'",
    )

ut_predict_ml_euromillions_dag()