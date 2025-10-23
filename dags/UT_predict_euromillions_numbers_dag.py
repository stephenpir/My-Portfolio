from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="UT_predict_euromillions_numbers",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["unit_test", "euromillions", "prediction", "statistical"],
    params={
        "stat_strategy": Param("most_frequent", type="string", enum=["most_frequent", "least_frequent", "most_overdue", "random"], title="Prediction Strategy", description="Strategy to use for statistical prediction."),
    },
    doc_md="""
    ### Unit Test DAG for `predict_euromillions_numbers.py`

    This DAG is designed to run the `predict_euromillions_numbers.py` script in isolation for unit testing purposes.
    It allows selecting the prediction strategy via a DAG parameter.
    """,
)
def ut_predict_euromillions_numbers_dag():
    run_predict_euromillions_numbers = BashOperator(
        task_id="run_predict_euromillions_numbers",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_euromillions_numbers.py' --strategy '{{{{ params.stat_strategy }}}}'",
    )

ut_predict_euromillions_numbers_dag()