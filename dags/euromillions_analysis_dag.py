from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# Define the project's root directory
#PROJECT_ROOT = "/Users/stephenpir/Desktop/Code/My Portfolio"
PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="euromillions_analysis_and_prediction",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule=None,  # This DAG is meant for manual triggers
    catchup=False,
    tags=["euromillions", "analysis", "prediction", "ml"],
    params={
        "stat_strategy": Param("most_frequent", type="string", enum=["most_frequent", "least_frequent", "most_overdue", "random"]),
        "ml_filter": Param("all", type="string", enum=["all", "tuesday", "friday"]),
        "show_analysis_details": Param(False, type="boolean"),
    },
    doc_md="""
    ### EuroMillions Analysis and Prediction DAG

    This DAG runs various analysis and ML prediction scripts. It is designed for manual execution.
    You can configure the run by passing parameters for prediction strategies and data filters.
    """,
)
def euromillions_analysis_dag():
    """
    Orchestrates running analysis and prediction scripts.
    """

    run_statistical_analysis = BashOperator(
        task_id="run_statistical_analysis",
        bash_command=(
            f"python '{PROJECT_ROOT}/Scenario_3/analyze_euromillions_data.py' "
            "{% if params.show_analysis_details %}--show-details{% endif %}"
        ),
    )

    run_stat_prediction = BashOperator(
        task_id="run_statistical_prediction",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_euromillions_numbers.py' --strategy '{{{{ params.stat_strategy }}}}'",
    )

    run_ml_prediction = BashOperator(
        task_id="run_ml_prediction",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_ml_euromillions.py' --filter '{{{{ params.ml_filter }}}}'",
    )

    run_pytorch_prediction = BashOperator(
        task_id="run_pytorch_prediction",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_pytorch_euromillions.py' --filter '{{{{ params.ml_filter }}}}'",
    )

    # Define task dependencies: Analysis runs first, then all predictions can run in parallel.
    run_statistical_analysis >> [
        run_stat_prediction,
        run_ml_prediction,
        run_pytorch_prediction,
    ]

euromillions_analysis_dag()