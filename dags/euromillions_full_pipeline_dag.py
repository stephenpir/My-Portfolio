from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

# Define the project's root directory
PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="euromillions_full_pipeline",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule="0 3 * * 3,6",  # Runs at 3 AM on Wednesday and Saturday
    catchup=False,
    tags=["euromillions", "ingestion", "analysis", "export", "pipeline"],
    params={
        "show_validation_details": Param(False, type="boolean", title="Show Validation Details"),
        "show_analysis_details": Param(False, type="boolean", title="Show Analysis Details"),
        "stat_strategy": Param("most_frequent", type="string", enum=["most_frequent", "least_frequent", "most_overdue", "random"]),
        "ml_filter": Param("all", type="string", enum=["all", "tuesday", "friday"], title="ML Model Filter"),
        "pytorch_filter": Param("all", type="string", enum=["all", "tuesday", "friday"], title="PyTorch Model Filter"),
    },
    doc_md="""
    ### EuroMillions End-to-End Data Pipeline

    This DAG orchestrates the complete data workflow for the EuroMillions project.

    **1. Ingestion & Validation:**
    - Scrapes the latest draw history.
    - Loads scraped data into the Oracle database.
    - Loads supplementary source data into the PostgreSQL database.
    - Validates data consistency between Oracle and PostgreSQL.

    **2. Analysis & Prediction (runs after successful validation):**
    - Performs statistical analysis and generates plots.
    - Runs multiple prediction models (statistical, ML, PyTorch).

    **3. Export (runs after successful Oracle load):**
    - Exports data from Oracle to local JSON/CSV files.
    - Loads the JSON data into a SQL Server table.
    """,
)
def euromillions_full_pipeline_dag():
    """
    Orchestrates the entire EuroMillions data pipeline from ingestion to export.
    """

    # --- Ingestion & Validation Tasks ---

    scrape_data = BashOperator(
        task_id="scrape_euromillions_data",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_2/scrape_euromillions_data.py'",
    )

    load_scraped_to_oracle = BashOperator(
        task_id="load_scraped_to_oracle",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_2/load_euromillions_data.py'",
    )

    load_source1_to_pg = BashOperator(
        task_id="load_source1_to_pg",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_1/load_source1_Euromillions_lottery_data_to_postgres.py'",
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=(
            f"python '{PROJECT_ROOT}/Scenario_2/validate_data.py' "
            "{% if params.show_validation_details %}--show-details{% endif %}"
        ),
    )

    # --- Analysis & Prediction Tasks ---

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
        bash_command=f"python '{PROJECT_ROOT}/Scenario_3/predict_pytorch_euromillions.py' --filter '{{{{ params.pytorch_filter }}}}'",
    )

    # --- Export Tasks ---

    export_from_oracle = BashOperator(
        task_id="export_oracle_to_files",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/export_oracle_to_local_files.py'",
    )

    load_to_sqlserver = BashOperator(
        task_id="load_json_to_sqlserver",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/load_json_to_sqlserver.py'",
    )

    # --- Define Task Dependencies ---

    # Ingestion phase
    scrape_data >> load_scraped_to_oracle
    [load_scraped_to_oracle, load_source1_to_pg] >> validate_data

    # Analysis phase (depends on successful validation)
    validate_data >> run_statistical_analysis >> [
        run_stat_prediction,
        run_ml_prediction,
        run_pytorch_prediction,
    ]

    # Export phase (depends on Oracle load, can run in parallel with analysis)
    load_scraped_to_oracle >> export_from_oracle >> load_to_sqlserver

euromillions_full_pipeline_dag()
