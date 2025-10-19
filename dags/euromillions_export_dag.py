from __future__ import annotations

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# Define the project's root directory
#PROJECT_ROOT = "/Users/stephenpir/Desktop/Code/My Portfolio"
PROJECT_ROOT = "/opt/airflow/my_portfolio"

@dag(
    dag_id="euromillions_export_and_load_sqlserver",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    schedule="@daily",  # Runs once a day
    catchup=False,
    tags=["euromillions", "export", "sqlserver"],
    doc_md="""
    ### EuroMillions Data Export DAG

    This DAG moves data from the primary Oracle database to other systems.
    1.  **Exports** data from Oracle to local JSON and CSV files.
    2.  **Loads** the JSON data into a SQL Server table.
    """,
)
def euromillions_export_dag():
    """
    Orchestrates exporting data from Oracle and loading it to SQL Server.
    """
    export_from_oracle = BashOperator(
        task_id="export_oracle_to_files",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/export_oracle_to_local_files.py'",
    )

    load_to_sqlserver = BashOperator(
        task_id="load_json_to_sqlserver",
        bash_command=f"python '{PROJECT_ROOT}/Scenario_4/load_json_to_sqlserver.py'",
    )

    export_from_oracle >> load_to_sqlserver

euromillions_export_dag()