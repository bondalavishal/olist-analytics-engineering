from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "retries": 0
}

with DAG(
    dag_id="olist_analytics_pipeline",
    start_date=datetime(2026, 2, 15),  # start now
    schedule="* * * * *",             # change replay speed here
    catchup=False,                    # no historical explosion
    max_active_runs=1,                # strictly sequential
    default_args=default_args,
    tags=["olist", "analytics_engineering"],
) as dag:

    ingest = BashOperator(
        task_id="incremental_ingestion",
        bash_command="""
        cd ~/Applications/Softwares/git/olist-analytics-engineering &&
        source venv/bin/activate &&
        python ingestion/incremental_loader.py
        """
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd ~/Applications/Softwares/git/olist-analytics-engineering/dbt/olist_project &&
        source ~/Applications/Softwares/dbt/.venv/bin/activate &&
        dbt run
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd ~/Applications/Softwares/git/olist-analytics-engineering/dbt/olist_project &&
        source ~/Applications/Softwares/dbt/.venv/bin/activate &&
        dbt test
        """
    )

    ingest >> dbt_run >> dbt_test