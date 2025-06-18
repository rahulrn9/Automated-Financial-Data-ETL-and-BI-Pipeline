from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from datetime import datetime, timedelta

from scripts.ingest_data import ingest_data
from scripts.transform_data import transform_data
from scripts.schema_drift_check import schema_drift_check
from scripts.load_data import load_data
from scripts.trigger_pbi_refresh import trigger_pbi_refresh

default_args = {
    "owner": "data-eng-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etl_financial_reporting",
    default_args=default_args,
    description="Hourly ETL for intraday trading data",
    schedule_interval="@hourly",
    start_date=datetime(2023, 10, 1),
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="ingest_intraday",
        python_callable=ingest_data,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id="transform_intraday",
        python_callable=transform_data,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id="schema_drift_check",
        python_callable=schema_drift_check,
        provide_context=True
    )

    t4 = PythonOperator(
        task_id="load_intraday",
        python_callable=load_data,
        provide_context=True
    )

    t5 = AwsGlueJobOperator(
        task_id="run_batch_glue",
        job_name="daily-batch-etl"
    )

    t6 = PythonOperator(
        task_id="refresh_powerbi",
        python_callable=trigger_pbi_refresh
    )

    # DAG sequence
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
