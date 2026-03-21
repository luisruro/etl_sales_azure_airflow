from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.pipeline import run_pipeline

default_args = {
    'owner': 'luis',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='etl_sales',
    default_args=default_args,
    description='ETL pipeline for sales data',
    schedule='0 9-18 * * 1-5',
    start_date=datetime(2026,1,1),
    catchup=False
) as dag:
    run_etl = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_pipeline
    )
    
    run_etl