from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from lastfm_pipeline import task_ingest_and_bronze, task_silver, task_gold

default_args = {
    'owner': 'matt',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='lastfm_music_pipeline',
    default_args=default_args,
    description='Daily ingestion of Last.fm chart data - bronze, silver and gold layers',
    schedule_interval='@daily',
    start_date=datetime(2026, 3, 25),
    catchup=False,
    tags=['lastfm', 'music', 'medallion']
) as dag:

    ingest_and_bronze = PythonOperator(
        task_id='ingest_and_bronze',
        python_callable=task_ingest_and_bronze
    )

    silver = PythonOperator(
        task_id='silver',
        python_callable=task_silver
    )

    gold = PythonOperator(
        task_id='gold',
        python_callable=task_gold
    )

    # Define task dependencies
    ingest_and_bronze >> silver >> gold