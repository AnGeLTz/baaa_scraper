from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor
import os

default_args = {
    'start_date' : datetime(2023,6,12),
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

def run_scrapper():
    print("Current working directory:", os.getcwd())
    import fetch_daily
    fetch_daily.run()


with DAG(dag_id = 'Fetch_daily',
        description = 'Gather todays links from accidents archives',
        default_args = default_args,
        schedule_interval='0 * * * *'
) as dag:

    wait_for_cleansing = ExternalTaskSensor(
        task_id='wait_for_cleansing',
        external_dag_id='cleansing',  # Specify the DAG ID to wait for
        external_task_id=None,  # Wait for any task in the "scraper_data" DAG to complete
        mode='reschedule',  # The sensor will reschedule until the dependency is met
        poke_interval=60,  # Check for completion every minute
        timeout=60 * 60 * 24,  # Timeout after 24 hours of waiting
        dag=dag
    )

    fetch_daily = PythonOperator(
        task_id = 'fetch_daily',
        python_callable = run_scrapper,
        dag = dag
    )

    fetch_daily

    