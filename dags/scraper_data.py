from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

default_args = {
    'start_date' : datetime(2023,1,1)
}

def run_scrapper_data():
    print("Current working directory:", os.getcwd())
    import scrapper_data
    scrapper_data.main()

with DAG(dag_id = 'scrapper_data',
        description = 'Gather all data from the urls.json links',
        default_args = default_args,
        schedule_interval = '@once'
) as dag:

    run_scrapper_data = PythonOperator(
        task_id = 'run_scrapper_data',
        python_callable = run_scrapper_data,
        dag = dag
    )

    # Trigger the execution of 'cleansing' DAG from 'scrapper_data' DAG
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_cleansing',
        trigger_dag_id='cleansing'
    )

    run_scrapper_data >> trigger_dag
