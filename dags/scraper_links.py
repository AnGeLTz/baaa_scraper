from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os

default_args = {
    'start_date' : datetime(2023,1,1)
}

def run_scrapper():
    print("Current working directory:", os.getcwd())
    import scrapper_links
    scrapper_links.main()

with DAG(dag_id = 'scrapper_links',
        description = 'Gather all links from accidents archives',
        default_args = default_args,
        schedule_interval = '@once'
) as dag1:

    run_scrapper_links = PythonOperator(
        task_id = 'run_scrapper_links',
        python_callable = run_scrapper,
        dag = dag1
    )

    # Trigger the execution of 'db_config' DAG from 'scrapper_links' DAG
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_db_config',
        trigger_dag_id='db_config'
    )

    run_scrapper_links >> trigger_dag