from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date' : datetime(2023,1,1)
}

with DAG(dag_id = 'db_config',
        description = "Initialize the database, run only once",
        default_args = default_args,
        schedule_interval = '@once'
) as dag2:

    db_init = PostgresOperator(
        task_id = 'db_init',
        postgres_conn_id = 'postdb',
        sql = 'db_config.sql',
        database = 'baaa'
    )

    # Trigger the execution of 'scrapper_data' DAG from 'db_config' DAG
    trigger_dag = TriggerDagRunOperator(
        task_id='trigger_scrapper_data',
        trigger_dag_id='scrapper_data'
    )

    db_init >> trigger_dag

