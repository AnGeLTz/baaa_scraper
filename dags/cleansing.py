from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'start_date' : datetime(2023,6,6)
}

with DAG(dag_id = 'cleansing',
        description = "Clean raw data and put them in the analitics schema",
        default_args = default_args,
        schedule_interval = '@once'
) as dag:

    cleansing = PostgresOperator(
        task_id = 'cleansing',
        postgres_conn_id = 'postdb',
        sql = 'cleansing.sql',
        database = 'baaa'
    )

    cleansing

