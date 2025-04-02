from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append('/opt/airflow/scripts/src')
from extract import extract
from load import load
from transform import run_queries
from config import (
    DATASET_ROOT_PATH,
    SQLITE_BD_ABSOLUTE_PATH,
    get_csv_to_table_mapping,
    PUBLIC_HOLIDAYS_URL
)

from sqlalchemy import create_engine


engine = create_engine(f"sqlite:///{SQLITE_BD_ABSOLUTE_PATH}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id="elt_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="Orquestación del pipeline ELT de Olist con Airflow",
    tags=["elt", "olist"]
) as dag:

    def extract_task():
        return extract(
            csv_folder=DATASET_ROOT_PATH,
            csv_table_mapping=get_csv_to_table_mapping(),
            public_holidays_url=PUBLIC_HOLIDAYS_URL
        )

    def load_task(**context):
        extracted_data = context['ti'].xcom_pull(task_ids='extract_data')
        load(extracted_data, engine)

    def transform_task():
        run_queries(engine)

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task
    )

    t2 = PythonOperator(
        task_id="load_data",
        python_callable=load_task,
        provide_context=True
    )

    t3 = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    t1 >> t2 >> t3