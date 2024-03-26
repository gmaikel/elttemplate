from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from cosmos import ProfileConfig, ProjectConfig, DbtTaskGroup, ExecutionConfig

from extract_load.utils import ExtractData, LoadData


def extract_schema(schema_name: str) -> None:
    extract_data = ExtractData(
        nom_schema=schema_name
    )
    extract_data.extract()


def load_schema(schema_name: str) -> None:
    load_data = LoadData(
        nom_schema=schema_name
    )
    load_data.load()


profile_config = ProfileConfig(
    profile_name='default',
    target_name='dev',
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="db_conn",
        profile_args={"database": "t_exchange_db",
                      "schema": "dbt_transform"},
    )
)

default_args = {
    'owner': '{{cookiecutter.owner}}',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='elt_dbt_airflow_snowflake_dag',
    default_args=default_args,
    description='ELT pipeline',
    start_date=datetime.today(),
    schedule_interval=None
) as dag:
    start = EmptyOperator(task_id='start', dag=dag)

    with TaskGroup(group_id='extract') as extract_group:
        extract_crypto = PythonOperator(
            task_id='extract_crypto',
            python_callable=extract_schema,
            op_kwargs={'schema_name': 'crypto'}
        )
        extract_forex = PythonOperator(
            task_id='extract_forex',
            python_callable=extract_schema,
            op_kwargs={'schema_name': 'forex'}
        )

        [extract_crypto, extract_forex]

    with TaskGroup(group_id='load') as load_group:
        load_crypto = PythonOperator(
            task_id='load_crypto',
            python_callable=load_schema,
            op_kwargs={'schema_name': 'crypto'}
        )
        load_forex = PythonOperator(
            task_id='load_forex',
            python_callable=load_schema,
            op_kwargs={'schema_name': 'forex'}
        )

        [load_crypto, load_forex]

    dbt_transform = DbtTaskGroup(
        group_id='dbt_transform',
        project_config=ProjectConfig("/opt/airflow/dags/transform"),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"),
        operator_args={"install_deps": True},
    )

    end = EmptyOperator(task_id='end')

    start >> extract_crypto >> load_crypto >> dbt_transform >> end
