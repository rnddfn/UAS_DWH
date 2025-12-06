from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_docker_demo',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['uas', 'etl']
) as dag:

    run_etl = DockerOperator(
        task_id='run_etl_container',
        image='my-etl:latest',          # nama image yang sudah kamu build
        api_version='auto',
        auto_remove=True,
        command=None,                   # gunakan CMD di Dockerfile (python etl.py)
        mount_tmp_dir=False,            # jangan mount tmp dir host (menghindari invalid bind path)
        docker_url='unix:///var/run/docker.sock',
        network_mode='airflow-net',     # cocokkan dengan network di docker-compose
        environment={
            'POSTGRES_HOST': 'db_postgres',
            'POSTGRES_USER': 'admin',
            'POSTGRES_PASSWORD': 'admin',
            'POSTGRES_DB': 'db_penjualan'
        },
        mounts=[
            Mount(source='/c/Users/USER/Documents/LocalProjects/Kuliah/BI/UAS_Warehouse/data', target='/usr/src/app/data', type='bind', read_only=True),
            Mount(source='/c/Users/USER/Documents/LocalProjects/Kuliah/BI/UAS_Warehouse/logs', target='/usr/src/app/logs', type='bind', read_only=False),
            Mount(source='/c/Users/USER/Documents/LocalProjects/Kuliah/BI/UAS_Warehouse/etl.py', target='/usr/src/app/etl.py', type='bind', read_only=True),
        ],
        # Jika perlu mount volume data, gunakan mount param (depends on provider version)
    )
    run_etl