"""
DAG de Airflow: orquesta el pipeline GDELT cada 15 minutos.
1. Loader descarga datos GDELT → Parquet (volumen compartido)
2. Spark ejecuta análisis → MongoDB
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Volumen Docker creado por docker compose (name: gdelt-analytics)
PARQUET_VOLUME = os.getenv("PARQUET_VOLUME_NAME", "gdelt-analytics_parquet_data")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK_NAME", "gdelt-analytics_gdelt-net")

PARQUET_MOUNT = Mount(
    source=PARQUET_VOLUME,
    target="/data/parquet",
    type="volume",
)

default_args = {
    "owner": "gdelt-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gdelt_pipeline",
    default_args=default_args,
    description="Pipeline GDELT: Loader → Spark → MongoDB",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt", "spark", "mongodb"],
) as dag:

    run_loader = DockerOperator(
        task_id="gdelt_loader",
        image="gdelt-analytics-loader",
        api_version="auto",
        auto_remove=True,
        command="python loader.py --once",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[PARQUET_MOUNT],
        environment={
            "PARQUET_PATH": "/data/parquet",
            "RAW_RETENTION_HOURS": "1",
            "GDELT_BASE_URL": "http://data.gdeltproject.org/gdeltv2",
        },
    )

    run_spark_analysis = DockerOperator(
        task_id="spark_analysis",
        image="gdelt-analytics-spark-analysis",
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[PARQUET_MOUNT],
        environment={
            "SPARK_MASTER": "spark://spark-master:7077",
            "PARQUET_PATH": "/data/parquet",
            "MONGO_URI": "mongodb://admin:gdelt2026@mongodb:27017/gdelt_analytics?authSource=admin",
        },
    )

    run_loader >> run_spark_analysis
