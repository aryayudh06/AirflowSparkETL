from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_etl_jan_feb_concurrent",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:

    process_january = SparkSubmitOperator(
        task_id="process_taxi_data_january",
        application="/opt/airflow/dags/spark_jobs/taxi_etl.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--input", "/opt/airflow/data/january",
            "--output", "mongodb://host.docker.internal:27017/nyc_taxi.trips_january_2025"
        ],
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.executor.instances": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.cores.max": "2",
            "spark.default.parallelism": "2",
            "spark.sql.shuffle.partitions": "2",          
        },
        executor_memory="1g",
    )

    process_february = SparkSubmitOperator(
        task_id="process_taxi_data_february",
        application="/opt/airflow/dags/spark_jobs/taxi_etl.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--input", "/opt/airflow/data/february",
            "--output", "mongodb://host.docker.internal:27017/nyc_taxi.trips_february_2025"
        ],
        packages="org.mongodb.spark:mongo-spark-connector_2.12:10.3.0",
        conf={
            "spark.executor.instances": "2",
            "spark.executor.cores": "1",
            "spark.executor.memory": "1g",
            "spark.cores.max": "2",
            "spark.default.parallelism": "2",
            "spark.sql.shuffle.partitions": "2",       
        },
        executor_memory="1g",
    )

    [process_january, process_february]
