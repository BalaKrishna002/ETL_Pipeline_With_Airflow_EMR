from airflow import DAG
from datetime import datetime
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
import os
import json
import boto3
from airflow.models import Variable



# CONFIG LOADER
config_cache = None

def load_config():
    global config_cache

    if config_cache:
        return config_cache

    source = os.getenv("CONFIG_SOURCE", "local")

    if source == "local":
        with open("/opt/airflow/config/e_commerce_pipeline_configs.json") as f:
            config_cache = json.load(f)
    else:
        s3 = boto3.client("s3")
        obj = s3.get_object(
            Bucket=Variable.get("CONFIG_BUCKET"),
            Key=Variable.get("CONFIG_KEY")
        )
        config_cache = json.loads(obj["Body"].read())

    return config_cache



# BUILD EMR CONFIG
def get_emr_config_fn(**context):
    config = load_config()
    return config["emr_config"]



# BUILD SILVER STEP
def build_silver_step_fn(**context):
    config = load_config()
    paths = config["paths"]
    ds = context["ds"]

    return [{
        'Name': 'Silver Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                f"{paths['scripts_bucket']}/silver_job.py",
                '--input', f"{paths['bronze']}/ingestion_date={ds}/",
                '--output', paths['silver']
            ]
        }
    }]



# BUILD GOLD STEP
def build_gold_step_fn(**context):
    config = load_config()
    paths = config["paths"]

    return [{
        'Name': 'Gold Job',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                f"{paths['scripts_bucket']}/gold_job.py",
                '--input', paths['silver'],
                '--output', paths['gold']
            ]
        }
    }]



# DAG
with DAG(
    dag_id="e-commerce_orders_data_pipeline",
    start_date=datetime(2026, 4, 20),
    schedule=None,
    catchup=False,
    default_args={"retries": 0}
) as dag:

    # Get EMR Config
    get_emr_config = PythonOperator(
        task_id="get_emr_config",
        python_callable=get_emr_config_fn
    )

    # Create Cluster
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_cluster",
        job_flow_overrides="{{ ti.xcom_pull(task_ids='get_emr_config') }}",
        aws_conn_id="aws_default",
        emr_conn_id=None,
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=40
    )

    # Build Steps
    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=build_silver_step_fn
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold_step_fn
    )

    # Add Steps to EMR
    add_silver = EmrAddStepsOperator(
        task_id="add_silver",
        job_flow_id=create_cluster.output,
        steps="{{ ti.xcom_pull(task_ids='build_silver') }}",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=120
    )

    add_gold = EmrAddStepsOperator(
        task_id="add_gold",
        job_flow_id=create_cluster.output,
        steps="{{ ti.xcom_pull(task_ids='build_gold') }}",
        aws_conn_id="aws_default",
        wait_for_completion=True,
        waiter_delay=30,
        waiter_max_attempts=120
    )

    # Terminate Cluster
    terminate = EmrTerminateJobFlowOperator(
        task_id="terminate_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule=TriggerRule.ALL_DONE,
        aws_conn_id="aws_default"
    )

    # DAG FLOW
    # Parallel preparation
    [get_emr_config, build_silver, build_gold] >> create_cluster

    # Execution flow
    create_cluster >> add_silver >> add_gold >> terminate
