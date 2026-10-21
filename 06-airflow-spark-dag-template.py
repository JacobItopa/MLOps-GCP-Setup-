"""
Production Airflow DAG Template: Containerized PySpark on Kubernetes
====================================================================
Key Highlights & Battle-Tested Safeguards:
1. KubernetesPodOperator runs PySpark serverless (no dedicated Spark cluster idle memory waste).
2. security_context={'run_as_user': 0} eliminates Linux RLIMIT_NPROC (ulimit -u) thread ceilings.
3. export _JAVA_OPTIONS="-Xss512k" & MALLOC_ARENA_MAX=2 prevents JVM thread stack OOM / EAGAIN.
4. S3A thread pool capped at 20 with 40 connections to prevent multipart upload deadlocks.
5. Dynamic Git-clone pulls the latest scripts on every execution.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG

try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ImportError:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

# ==============================================================================
# PIPELINE CONFIGURATION: Customize these for your project / cluster
# ==============================================================================
NAMESPACE = os.environ.get("AIRFLOW_NAMESPACE", "ml-workshop")
DAG_GIT_REPO = os.environ.get("DAG_GIT_REPO", "https://github.com/<YOUR_USER>/<YOUR_DAG_REPO>.git")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "quay.io/jupyter/pyspark-notebook:latest")

default_args = {
    'owner': 'ml-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'spark_mlops_data_pipeline',
    default_args=default_args,
    description='Automated Spark data pipeline: extracts, enriches, cleans, and stores Parquet in MinIO',
    schedule_interval='@daily',  # Runs every day at midnight UTC; set to None for on-demand
    catchup=False,
    tags=['mlops', 'spark', 'production'],
) as dag:

    # Task 1: Ingest & Enrich Data
    ingest_task = KubernetesPodOperator(
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/bin/bash', '-c'],
        arguments=[
            'ulimit -u 65535 2>/dev/null || true && '
            'export PATH="/opt/conda/bin:$PATH" && '
            'export _JAVA_OPTIONS="-Xss512k" && '
            'export MALLOC_ARENA_MAX=2 && '
            'python -m pip install -q pyspark psycopg2-binary minio && '
            'rm -rf /tmp/app && '
            f'git clone --depth 1 {DAG_GIT_REPO} /tmp/app && '
            'cd /tmp/app && '
            'python scripts/merge_data.py'
        ],
        security_context={'run_as_user': 0},
        labels={'app': 'spark-ingest-task'},
        name='spark-ingest',
        task_id='ingest_and_merge_data',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task 2: Clean & Feature Engineer Data
    clean_task = KubernetesPodOperator(
        namespace=NAMESPACE,
        image=SPARK_IMAGE,
        cmds=['/bin/bash', '-c'],
        arguments=[
            'ulimit -u 65535 2>/dev/null || true && '
            'export PATH="/opt/conda/bin:$PATH" && '
            'export _JAVA_OPTIONS="-Xss512k" && '
            'export MALLOC_ARENA_MAX=2 && '
            'python -m pip install -q pyspark psycopg2-binary minio && '
            'rm -rf /tmp/app && '
            f'git clone --depth 1 {DAG_GIT_REPO} /tmp/app && '
            'cd /tmp/app && '
            'python scripts/clean_data.py'
        ],
        security_context={'run_as_user': 0},
        labels={'app': 'spark-clean-task'},
        name='spark-clean',
        task_id='clean_and_feature_engineer',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Pipeline execution order
    ingest_task >> clean_task
