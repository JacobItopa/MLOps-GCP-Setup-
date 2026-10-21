"""
Production MLOps Template: Airflow DAG for Seldon Model Serving Deployment
Deploys an MLflow-registered model into a Kubernetes Seldon Core inference graph.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable

try:
    from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
except ImportError:
    from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

DOCKERHUB_USER = Variable.get("DOCKERHUB_USER", default_var="YOUR_DOCKERHUB_USERNAME")
DOCKERHUB_PASSWORD = Variable.get("DOCKERHUB_PASSWORD", default_var="YOUR_DOCKERHUB_TOKEN_OR_PASSWORD")
MODEL_NAME = Variable.get("MODEL_NAME", default_var="flighs-ontime")
MODEL_VERSION = Variable.get("MODEL_VERSION", default_var="1")
IMAGE_TAG = Variable.get("MODEL_IMAGE_TAG", default_var="1.0.0")

CONTAINER_REGISTRY = "https://index.docker.io/v1/"
CONTAINER_DETAILS = f"{DOCKERHUB_USER}/flight-delay-model:{IMAGE_TAG}"
CLUSTER_DOMAIN_NAME = Variable.get("CLUSTER_DOMAIN_NAME", default_var="192.168.49.2.nip.io")
SERVING_REPO_URL = Variable.get("SERVING_REPO_URL", default_var="https://github.com/JacobItopa/Machine-Learning-on-Kubernetes.git")

default_args = {
    'owner': 'ml-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'flights_model_deploy',
    default_args=default_args,
    description='Build model serving container with Kaniko and deploy Seldon inference graph',
    schedule_interval=None,
    catchup=False,
    tags=['mlops', 'seldon', 'model-serving', 'dockerhub'],
) as dag:

    build_push_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/ml-on-k8s/kaniko-container-builder:1.0.0',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'set -e && '
            'echo "=== Downloading serving codebase from repository ===" && '
            'rm -rf /tmp/app /tmp/repo.tar.gz /tmp/Machine-Learning* && '
            f'if command -v git >/dev/null 2>&1; then '
            f'  git clone --depth 1 {SERVING_REPO_URL} /tmp/app; '
            f'else '
            f'  python3 -c "import urllib.request, tarfile; '
            f'urllib.request.urlretrieve(\'{SERVING_REPO_URL.replace(\".git\", \"\")}/archive/refs/heads/main.tar.gz\', \'/tmp/repo.tar.gz\'); '
            f'tarfile.open(\'/tmp/repo.tar.gz\').extractall(\'/tmp\')" && '
            f'  mv /tmp/*-main /tmp/app 2>/dev/null || mv /tmp/Machine-Learning* /tmp/app; '
            f'fi && '
            'if [ -d /tmp/app/seldon_serving ]; then cd /tmp/app/seldon_serving; elif [ -d /tmp/app/chapter10/model_deploy_pipeline/model_build_push ]; then cd /tmp/app/chapter10/model_deploy_pipeline/model_build_push; fi && '
            'echo "=== Running build_push_image.py to download artifacts and invoke Kaniko ===" && '
            'python3 build_push_image.py'
        ],
        env_vars={
            'MODEL_NAME': MODEL_NAME,
            'MODEL_VERSION': MODEL_VERSION,
            'CONTAINER_REGISTRY': CONTAINER_REGISTRY,
            'CONTAINER_REGISTRY_USER': DOCKERHUB_USER,
            'CONTAINER_REGISTRY_PASSWORD': DOCKERHUB_PASSWORD,
            'CONTAINER_DETAILS': CONTAINER_DETAILS,
            'AWS_SECRET_ACCESS_KEY': 'minio123',
            'AWS_ACCESS_KEY_ID': 'minio',
        },
        security_context={'run_as_user': 0},
        labels={'app': 'kaniko-model-builder'},
        name='kaniko-build-flights-model',
        task_id='build_push_image',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    deploy_task = KubernetesPodOperator(
        namespace='ml-workshop',
        image='quay.io/ml-on-k8s/airflow-python-runner:0.0.11',
        cmds=['/bin/bash', '-c'],
        arguments=[
            'set -e && '
            'echo "=== Downloading deployment templates from repository ===" && '
            'rm -rf /tmp/app /tmp/repo.tar.gz /tmp/Machine-Learning* && '
            f'if command -v git >/dev/null 2>&1; then '
            f'  git clone --depth 1 {SERVING_REPO_URL} /tmp/app; '
            f'else '
            f'  python3 -c "import urllib.request, tarfile; '
            f'urllib.request.urlretrieve(\'{SERVING_REPO_URL.replace(\".git\", \"\")}/archive/refs/heads/main.tar.gz\', \'/tmp/repo.tar.gz\'); '
            f'tarfile.open(\'/tmp/repo.tar.gz\').extractall(\'/tmp\')" && '
            f'  mv /tmp/*-main /tmp/app 2>/dev/null || mv /tmp/Machine-Learning* /tmp/app; '
            f'fi && '
            'if [ -d /tmp/app/seldon_serving ]; then cd /tmp/app/seldon_serving; elif [ -d /tmp/app/chapter10/model_deploy_pipeline/model_deploy ]; then cd /tmp/app/chapter10/model_deploy_pipeline/model_deploy; fi && '
            'echo "=== Running deploy_model.py to render and apply SeldonDeployment ===" && '
            'python3 deploy_model.py'
        ],
        env_vars={
            'MODEL_NAME': MODEL_NAME,
            'MODEL_VERSION': MODEL_VERSION,
            'CONTAINER_REGISTRY': CONTAINER_REGISTRY,
            'CONTAINER_DETAILS': CONTAINER_DETAILS,
            'CLUSTER_DOMAIN_NAME': CLUSTER_DOMAIN_NAME,
        },
        security_context={'run_as_user': 0},
        labels={'app': 'seldon-model-deployer'},
        name='seldon-deploy-flights-model',
        task_id='deploy_model',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    build_push_task >> deploy_task
