# 🚀 Production MLOps Platform Manifests & Playbook (GCP & Kubernetes)

This repository contains the complete, battle-tested Kubernetes manifests, automation scripts, and Airflow DAG templates required to deploy an enterprise-grade, end-to-end MLOps platform from scratch on **Google Cloud Platform (GCP)** and Kubernetes.

---

## 🏗️ Architecture & Component Index

```text
Production_MLOps_Manifests/
├── 00-gcp-setup.sh                          # GCP VM, SSD Disk, and VPC Firewall automation script
├── 00-jupyter-mlflow-template.py             # Reusable interactive notebook/script for training
├── 01-minio.yaml                            # Production MinIO S3 Object Storage + Auto-Bucket Bootstrap Job
├── 01-jupyterlab.yaml                       # JupyterLab deployment with 20GB PersistentVolume
├── 02-mlflow.yaml                           # MLflow 2.x Tracking Server + Model Registry
├── 03-seldon-example.yaml                   # Baseline single-pod SeldonDeployment reference
├── 04-spark-cluster.yaml                    # Dedicated standalone Spark cluster manifest
├── 05-airflow-values.yaml                   # Production Apache Airflow Helm values (Git-Sync + RBAC)
├── 06-airflow-spark-dag-template.py         # Airflow DAG: Serverless PySpark ETL on Kubernetes
├── 07-airflow-seldon-deploy-dag-template.py # Airflow DAG: Kaniko Build & Seldon Ingress Deployment
├── 08-seldon-servicemonitor.yaml            # Prometheus Operator ServiceMonitor for Seldon Core
├── 09-grafana-seldon-dashboard.json         # Production Grafana Dashboard (Latency, Reqs, Success %)
├── 10-test-inference.py                     # Multi-iteration latency & prediction test client
├── mlflow_util.py                           # 1-line MLflow + MinIO/S3 auto-logger & Parquet loader
├── spark_util.py                            # Production PySpark S3A connector & JVM thread tuner
├── scripts/
│   ├── merge_data.py                        # Step 1: PostgreSQL extraction + MinIO broadcast join
│   └── clean_data.py                        # Step 2: Constant column pruning + vectorized target label
└── seldon_serving/
    ├── Dockerfile                           # Modern Python 3.11-slim-bookworm microservice image
    ├── base_requirements.txt                # Seldon Core, Scikit-learn, Category Encoders wheels
    ├── Transformer.py                       # Stage 1: Categorical ordinal encoder transformer
    ├── Predictor.py                         # Stage 2: Scikit-Learn Random Forest predictor
    ├── SeldonDeploy.yaml                    # Production SeldonDeployment template (2500Mi memory)
    ├── Ingress.yaml                         # NGINX Ingress template for external prediction traffic
    ├── build_push_image.py                  # In-cluster Kaniko build & multi-auth push runner
    └── deploy_model.py                      # Dynamic Jinja2 renderer & OpenShift/K8s applier
```

---

## 🌐 Phase 1: GCP Infrastructure Provisioning

To prevent common cloud bottlenecks (disk exhaustion and CPU scheduling starvation), provision your GCP VM using [`00-gcp-setup.sh`](./00-gcp-setup.sh):

```bash
# 1. Provision GCP VM + Firewall rules
chmod +x 00-gcp-setup.sh
./00-gcp-setup.sh <PROJECT_ID> <ZONE> <INSTANCE_NAME>

# 2. SSH into your newly created VM
gcloud compute ssh <INSTANCE_NAME> --zone=<ZONE>

# 3. Start Minikube with production resources and enable Ingress
minikube start --cpus=6 --memory=24576 --disk-size=100g --driver=docker
minikube addons enable ingress
```

### 🛡️ GCP Production Sizing Rules:
1. **Compute Engine VM (`n2-standard-8` or `c4d-standard-8`):** Minimum **8 vCPUs and 32GB RAM**. Running Airflow (Scheduler, Web, Worker), MLflow, Jupyter, PostgreSQL, Seldon inference pods, and Prometheus simultaneously requires ~7.0 cores of requests.
2. **Boot Disk (`150GB pd-ssd`):** Never use standard 50-80GB disks. Pulling container builder base images and accumulating intermediate Docker/containerd layers consumes 40-70GB over time.
3. **VPC Firewall Rules:**
   - Ingress `80, 443`: Public API & Ingress routing (`nip.io`).
   - Web UIs `8080` (Airflow), `5500` (MLflow), `8888` (JupyterLab), `3000` (Grafana), `9090` (Prometheus).

---

## 💾 Phase 2: Platform Foundation (Database & S3 Storage)

Deploy PostgreSQL metadata store and MinIO S3 Object Storage:

```bash
# 1. Create dedicated MLOps namespace
kubectl create namespace ml-workshop

# 2. Deploy PostgreSQL (Backend metadata store for Airflow & MLflow)
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install mlops-db bitnami/postgresql \
  --set auth.postgresPassword=admin \
  --namespace ml-workshop

# 3. Deploy MinIO S3 Object Storage & Auto-Bootstrap Buckets
kubectl apply -f 01-minio.yaml -n ml-workshop
```

> **Automated MinIO Bootstrap:** [`01-minio.yaml`](./01-minio.yaml) provisions a 50GB persistent volume, runs MinIO (ports 9000 API / 9001 Console), and automatically executes a bootstrap Kubernetes Job that creates the required S3 buckets (`mlflow`, `flights-data`, `airport-data`, `rawdata`) with open access policies.

---

## 📓 Phase 3: Interactive Training (JupyterLab + MLflow 2.x)

Deploy persistent JupyterLab and the MLflow Tracking Server:

```bash
# Deploy JupyterLab (Workspace PVC + PySpark environment)
kubectl apply -f 01-jupyterlab.yaml -n ml-workshop

# Deploy MLflow 2.x Tracking Server & Model Registry
kubectl apply -f 02-mlflow.yaml -n ml-workshop
```

- **JupyterLab Access:** Port `8888` (Default token: `workshop`).
- **MLflow Access:** Port `5500` (Points to `s3://mlflow/` and `mlops-db`).

### Using [`mlflow_util.py`](./mlflow_util.py) in Notebooks:
```python
from mlflow_util import init_mlflow, load_dataset_from_s3

# 1-line MLflow + MinIO connection
client = init_mlflow(
    host="http://mlflow:5500",
    experiment_name="My-Production-Experiment",
    s3_endpoint="http://minio-ml-workshop:9000"
)

# Load clean parquet training data directly from S3
df = load_dataset_from_s3("s3://flights-data/flights-clean")
```

---

## ⏱️ Phase 4: Production Airflow with GitOps

Deploy Apache Airflow with **Git-Sync** and dynamic **KubernetesPodOperator** RBAC permissions:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm install airflow apache-airflow/airflow \
  -f 05-airflow-values.yaml \
  --namespace ml-workshop
```

> **Key Configs in [`05-airflow-values.yaml`](./05-airflow-values.yaml):** Disables redundant subchart PostgreSQL (`postgresql.enabled: false`), connects to shared `mlops-db`, enables automated Git-Sync from your GitHub repo, and generates the `airflow-worker-sa` ServiceAccount with pod creation RBAC.

### Airflow UI Configuration (`Admin -> Variables`):
Configure the following Airflow Variables in the Airflow UI (accessible at `http://<EXTERNAL_IP>:8080`):

| Variable Key | Purpose | Example Value |
| :--- | :--- | :--- |
| `DOCKERHUB_USER` | Container registry username | `y2jay1` |
| `DOCKERHUB_PASSWORD` | Registry access token / password | `dckr_pat_...` |
| `MODEL_NAME` | Name of registered model in MLflow | `flighs-ontime` |
| `MODEL_VERSION` | Version of model to serve | `1` |
| `CLUSTER_DOMAIN_NAME` | Base DNS for external ingress | `192.168.49.2.nip.io` |
| `SERVING_REPO_URL` | GitHub repository containing serving code | `https://github.com/JacobItopa/Machine-Learning-on-Kubernetes.git` |

---

## ⚡ Phase 5: Serverless Spark Pipelines on Kubernetes

Instead of keeping static Spark clusters running 24/7 (which waste 4-6 cores of CPU), use [`06-airflow-spark-dag-template.py`](./06-airflow-spark-dag-template.py).

### 5 Battle-Tested Production Safeguards in [`spark_util.py`](./spark_util.py):
1. **Container PATH Resolution (`export PATH="/opt/conda/bin:$PATH"`):** Resolves non-login shell Python path issues.
2. **Root Execution (`run_as_user: 0`):** Prevents host-wide `pthread_create failed (EAGAIN)` thread exhaustion on non-root UIDs.
3. **JVM Thread Tuning (`-Xss512k`, `MALLOC_ARENA_MAX=2`):** Halves JVM memory footprint and prevents fragmentation crashes.
4. **S3A Multipart Upload Tuning (`threads.max=20`, `fast.upload.buffer=disk`):** Eliminates Hadoop TransferManager thread deadlocks on large parquet writes.
5. **Vectorized Spark SQL:** Replaces row-by-row `@udf` decorators with native `pyspark.sql.functions.when` (30x speedup).

---

## 🚢 Phase 6: Automated Container Build (Kaniko) & Seldon Serving

### 1. Install Seldon Core Operator:
Ensure the Seldon Core Operator and Custom Resource Definitions (CRDs) are installed on your Kubernetes cluster:

```bash
helm repo add seldonio https://storage.googleapis.com/seldon-charts
helm repo update
helm install seldon-core-operator seldonio/seldon-core-operator \
  --namespace seldon-system \
  --create-namespace \
  --set usageMetrics.enabled=false \
  --set istio.enabled=false
```

### 2. Run the Airflow Seldon Deployment DAG:
Execute [`07-airflow-seldon-deploy-dag-template.py`](./07-airflow-seldon-deploy-dag-template.py) to achieve zero-touch model deployment:

```
[ MLflow Registry ] ──► [ Task 1: Kaniko Builder ] ──► [ Docker Hub / Artifact Registry ]
                                                               │
[ Seldon Core Pod ] ◄── [ Task 2: Deployer Pod ]  ◄────────────┘
```

- **Task 1: In-Cluster Build ([`seldon_serving/build_push_image.py`](./seldon_serving/build_push_image.py)):**
  - Fetches `model.pkl` and `FlgithsDelayOrdinalEncoder.pkl` directly from S3/MinIO via MLflow 2.x REST metadata API.
  - Mounts universal multi-auth Docker credentials.
  - Compiles [`seldon_serving/Dockerfile`](./seldon_serving/Dockerfile) using Kaniko and pushes to Docker Hub without needing a local Docker daemon.
- **Task 2: Seldon Core Serving ([`seldon_serving/SeldonDeploy.yaml`](./seldon_serving/SeldonDeploy.yaml)):**
  - **2-Stage Inference Graph:** `Transformer` (categorical ordinal encoding) $\to$ `Predictor` (Scikit-Learn `predict_proba`).
  - **Memory Sizing:** Limits set to **`2500Mi`** (prevents `OOMKilled` Exit Code 137 during Python deserialization).
  - Automatically provisions NGINX Ingress: `http://flighs-ontime.<IP>.nip.io/api/v1.0/predictions`.

---

## 📊 Phase 7: Observability (Prometheus & Grafana)

Connect Seldon Core metrics into Prometheus and Grafana for real-time monitoring:

### 1. Install Prometheus Operator & Grafana Stack:
If Prometheus & Grafana are not already running in your cluster, deploy them via Helm:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace ml-workshop \
  --set grafana.service.type=ClusterIP \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

### 2. Apply Seldon ServiceMonitor:
```bash
kubectl apply -f 08-seldon-servicemonitor.yaml -n ml-workshop
```
> **Key Fix:** Uses `port: http` instead of `port: "8000"`, matching Seldon Core's named port so targets are never dropped (`droppedTargetCounts: 0`).

### 3. Import Production Grafana Dashboard:
- File: [`09-grafana-seldon-dashboard.json`](./09-grafana-seldon-dashboard.json)
- Go to Grafana $\to$ **Dashboards** $\to$ **Import** $\to$ Upload JSON.
- Select your **Prometheus** data source in the dropdown.
- **Panels Included:** Global Request Rate, Success %, 4xx/5xx error counters, Reqs/sec per model image, and Latency Percentiles (p50, p75, p90, p95, p99).

### 4. Run Live Prediction Benchmark:
```bash
python 10-test-inference.py --url http://flighs-ontime.<IP>.nip.io/api/v1.0/predictions --count 20
```

---

## 🛠️ Production Troubleshooting Reference

| Symptom | Root Cause | Permanent Resolution |
| :--- | :--- | :--- |
| `Pod took too long to start` / `Pending` | 0/1 nodes available: `Insufficient cpu` | Prune inactive Spark clusters (`kubectl delete sparkcluster ...`) or scale VM to 8 vCPU (`n2-standard-8`). |
| `no space left on device` during image pull | VM root disk saturated with old layers | Prune exited containers (`crictl rmi --prune`) and provision GCP disks with $\ge$ 150GB SSD. |
| MinIO buckets missing on startup | MinIO initialized without automated bucket creation | Use [`01-minio.yaml`](./01-minio.yaml) which includes the automated `aws-cli` bootstrap Job. |
| Airflow installs duplicate PostgreSQL pod | Subchart default is `postgresql.enabled: true` | Ensure `postgresql.enabled: false` is declared in [`05-airflow-values.yaml`](./05-airflow-values.yaml). |
| Predictor container `OOMKilled` (Exit 137) | Model deserialization exceeds 1GiB | Set `limits.memory: 2500Mi` in `SeldonDeploy.yaml`. |
| Ingress returns `404 Not Found` | Hostname typo or missing Ingress rule | Verify `kubectl get ingress` and ensure exact hostname match (`grafana` vs `grafna`). |
| Prometheus shows `droppedTargetCounts: 71` | ServiceMonitor port name mismatch | Use named port `port: http` in `ServiceMonitor` spec. |
| Grafana panels display `N/A` for Success/Errors | Metric label mismatch | Query Seldon's native `code="200"` instead of legacy `status` label. |
