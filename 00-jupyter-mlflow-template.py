"""
JupyterLab MLflow & MinIO Tracking Template
===========================================
You can use either the automated 1-line helper (mlflow_util) or the manual configuration below.
"""

# ==============================================================================
# METHOD 1: Clean 1-Liner using mlflow_util (Recommended)
# ==============================================================================
import mlflow_util

# Initializes MLflow, sets tracking URI, configures MinIO credentials, and enables autologging
mlflow = mlflow_util.init_mlflow("FlightsDelay-mluser")

# Read clean parquet data directly from MinIO into a Pandas DataFrame
df = mlflow_util.read_parquet_from_s3("s3://flights-data/flights-clean")


# ==============================================================================
# METHOD 2: Manual In-Notebook Configuration
# ==============================================================================
# import os
# import mlflow
#
# HOST = "http://mlflow:5500"
#
# # Point boto3 to MinIO for model artifact persistence
# os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://minio-ml-workshop:9000' # or http://minio:9000
# os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
# os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
# os.environ['AWS_REGION'] = 'us-east-1'
# os.environ['AWS_BUCKET_NAME'] = 'mlflow'
#
# mlflow.set_tracking_uri(HOST)
# mlflow.set_experiment("FlightsDelay-mluser")
# mlflow.sklearn.autolog(log_input_examples=True)
