import os
import socket
import sys
import time
import subprocess


# Robust JAVA_HOME resolution
if "JAVA_HOME" not in os.environ:
    if os.path.exists("/opt/conda/bin/java"):
        os.environ["JAVA_HOME"] = "/opt/conda"
    else:
        try:
            java_path = subprocess.run(["which", "java"], capture_output=True, text=True).stdout.strip()
            if java_path and os.path.exists(java_path):
                real_java = os.path.realpath(java_path)
                os.environ["JAVA_HOME"] = os.path.dirname(os.path.dirname(real_java))
        except Exception:
            pass


def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])


def get_cluster_name(index=0, from_env=False):
    # Get the cluster name from env var, spark-info.txt, or default
    if os.path.exists('spark-info.txt') and not from_env:
        with open('spark-info.txt', 'r') as sparkinfo:
            lines = sparkinfo.readlines()
            if lines:
                spark_cluster_name = lines[0 + (index * 2)].replace("\n", "").strip()
                os.environ['SPARK_CLUSTER'] = spark_cluster_name
                return spark_cluster_name

    cluster = os.environ.get('SPARK_CLUSTER', 'spark-cluster-sample')
    if cluster.startswith('spark-cluster-') or cluster.startswith('local'):
        return cluster
    return f"spark-cluster-{cluster}"


def get_driver_host_ip():
    # Returns the IP address where this process is running on
    try:
        ip_address = socket.gethostbyname(socket.gethostname())
    except Exception:
        ip_address = "127.0.0.1"
    return ip_address


def init_environment(submit_args):
    # Prepares Spark related configuration through PYSPARK_SUBMIT_ARGS env variable
    cluster_name = get_cluster_name()
    # Default to local[1] to minimize thread creation in containerized Kubernetes environments
    master_target = os.environ.get('SPARK_MASTER', 'local[1]')
    
    # If the user explicitly sets SPARK_MASTER=cluster, connect to standalone master
    if master_target == "cluster":
        master_url = f"spark://{cluster_name}:7077"
    else:
        master_url = master_target

    # Ensure optimal driver memory for 5.8M row dataset
    if "--driver-memory" not in submit_args and "spark.driver.memory" not in submit_args:
        submit_args = f"--driver-memory 2g {submit_args}"

    # Strict thread and resource limits to prevent OS-level EAGAIN / thread exhaustion
    thread_safety_confs = (
        "--conf spark.ui.enabled=false "
        "--conf spark.driver.extraJavaOptions=-Xss512k "
        "--conf spark.executor.extraJavaOptions=-Xss512k "
        "--conf spark.sql.shuffle.partitions=4 "
        "--conf spark.default.parallelism=1 "
        "--conf spark.shuffle.io.serverThreads=1 "
        "--conf spark.shuffle.io.clientThreads=1 "
        "--conf spark.cleaner.referenceTracking.blocking=false "
        "--conf spark.hadoop.fs.s3a.threads.max=20 "
        "--conf spark.hadoop.fs.s3a.connection.maximum=40 "
        "--conf spark.hadoop.fs.s3a.fast.upload.buffer=disk "
    )
    submit_args = f"{thread_safety_confs} {submit_args}"

    # PySpark 3.4/3.5 bundles Hadoop 3.3.x runtime libraries.
    # Legacy hadoop-aws:3.2.0 causes NoSuchMethodError on SemaphoredDelegatingExecutor during S3A write operations.
    # Automatically upgrade hadoop-aws:3.2.0 to 3.3.4 + matching aws-java-sdk-bundle
    if "hadoop-aws:3.2.0" in submit_args:
        submit_args = submit_args.replace(
            "org.apache.hadoop:hadoop-aws:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

    os.environ['PYSPARK_SUBMIT_ARGS'] = f"{submit_args} --master {master_url} pyspark-shell "


def configure_minio_s3a(spark):
    """
    Configures S3A settings for self-hosted MinIO to prevent:
    - ClassNotFoundException (missing AWS SDK v2 classes)
    - NumberFormatException (30s, 60s, 24h string values in Hadoop 3.2)
    - OutOfMemoryError: unable to create native thread (capping S3A thread pool)
    - Heap exhaustion during multipart upload (buffering to disk instead of RAM)
    """
    try:
        # Dynamically resolve MinIO endpoint and credentials from environment variables with sensible defaults
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio-ml-workshop:9000"))
        minio_access_key = os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ACCESS_KEY", "minio"))
        minio_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_SECRET_KEY", "minio123"))

        hadoop_conf.set("fs.s3a.endpoint", minio_endpoint)
        hadoop_conf.set("fs.s3a.access.key", minio_access_key)
        hadoop_conf.set("fs.s3a.secret.key", minio_secret_key)
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        
        # S3A TransferManager requires at least 20 threads to avoid multipart upload deadlock
        hadoop_conf.set("fs.s3a.threads.max", "20")
        hadoop_conf.set("fs.s3a.connection.maximum", "40")
        
        # Buffer uploads to local container disk instead of consuming JVM heap memory
        hadoop_conf.set("fs.s3a.fast.upload.buffer", "disk")
        hadoop_conf.set("fs.s3a.multipart.size", "67108864")
        
        # Raw numeric overrides for Hadoop compatibility
        hadoop_conf.set("fs.s3a.connection.timeout", "30000")
        hadoop_conf.set("fs.s3a.connection.establish.timeout", "30000")
        hadoop_conf.set("fs.s3a.connection.request.timeout", "30000")
        hadoop_conf.set("fs.s3a.threads.keepalivetime", "60")
        hadoop_conf.set("fs.s3a.multipart.purge", "false")
        hadoop_conf.set("fs.s3a.multipart.purge.age", "86400")
    except Exception as e:
        print(f"Warning: Could not configure Hadoop S3A settings: {e}")


def getOrCreateSparkSession(application_name, submit_args="", log_level="INFO"):
    # Creates a Spark session from a given Spark application name and optional submit_args

    try:
        from pyspark.sql import SparkSession
    except ImportError:
        install("pyspark")
        from pyspark.sql import SparkSession

    print("Initializing environment variables for Spark")
    init_environment(submit_args)
    
    # Use 127.0.0.1 for local mode, or driver IP for cluster mode
    master_target = os.environ.get('SPARK_MASTER', 'local[1]')
    bind_ip = "127.0.0.1" if master_target.startswith("local") else get_driver_host_ip()

    sparkSessionBuilder = SparkSession.builder \
        .config("spark.driver.host", bind_ip) \
        .config("spark.driver.bindAddress", bind_ip) \
        .appName(application_name)

    print("Creating a spark session...")
    spark = sparkSessionBuilder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    
    # Apply MinIO S3A configuration fixes automatically
    configure_minio_s3a(spark)
    
    print("Spark session created")
    return spark
