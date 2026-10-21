import os
import sys

# Ensure root directory is on Python path so spark_util can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

import spark_util


def run_clean():
    """
    Step 2: Clean, Prune Constants, and Compute Features
    - Reads merged dataset from MinIO S3 Parquet
    - Drops unnecessary intermediate join keys
    - Automatically discovers and removes single-value (constant) columns using approx_count_distinct
    - Computes target label (DELAYED) using native Spark SQL expressions (vectorized Tungsten engine)
      avoiding Python UDF serialization bottlenecks across millions of rows
    - Writes the cleaned dataset to MinIO S3 in Parquet format
    """
    print("=== Starting Step 2: Clean and Prepare Features ===")

    s3_data_bucket = os.environ.get("S3_DATA_BUCKET", "flights-data")
    delay_threshold = int(os.environ.get("DELAY_THRESHOLD_MINUTES", "15"))

    submit_args = (
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    spark = spark_util.getOrCreateSparkSession("Clean and Prepare Features", submit_args)
    spark.sparkContext.setLogLevel("INFO")

    from pyspark.sql.functions import array, col, explode, struct, lit, approx_count_distinct, when

    input_location = f"s3a://{s3_data_bucket}/flights/*.parquet"
    print(f"Reading merged dataset from {input_location}...")
    df = spark.read.parquet(input_location)
    print(f"Raw record count: {df.count()}")

    # 1. Drop redundant join keys
    redundant_keys = ["AL_IATA_CODE", "ORIG_IATA_CODE", "DEST_IATA_CODE"]
    existing_keys = [k for k in redundant_keys if k in df.columns]
    if existing_keys:
        df = df.drop(*existing_keys)

    # 2. Fast single-value (constant) column pruning using approx_count_distinct
    print("Scanning for single-value (zero-variance / constant) columns...")
    df_distinct = df.agg(*(approx_count_distinct(col(c)).alias(c) for c in df.columns))

    cols, dtypes = zip(*((c, t) for (c, t) in df_distinct.dtypes))
    kvs = explode(array([
        struct(lit(c).alias("column_name"), col(c).alias("distinct_count")) for c in cols
    ])).alias("kvs")

    distinct_count = df_distinct.select([kvs]).select(["kvs.column_name", "kvs.distinct_count"])
    uni_value_fields = distinct_count.filter(distinct_count.distinct_count == 1)

    constant_cols = [str(row.column_name) for row in uni_value_fields.select("column_name").collect()]
    if constant_cols:
        print(f"Dropping constant columns (zero variance): {constant_cols}")
        df = df.drop(*constant_cols)

    # 3. Vectorized feature calculation: DELAYED label (0 or 1)
    # Using native Spark SQL expressions avoids Python UDF row-by-row serialization
    print(f"Computing target label 'DELAYED' (departure delay >= {delay_threshold} mins)...")
    is_delayed_expr = when(col("cancelled") == 1, 0) \
        .when((col("departure_delay").isNotNull()) & (col("departure_delay") >= delay_threshold), 1) \
        .otherwise(0)

    df_clean = df.withColumn("DELAYED", is_delayed_expr)

    # 4. Save clean dataset to MinIO in Parquet format
    output_location = f"s3a://{s3_data_bucket}/flights-clean"
    print(f"Writing clean dataset to {output_location} in Parquet format...")

    df_clean.write.mode("overwrite") \
        .option("header", "true") \
        .format("parquet").save(output_location)

    # Read Parquet metadata directly for instantaneous verification without re-evaluating the DAG
    final_count = spark.read.parquet(output_location).count()
    print(f"=== Clean Data Pipeline Completed Successfully! Final records: {final_count} ===")
    spark.stop()


if __name__ == "__main__":
    run_clean()
