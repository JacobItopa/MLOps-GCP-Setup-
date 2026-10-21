import os
import sys

# Ensure root directory is on Python path so spark_util can be imported
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
for p in [current_dir, parent_dir]:
    if p not in sys.path:
        sys.path.insert(0, p)

import spark_util


def run_merge():
    """
    Step 1: Extract, Enrich, and Merge Pipeline
    - Extracts raw table data from PostgreSQL via JDBC
    - Reads reference/lookup CSVs from MinIO S3 Object Storage
    - Performs broadcast hash joins with prefixed column names to avoid ambiguity
    - Writes enriched dataset in Parquet format to MinIO S3
    """
    print("=== Starting Step 1: Merge & Enrich Data ===")

    # Database and S3 connection configurations with environment variable overrides
    db_url = os.environ.get("DB_URL", "jdbc:postgresql://pg-flights-data:5432/postgres")
    db_table = os.environ.get("DB_TABLE", "flights")
    db_user = os.environ.get("DB_USER", "postgres")
    db_password = os.environ.get("DB_PASSWORD", "postgres")
    s3_lookup_bucket = os.environ.get("S3_LOOKUP_BUCKET", "airport-data")
    s3_data_bucket = os.environ.get("S3_DATA_BUCKET", "flights-data")

    submit_args = (
        "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.3.3"
    )

    spark = spark_util.getOrCreateSparkSession("Enrich and Merge Data", submit_args)
    spark.sparkContext.setLogLevel("INFO")

    from pyspark.sql.functions import broadcast, col

    # 1. Read primary data from PostgreSQL
    print(f"Reading {db_table} table from PostgreSQL ({db_url})...")
    df_main = spark.read \
        .format("jdbc") \
        .option("url", db_url) \
        .option("dbtable", db_table) \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # 2. Read lookup reference datasets from MinIO
    print(f"Reading lookup datasets from s3a://{s3_lookup_bucket}/...")
    df_airlines = spark.read.format("csv").option("header", "true").load(f"s3a://{s3_lookup_bucket}/airlines.csv")
    df_airports = spark.read.format("csv").option("header", "true").load(f"s3a://{s3_lookup_bucket}/airports.csv")

    # 3. Rename columns with unique prefixes to prevent Spark SQL ambiguous reference errors
    df_airlines = df_airlines.select([col(c).alias("AL_" + c) for c in df_airlines.columns])
    df_o_airports = df_airports.select([col(c).alias("ORIG_" + c) for c in df_airports.columns])
    df_d_airports = df_airports.select([col(c).alias("DEST_" + c) for c in df_airports.columns])

    # 4. Join datasets using broadcast joins for maximum performance
    print("Joining data with lookup tables using broadcast joins...")
    df_merged = df_main \
        .join(broadcast(df_airlines), df_main.airline == df_airlines.AL_IATA_CODE) \
        .join(broadcast(df_o_airports), df_main.origin_airport == df_o_airports.ORIG_IATA_CODE) \
        .join(broadcast(df_d_airports), df_main.destination_airport == df_d_airports.DEST_IATA_CODE)

    # 5. Write merged dataset to MinIO in Parquet format
    output_location = f"s3a://{s3_data_bucket}/flights"
    print(f"Writing merged dataset to {output_location} in Parquet format...")

    df_merged.write.mode("overwrite") \
        .option("header", "true") \
        .format("parquet").save(output_location)

    merged_count = spark.read.parquet(output_location).count()
    print(f"=== Merge Data Completed Successfully! Saved records: {merged_count} ===")
    spark.stop()


if __name__ == "__main__":
    run_merge()
