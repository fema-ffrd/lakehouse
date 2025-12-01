import argparse
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import SparkSession

# import dotenv

# Example credentials from the connection string
PG_CONN_STRING = "jdbc:postgresql://postgres:5432/postgres"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# 512 MB in bytes
TARGET_FILE_SIZE_BYTES = 536870912


def parse_args():
    parser = argparse.ArgumentParser(description="Run Iceberg compaction for a specific namespace/table.")

    # Required arguments
    parser.add_argument(
        "--namespace",
        required=True,
        help="Iceberg namespace (e.g. 'conformance')",
    )
    parser.add_argument(
        "--table",
        required=True,
        help="Iceberg table name (e.g. 'hydrology')",
    )

    # Optional arguments with defaults (env -> constant -> hard-coded)
    parser.add_argument(
        "--catalog-name",
        default=os.getenv("CATALOG_NAME", "trinity"),
        help=f"Catalog name (default: env CATALOG_NAME or 'trinity')",
    )
    parser.add_argument(
        "--catalog-root",
        default=os.getenv("CATALOG_ROOT", "s3://trinity-pilot/warehouse"),
        help=f"Catalog root (default: env CATALOG_ROOT or 's3://trinity-pilot/warehouse')",
    )
    parser.add_argument(
        "--pg-conn-string",
        default=os.getenv("PG_CONN_STRING", PG_CONN_STRING),
        help=f"Postgres JDBC connection string (default: env PG_CONN_STRING or '{PG_CONN_STRING}')",
    )
    parser.add_argument(
        "--pg-user",
        default=os.getenv("PG_USER", PG_USER),
        help=f"Postgres user (default: env PG_USER or '{PG_USER}')",
    )
    parser.add_argument(
        "--pg-password",
        default=os.getenv("PG_PASSWORD", PG_PASSWORD),
        help=f"Postgres password (default: env PG_PASSWORD or value in script)",
    )
    parser.add_argument(
        "--max-parallelism",
        type=int,
        default=int(os.getenv("MAX_PARALLELISM", 4)),
        help="Max parallel partitions to compact (default: 4 or env MAX_PARALLELISM)",
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=int(os.getenv("TARGET_FILE_SIZE_BYTES", TARGET_FILE_SIZE_BYTES)),
        help=(
            "Target file size in bytes for compaction "
            f"(default: env TARGET_FILE_SIZE_BYTES or {TARGET_FILE_SIZE_BYTES})"
        ),
    )

    return parser.parse_args()


def connect_iceberg_catalog(
    pg_conn_string, catalog_name, catalog_root, s3_access_key_id, s3_secret_access_key, s3_region, pg_user, pg_password
) -> SparkSession:
    """
    Configures and starts a Spark session with a JDBC (Postgres) catalog
    and S3 storage configuration, ensuring Iceberg SQL extensions are active.
    """
    spark_builder = (
        SparkSession.builder.appName("Iceberg Compaction with Spark")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config(f"spark.sql.catalog.{catalog_name}.uri", pg_conn_string)
        .config(f"spark.sql.catalog.{catalog_name}.warehouse", catalog_root)
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", s3_access_key_id)
        .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", s3_secret_access_key)
        .config(f"spark.sql.catalog.{catalog_name}.s3.region", s3_region)
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.user", pg_user)
        .config(f"spark.sql.catalog.{catalog_name}.jdbc.password", pg_password)
        .enableHiveSupport()
    )

    # Use getOrCreate() to retrieve or create the Spark session with these specific configs
    spark = spark_builder.getOrCreate()
    logging.info("Spark session and Iceberg catalog connected successfully.")
    return spark


def compact_iceberg_partition(spark, full_table_identifier, partition_spec_dict):
    logging.info(f"Starting compaction for partition: {partition_spec_dict}")

    # Dynamically build the WHERE clause using DOUBLE quotes for string values
    where_clause_parts = []
    for k, v in partition_spec_dict.items():
        if isinstance(v, str):
            # Use double quotes for the value inside the SQL string
            where_clause_parts.append(f'{k} = "{v}"')
        else:
            where_clause_parts.append(f"{k} = {v}")

    where_clause = " AND ".join(where_clause_parts)

    catalog_name, _, _ = full_table_identifier.partition(".")

    # Pass the where_clause string as a single, properly quoted argument
    # The outer quotes for the 'where' argument are single quotes, so inner double quotes are fine.
    procedure_call = (
        f"CALL {catalog_name}.system.rewrite_data_files(table => '{full_table_identifier}', where => '{where_clause}')"
    )

    logging.info(f"Executing: {procedure_call}")
    spark.sql(procedure_call)
    logging.info(f"Completed compaction for partition: {partition_spec_dict}")


def compact_and_parallelize_partitions_fixed(
    spark: SparkSession, catalog_name: str, namespace: str, table_name: str, max_parallelism: int = 4
):
    full_table_identifier = f"{catalog_name}.{namespace}.{table_name}"
    logging.info(f"Starting parallelized compaction for table: {full_table_identifier}")

    # Set the target file size for the session
    # Note: this sets the default for the session.
    # The 'rewrite_data_files' procedure also has its own properties for target file size.
    spark.conf.set(f"spark.sql.catalog.{catalog_name}.write.target-file-size-bytes", str(TARGET_FILE_SIZE_BYTES))

    # --- FIX 1: Use the Iceberg metadata table for partitions ---
    # The '.partitions' suffix gives you the data you need
    # partitions_df = spark.sql(f"SELECT partition_spec FROM {full_table_identifier}.partitions")
    partitions_df = spark.sql(f"SELECT partition FROM {full_table_identifier}.partitions")

    # Extract the full partition specification strings
    # The structure of the metadata table columns might vary, use .show() to confirm column names if needed.
    # A common column name for the spec string is 'partition_spec' or a list of partition column values.
    # Based on the previous logs, the partition values are Row(realization_id=1, model_id='trinity')
    # If you need to construct the 'WHERE' clause string, you might need a helper function.

    # A simpler way to get distinct combinations of partition column values
    partitions_df = spark.read.table(full_table_identifier).selectExpr("realization_id", "model_id").distinct()
    partition_specs = [row.asDict() for row in partitions_df.collect()]
    # ^ This creates a list of dictionaries for each unique partition combo, which is easier to filter with later

    if not partition_specs:
        logging.info("Table is not partitioned or no partitions found. Running single compaction job.")
        # --- FIX 2: Ensure correct catalog is used for procedure call ---
        spark.sql(f"CALL {catalog_name}.system.rewrite_data_files(table => '{full_table_identifier}')")
    else:
        logging.info(f"Found {len(partition_specs)} partitions. Processing with {max_parallelism} workers.")

        # compact_iceberg_partition function needs to accept a dictionary/Row object for filtering
        with ThreadPoolExecutor(max_workers=max_parallelism) as executor:
            futures = [
                executor.submit(
                    compact_iceberg_partition,
                    spark,  # Pass the spark session to the worker function
                    full_table_identifier,
                    spec,  # Pass the dictionary/Row object
                )
                for spec in partition_specs
            ]
            for future in as_completed(futures):
                # This will raise exceptions if any task failed
                future.result()

    logging.info("Parallel compaction complete.")


if __name__ == "__main__":
    args = parse_args()

    # Optionally override global if you want the CLI to control it:
    global TARGET_FILE_SIZE_BYTES
    TARGET_FILE_SIZE_BYTES = args.target_file_size_bytes

    # 1. Connect and configure Spark
    spark_session = connect_iceberg_catalog(
        args.pg_conn_string,
        args.catalog_name,
        args.catalog_root,
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_REGION,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
    )

    # 2. Run the distributed compaction and sort
    # NOTE: function signature is (spark, catalog_name, namespace, table_name, max_parallelism)
    compact_and_parallelize_partitions_fixed(
        spark_session,
        args.catalog_name,
        args.namespace,  # e.g. "conformance"
        args.table,  # e.g. "hydrology"
        max_parallelism=args.max_parallelism,
    )

    # 3. Stop Spark session
    spark_session.stop()
