"""Utility functions for connecting to Iceberg catalog and managing configurations."""

import logging
import os

import dotenv
from pyiceberg.catalog.sql import SqlCatalog

dotenv.load_dotenv()

# Catalog info
S3_BUCKET = "trinity-pilot"
CATALOG_NAME = "trinity"
WAREHOUSE_NAME = "warehouse"
CATALOG_ROOT = f"s3://{S3_BUCKET}/{WAREHOUSE_NAME}"

# AWS
S3_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("AWS_REGION", "us-east-1")

# Credentials and connection info
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")

PG_CONN_STRING = (
    f"postgresql+psycopg://{POSTGRES_USER}:{POSTGRES_PASSWORD}" f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)


def connect_to_catalog(
    pg_conn_string=PG_CONN_STRING,
    catalog_name=CATALOG_NAME,
    catalog_root=CATALOG_ROOT,
    s3_access_key_id=S3_ACCESS_KEY_ID,
    s3_secret_access_key=S3_SECRET_ACCESS_KEY,
    s3_region=S3_REGION,
):
    """Connect to an Iceberg SQL catalog using provided connection parameters.
    Args:
       pg_conn_string (str): PostgreSQL connection string.
       catalog_name (str): Name of the Iceberg catalog.
       catalog_root (str): Root path for the Iceberg catalog in S3.
       s3_access_key_id (str): AWS S3 access key ID.
       s3_secret_access_key (str): AWS S3 secret access key.
       s3_region (str): AWS S3 region.
    Returns:
       SqlCatalog: Connected Iceberg SQL catalog object or None if connection fails.
    """
    try:
        catalog = SqlCatalog(
            catalog_name,
            **{
                "uri": pg_conn_string,
                "icedev": catalog_root,
                "s3.access-key-id": s3_access_key_id,
                "s3.secret-access-key": s3_secret_access_key,
                "s3.path-style-access": "true",
                "s3.region": s3_region,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )
        logging.info(f"Connected to Iceberg catalog: `{catalog_name}`")
        return catalog
    except Exception as e:
        logging.error(f"Cannot connect to Iceberg catalog: {e}")
        return None
