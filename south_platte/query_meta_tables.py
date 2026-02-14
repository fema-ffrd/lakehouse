import logging
import os

logging.basicConfig(level=logging.INFO)

import duckdb
import pandas as pd
from dotenv import load_dotenv


def get_duckdb_connection():
    """Initialize DuckDB with necessary extensions and S3/Postgres configuration."""
    load_dotenv()

    S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
    S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
    S3_REGION = os.getenv("S3_REGION")
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT")

    con = duckdb.connect(database=":memory:")

    # Install & load extensions
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL iceberg;")
    con.execute("LOAD iceberg;")
    con.execute("INSTALL postgres_scanner;")
    con.execute("LOAD postgres_scanner;")

    # Configure S3
    con.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY_ID}';")
    con.execute(f"SET s3_secret_access_key='{S3_SECRET_ACCESS_KEY}';")
    con.execute(f"SET s3_region='{S3_REGION}';")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl='true';")

    # Postgres connection string
    pg_str = f"dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST} port={POSTGRES_PORT}"

    return con, pg_str


def get_iceberg_metadata(con, pg_str, table_name="storms"):
    """Get Iceberg table metadata from the Postgres catalog."""
    try:
        catalogs = con.sql(
            f"""
            SELECT *
            FROM postgres_scan(
                '{pg_str}',
                'public',
                'iceberg_tables'
            )
            WHERE table_name = '{table_name}'
            LIMIT 20;
        """
        ).df()
        return catalogs
    except Exception as e:
        logging.error(f"Error fetching Iceberg metadata: {e}")
        return None


def query_gages(con, metadata_location):
    """Query gages and extract annual_maxima_series href and file:values."""
    try:
        import json

        warehouse_path = metadata_location.rsplit("/", 1)[0].replace("metadata", "data")

        # Use DuckDB's JSON functions to extract only needed data
        result = con.sql(
            f"""
            SELECT 
                id,
                station_nm,
                json_extract_string(assets, '$.annual_maxima_series.href') as annual_maxima_series_href,
                json_extract(assets, '$.annual_maxima_series.file:values') as ams_values
            FROM read_parquet('{warehouse_path}/*.parquet')
            WHERE json_extract_string(assets, '$.annual_maxima_series.href') IS NOT NULL;
        """
        ).df()

        # Parse ams_values from JSON string to actual objects
        result["ams_values"] = result["ams_values"].apply(lambda x: json.loads(x) if isinstance(x, str) else x)

        return result

    except Exception as e:
        logging.error(f"Error querying gages: {e}")
        return None


def main():
    """Main function to query Iceberg metadata and storms data."""
    con, pg_str = get_duckdb_connection()

    try:
        # Get metadata for the storms table
        metadata = get_iceberg_metadata(con, pg_str, "gages")

        if metadata is not None and len(metadata) > 0:
            metadata_location = metadata.iloc[0]["metadata_location"]
            logging.info(f"Metadata location: {metadata_location}")
            gages = query_gages(con, metadata_location)
            if gages is not None and len(gages) > 0:
                logging.info(f"Retrieved {len(gages)} gages")
                # Save to JSON with unescaped forward slashes
                output_file = "gages_ams_data.json"
                json_str = gages.to_json(orient="records", indent=2, default_handler=str)
                # Unescape forward slashes in URLs
                json_str = json_str.replace("\\/", "/")
                with open(output_file, "w") as f:
                    f.write(json_str)
                logging.info(f"Saved gages data to {output_file}")
            else:
                logging.warning("No gages found in the table.")

        else:
            logging.warning("stac.gages table not found in metadata")

    finally:
        con.close()


if __name__ == "__main__":
    main()
