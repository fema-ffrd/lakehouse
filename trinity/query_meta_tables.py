import logging
import os

logging.basicConfig(level=logging.INFO)

import duckdb
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


def query_storms(con, pg_str, metadata_location):
    """Query and list all storms from the STAC items Iceberg table using metadata location."""
    try:
        # Query the Iceberg table using the metadata location
        result = con.sql(
            f"""
            SELECT 
                id,
                collection,
                storm_type,
                datetime,
                assets
            FROM iceberg_scan('{metadata_location}')
            ORDER BY datetime DESC
            LIMIT 5;
        """
        ).df()

        return result

    except Exception as e:
        logging.error(f"Error querying storms: {e}")

        # If no snapshots, try reading the metadata JSON directly
        logging.info(f"Attempting alternative approach with metadata location: {metadata_location}")
        try:
            metadata_content = con.sql(
                f"""
                SELECT * FROM read_json_auto('{metadata_location}')
                LIMIT 5;
            """
            ).df()
            logging.info(f"Metadata structure:\n{metadata_content}")
            return None
        except Exception as e2:
            logging.error(f"Alternative approach also failed: {e2}")
            return None


def query_storms_with_assets(con, pg_str, metadata_location):
    """Query storms and extract asset hrefs, returning a list of storm records."""
    import json

    try:
        # Get the base query results
        storms_df = query_storms(con, pg_str, metadata_location)

        if storms_df is None or len(storms_df) == 0:
            return []

        # Process results and extract asset hrefs
        storms_list = []
        for idx, row in storms_df.iterrows():
            storm_id = row["id"]
            collection = row["collection"]
            storm_type = row["storm_type"]
            datetime = row["datetime"]
            assets_str = row["assets"]

            # Try to parse assets as JSON string
            aorc_storm_href = None
            try:
                # Parse JSON string
                assets_dict = json.loads(assets_str)
                # Extract href from aorc_storm asset
                if "aorc_storm" in assets_dict:
                    aorc_asset = assets_dict["aorc_storm"]
                    if isinstance(aorc_asset, dict) and "href" in aorc_asset:
                        aorc_storm_href = aorc_asset["href"]
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logging.debug(f"Failed to parse assets for {storm_id}: {e}")

            # Add storm record to list
            storms_list.append(
                {
                    "id": storm_id,
                    "collection": collection,
                    "storm_type": storm_type,
                    "datetime": str(datetime),
                    "aorc_storm_href": aorc_storm_href,
                }
            )

        return storms_list

    except Exception as e:
        logging.error(f"Error querying storms with assets: {e}")
        return []


def main():
    """Main function to query Iceberg metadata and storms data."""
    con, pg_str = get_duckdb_connection()

    try:
        # Get metadata for the storms table
        metadata = get_iceberg_metadata(con, pg_str, "storms")

        if metadata is not None and len(metadata) > 0:
            metadata_location = metadata.iloc[0]["metadata_location"]
            logging.info(f"Metadata location: {metadata_location}")

            # Query storms and extract assets
            storms_list = query_storms_with_assets(con, pg_str, metadata_location)

            if storms_list:
                logging.info(f"Retrieved {len(storms_list)} storms from stac.storms table")
                for _, storm in enumerate(storms_list, 1):
                    print(storm)
                    logging.info(
                        f" storm_id {storm['id']} | {storm['storm_type']} | {storm['datetime'][:10]} | {storm['aorc_storm_href']}"
                    )
            else:
                logging.warning("stac.storms table appears to be empty or no storms found")
        else:
            logging.warning("stac.storms table not found in metadata")

    finally:
        con.close()


if __name__ == "__main__":
    main()
