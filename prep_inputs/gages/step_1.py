import json
import logging
import os

import boto3
import duckdb
import pandas as pd
import pystac
from dotenv import load_dotenv
from stac_geoparquet.arrow import stac_table_to_items, to_parquet

logging.basicConfig(level=logging.INFO)
collection_url = "s3://south-platte/stac/gages/gages/collection.json"

# Parse S3 URL
s3_path = collection_url.replace("s3://", "")
bucket, key = s3_path.split("/", 1)

# Read from S3
s3_client = boto3.client("s3")
response = s3_client.get_object(Bucket=bucket, Key=key)
collection_data = json.loads(response["Body"].read())

# Create collection object
collection = pystac.Collection.from_dict(collection_data)
print(f"Collection: {collection.id}")

# Load all items from the collection by reading item links directly
stac_items = []
item_count = 0

# Get item links from the collection
item_links = [link for link in collection.links if link.rel == "item"]
print(f"Found {len(item_links)} item links in collection")

# Get the base URL for resolving relative paths
collection_base = collection_url.rsplit("/", 1)[0]  # Remove the filename

# Load each item directly from S3
for link in item_links:
    try:
        item_href = link.href

        # Handle relative paths by resolving against collection base
        if item_href.startswith("./"):
            item_href = collection_base + "/" + item_href[2:]  # Remove ./ and append to base

        # If it's an S3 URL, read it directly
        if item_href.startswith("s3://"):
            s3_item_path = item_href.replace("s3://", "")
            item_bucket, item_key = s3_item_path.split("/", 1)
            response = s3_client.get_object(Bucket=item_bucket, Key=item_key)
            item_data = json.loads(response["Body"].read())
            stac_items.append(item_data)
            item_count += 1
            if item_count % 100 == 0:
                print(f"Loaded {item_count} items...")
        else:
            print(f"Skipping non-S3 item link: {item_href}")
    except Exception as e:
        print(f"Warning: Could not load item from {link.href}: {e}")

print(f"Total loaded {len(stac_items)} items")

if len(stac_items) == 0:
    print("Error: No items were loaded. Exiting.")
    exit(1)

# Process all items: fix bbox format, handle assets, and update links
processed_items = []
for item_dict in stac_items:
    # Get the item's base URL for resolving relative asset paths
    item_id = item_dict.get("id")
    if item_id:
        item_base = f"{collection_base}/{item_id}"
    else:
        item_base = collection_base

    # Fix bbox format if it's a dict, convert to list
    if "bbox" in item_dict:
        bbox = item_dict["bbox"]
        if isinstance(bbox, dict):
            # Convert from {xmin, ymin, xmax, ymax} to [xmin, ymin, xmax, ymax]
            item_dict["bbox"] = [bbox.get("xmin"), bbox.get("ymin"), bbox.get("xmax"), bbox.get("ymax")]

    # Process assets: clean up null values, fix roles field, and convert hrefs to absolute
    if "assets" in item_dict:
        assets = item_dict["assets"]

        # Remove null assets
        for key in list(assets.keys()):
            if assets[key] is None:
                del assets[key]

        # Fix roles field and convert asset hrefs to absolute URLs
        for asset_key, asset_value in assets.items():
            if isinstance(asset_value, dict):
                # Fix roles field if it's a string representation
                if "roles" in asset_value:
                    roles = asset_value["roles"]
                    if isinstance(roles, str):
                        # Try to eval string representation of list
                        try:
                            asset_value["roles"] = eval(roles)
                        except:
                            # If it fails, just wrap it in a list
                            asset_value["roles"] = [roles]

                # Convert href to absolute S3 URL if it's relative
                if "href" in asset_value:
                    href = asset_value["href"]
                    if not href.startswith("s3://") and not href.startswith("http"):
                        # It's a relative path, resolve it
                        if href.startswith("./"):
                            asset_value["href"] = f"{item_base}/{href[2:]}"
                        else:
                            asset_value["href"] = f"{item_base}/{href}"

    # Update links to have absolute references based on the original collection
    if "links" in item_dict:
        for link in item_dict["links"]:
            rel = link.get("rel")
            href = link.get("href")

            if rel == "collection":
                # Set collection link to the original collection URL
                link["href"] = collection_url
            elif rel == "parent":
                # Set parent link to the original collection URL
                link["href"] = collection_url
            elif rel == "root":
                # Set root link to S3 catalog
                link["href"] = "s3://south-platte/stac/gages/catalog.json"
            elif rel == "self":
                # Set self link to the collection URL
                link["href"] = collection_url

    processed_items.append(item_dict)

print(f"Processed {len(processed_items)} items")

# Save first item as JSON for verification
first_item_dict = processed_items[0]
output_path = "first_gage_item.json"
with open(output_path, "w") as f:
    json.dump(first_item_dict, f, indent=2, default=str)

print(f"Saved first item to {output_path}")

# Save all processed items to new geoparquet file
import pyarrow as pa
from stac_geoparquet.arrow import parse_stac_items_to_arrow, to_parquet

output_geopq = "gages_all_items_geopq.parquet"

# Convert items to arrow table
arrow_table = parse_stac_items_to_arrow(processed_items)

# Save to parquet
to_parquet(arrow_table, output_geopq)
print(f"Saved {len(processed_items)} items to {output_geopq}")


# Validate STAC items
def validate_stac_items(items):
    """Validate STAC items using pystac"""
    valid_count = 0
    invalid_count = 0

    for idx, item_dict in enumerate(items):
        try:
            # Create Item object from dictionary
            item = pystac.Item.from_dict(item_dict)

            # Validate using core schema only (doesn't resolve remote links)
            try:
                pystac.validation.validate_core(item)
                valid_count += 1
            except Exception as val_error:
                # If validation fails, still try basic checks
                if idx < 5:
                    print(f"\nItem {idx} (id: {item_dict.get('id')}) core validation error: {val_error}")
                invalid_count += 1

        except Exception as e:
            invalid_count += 1
            if idx < 5:  # Print detailed errors for first few invalid items
                print(f"\nItem {idx} (id: {item_dict.get('id')}) parsing error: {e}")
                print(f"  Item dict keys: {item_dict.keys()}")
                import traceback

                traceback.print_exc()
            else:
                print(f"Item {idx} (id: {item_dict.get('id')}) parsing error: {e}")

    print(f"\nValidation Results: {valid_count} valid, {invalid_count} invalid out of {len(items)} items")
    return valid_count, invalid_count


# Validate the processed items
print("\nValidating STAC items...")
validate_stac_items(processed_items)
