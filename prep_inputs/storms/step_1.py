import json

import boto3
import pandas as pd
import pystac
from stac_geoparquet.arrow import stac_table_to_items, to_parquet

collection_url = "s3://trinity-pilot/stac/stormlit/storms-db/72hr-events/collection.json"
all_items_geopq = "s3://trinity-pilot/stac/stormlit/storms-db/72hr-events/all-items.parquet"

# Parse S3 URL
s3_path = collection_url.replace("s3://", "")
bucket, key = s3_path.split("/", 1)

# Read from S3
s3_client = boto3.client("s3")
response = s3_client.get_object(Bucket=bucket, Key=key)
collection_data = json.loads(response["Body"].read())

# Create collection object
collection = pystac.Collection.from_dict(collection_data)
print(collection)

# Read the all_items geoparquet asset using stac-geoparquet
geopq_df = pd.read_parquet(all_items_geopq)
print(f"Loaded {len(geopq_df)} items")

# Parse STAC items from geoparquet table
stac_items = stac_table_to_items(geopq_df)

# Process all items: consolidate AORC_* to AORC_SOURCE and drop nulls
processed_items = []
for item_dict in stac_items:
    # Fix bbox format if it's a dict, convert to list
    if "bbox" in item_dict:
        bbox = item_dict["bbox"]
        if isinstance(bbox, dict):
            # Convert from {xmin, ymin, xmax, ymax} to [xmin, ymin, xmax, ymax]
            item_dict["bbox"] = [bbox.get("xmin"), bbox.get("ymin"), bbox.get("xmax"), bbox.get("ymax")]

    # Process assets: consolidate AORC_* to AORC_SOURCE and drop nulls
    if "assets" in item_dict:
        assets = item_dict["assets"]
        aorc_sources = []

        for key, value in list(assets.items()):
            if key.startswith("AORC_"):
                if value is not None:
                    # Clean up the asset data
                    if isinstance(value, dict):
                        # Fix roles field if it contains MIME types
                        if "roles" in value:
                            roles = value["roles"]
                            if isinstance(roles, list) and roles and roles[0].startswith("application/"):
                                # Replace with proper role
                                value["roles"] = ["data"]
                    aorc_sources.append(value)
                # Remove all individual AORC_* keys
                del assets[key]

        # Add consolidated AORC_SOURCE if we found any non-null values
        # Store only the first one as a proper asset (STAC assets must be dicts, not lists)
        if aorc_sources:
            assets["AORC_SOURCE"] = aorc_sources[0]

        # Fix roles field in all assets if it's a string representation
        for asset_key, asset_value in assets.items():
            if isinstance(asset_value, dict) and "roles" in asset_value:
                roles = asset_value["roles"]
                if isinstance(roles, str):
                    # Try to eval string representation of list
                    try:
                        asset_value["roles"] = eval(roles)
                    except:
                        # If it fails, just wrap it in a list
                        asset_value["roles"] = [roles]

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
                link["href"] = "s3://trinity-pilot/stac/stormlit/storms-db/catalog.json"
            elif rel == "self":
                # Set self link to S3 item path
                item_id = item_dict.get("id")
                if item_id:
                    link["href"] = f"s3://trinity-pilot/stac/stormlit/storms-db/72hr-events/{item_id}/{item_id}.json"

    processed_items.append(item_dict)

print(f"Processed {len(processed_items)} items")

# Save first item as JSON for verification
first_item_dict = processed_items[0]
output_path = "first_item.json"
with open(output_path, "w") as f:
    json.dump(first_item_dict, f, indent=2, default=str)

print(f"Saved first item to {output_path}")

# Save all processed items to new geoparquet file
import pyarrow as pa
from stac_geoparquet.arrow import parse_stac_items_to_arrow, to_parquet

output_geopq = "all_items_geopq_v2.parquet"

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
