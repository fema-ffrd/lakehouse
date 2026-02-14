import json
from datetime import datetime

import icechunk
import numpy as np
import pyarrow.parquet as pq
import pystac
import zarr
from stac_geoparquet.arrow import parse_stac_items_to_arrow, stac_table_to_items


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy and pandas types."""

    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        return super().default(obj)


def open_repo(bucket: str, prefix: str, region: str = "us-east-1") -> icechunk.Repository:
    """Open an Icechunk repository from an S3 bucket and prefix."""
    storage_config = icechunk.s3_storage(
        bucket=bucket,
        prefix=prefix,
        region=region,
    )
    repo = icechunk.Repository.open(storage_config)
    return repo


def open_session(repo: icechunk.Repository, branch: str = "main"):
    """Open a session from an Icechunk repository."""
    session = repo.readonly_session(branch=branch)
    try:
        zarr_group = zarr.open(session.store, mode="r")
        return zarr_group
    except Exception as e:
        print(f"Error opening with zarr: {e}")
        raise


def get_storm_index_map(zarr_group) -> dict:
    """Create a mapping of storm_id to array index."""
    storm_ids = zarr_group["storm_id"][:]
    return {int(sid): idx for idx, sid in enumerate(storm_ids)}


def add_icechunk_assets(item: pystac.Item, storm_id: int, storm_index: int) -> pystac.Item:
    """Add icechunk asset (combined precipitation and temperature) to a STAC item."""

    base_href = "s3://trinity-pilot/test/trinity-storms.icechunk/"

    # Combined AORC Storm Asset (single zarr reference with multiple bands)
    aorc_asset = pystac.Asset(
        href=base_href,
        media_type="application/vnd.zarr; version=2",
        title="AORC storm variables",
        description="AORC precipitation and temperature for this storm stored as Icechunk/Zarr",
        roles=["data", "zarr"],
        extra_fields={
            "trinity:storm_index": storm_index,
            "trinity:storm_id": storm_id,
            "trinity:icechunk_branch": "main",
            "trinity:icechunk_snapshot": None,  # Could be populated with actual snapshot ID
            "eo:bands": [
                {"name": "APCP_surface", "description": "Total precipitation", "unit": "kg/m^2"},
                {"name": "TMP_2maboveground", "description": "2m air temperature", "unit": "K"},
            ],
        },
    )

    item.add_asset("aorc_storm", aorc_asset)

    return item


def main():
    """Main function to convert v2 parquet to v3 with icechunk assets."""

    # Configuration
    v2_parquet = "/Users/slawler/Desktop/lakehouse/all_items_geopq_v2.parquet"
    v3_parquet = "/Users/slawler/Desktop/lakehouse/all_items_geopq_v3.parquet"
    icechunk_bucket = "trinity-pilot"
    icechunk_prefix = "test/trinity-storms.icechunk"

    print("Step 1: Opening icechunk repository...")
    repo = open_repo(bucket=icechunk_bucket, prefix=icechunk_prefix)
    zarr_group = open_session(repo=repo, branch="main")
    print("  ✓ Successfully opened icechunk repository")

    # Create storm_id to array index mapping
    print("\nStep 2: Creating storm ID mapping...")
    storm_index_map = get_storm_index_map(zarr_group)
    print(f"  ✓ Found {len(storm_index_map)} storms in icechunk repository")

    # Read v2 parquet items
    print(f"\nStep 3: Reading {v2_parquet}...")
    table = pq.read_table(v2_parquet)
    items_dicts = list(stac_table_to_items(table))

    # Convert dicts to pystac.Item objects
    items = [pystac.Item.from_dict(item_dict) for item_dict in items_dicts]
    print(f"  ✓ Loaded {len(items)} STAC items")

    # Add icechunk assets to each item
    print("\nStep 4: Adding icechunk assets to items...")
    updated_items = []
    items_with_icechunk = 0
    items_without_storm = 0

    for i, item in enumerate(items):
        # The item.id is the storm_id in icechunk
        storm_id = item.id

        try:
            storm_id = int(storm_id)
        except (ValueError, TypeError):
            print(f"  ⚠ Item {i}: Could not parse storm_id '{storm_id}'")
            items_without_storm += 1
            updated_items.append(item)
            continue

        if storm_id in storm_index_map:
            storm_index = storm_index_map[storm_id]
            item = add_icechunk_assets(item, storm_id, storm_index)

            # Add derived_from link for AORC source
            item.add_link(
                pystac.Link(
                    rel="derived_from",
                    target="s3://noaa-nws-aorc-v1-1-1km/2017.zarr",
                    media_type="application/vnd+zarr",
                    title="NOAA AORC v1.1.1 1km (2017 Zarr store)",
                )
            )

            # Remove AORC_SOURCE asset if it exists (now a link)
            if "AORC_SOURCE" in item.assets:
                del item.assets["AORC_SOURCE"]

            # Add stac extensions for datacube and eo
            if "https://stac-extensions.github.io/eo/v1.1.0/schema.json" not in item.stac_extensions:
                item.stac_extensions.append("https://stac-extensions.github.io/eo/v1.1.0/schema.json")
            if "https://stac-extensions.github.io/datacube/v2.2.0/schema.json" not in item.stac_extensions:
                item.stac_extensions.append("https://stac-extensions.github.io/datacube/v2.2.0/schema.json")

            # Add cube:dimensions
            if "cube:dimensions" not in item.properties:
                item.properties["cube:dimensions"] = {
                    "time": {"type": "temporal"},
                    "y": {"type": "spatial"},
                    "x": {"type": "spatial"},
                }

            items_with_icechunk += 1
        else:
            print(f"  ⚠ Item {i}: storm_id {storm_id} not found in icechunk repository")
            items_without_storm += 1

        updated_items.append(item)

        # Progress indicator
        if (i + 1) % 50 == 0:
            print(f"    Processed {i + 1}/{len(items)} items...")

    print(f"  ✓ Added icechunk assets to {items_with_icechunk} items")
    if items_without_storm > 0:
        print(f"  ⚠ {items_without_storm} items missing or not found in icechunk")

    # Convert back to arrow table and save
    print(f"\nStep 5: Converting to arrow and saving to {v3_parquet}...")
    arrow_table = parse_stac_items_to_arrow(updated_items)

    # Handle if it returns a RecordBatchReader, convert to table
    if hasattr(arrow_table, "read_all"):
        arrow_table = arrow_table.read_all()

    pq.write_table(arrow_table, v3_parquet)
    print(f"  ✓ Saved v3 parquet with {len(updated_items)} items")

    # Save third item to JSON for verification
    print(f"\nStep 6: Saving third item to JSON for verification...")
    if len(updated_items) > 2:
        third_item = updated_items[2]
        third_item_dict = third_item.to_dict(transform_hrefs=False)

        output_file = "third_item_v3.json"
        with open(output_file, "w") as f:
            json.dump(third_item_dict, f, indent=2, cls=NumpyEncoder)
        print(f"  ✓ Saved third item to {output_file}")
    else:
        print(f"  ⚠ Only {len(updated_items)} items available")

    print("\n" + "=" * 60)
    print("SUCCESS: v3 parquet created with icechunk assets!")
    print(f"  Input:  {v2_parquet}")
    print(f"  Output: {v3_parquet}")
    print("=" * 60)


if __name__ == "__main__":
    main()
