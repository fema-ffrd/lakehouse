import json

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import shapely.geometry
from shapely import from_wkb


# Custom JSON encoder to handle numpy types and pandas types
class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif hasattr(obj, "item"):  # Handle other numpy-like types
            return obj.item()
        return super().default(obj)


# Read the new geoparquet file
geopq_file = "all_items_geopq_v2.parquet"
table = pq.read_table(geopq_file)
geopq_df = table.to_pandas()
print(f"Loaded {len(geopq_df)} items from {geopq_file}")


# Helper function to parse string representations
def parse_string_fields(obj):
    """Recursively parse string fields that look like dicts/lists"""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str):
                # Try to parse string representations
                if value.startswith("[") or value.startswith("{"):
                    try:
                        obj[key] = eval(value)
                    except:
                        pass  # Keep as string if eval fails
            elif isinstance(value, dict):
                parse_string_fields(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        parse_string_fields(item)
    return obj


# Parse string fields in the entire dataframe
for col in geopq_df.columns:
    if geopq_df[col].dtype == "object":

        def parse_col_value(x):
            if isinstance(x, dict):
                return parse_string_fields(x)
            elif isinstance(x, str) and (x.startswith("[") or x.startswith("{")):
                try:
                    return eval(x)
                except:
                    return x
            return x

        geopq_df[col] = geopq_df[col].apply(parse_col_value)

# Get the second item (index 1)
second_item_row = geopq_df.iloc[1]
second_item_dict = second_item_row.to_dict()

# Convert WKB geometry to GeoJSON
if "geometry" in second_item_dict and second_item_dict["geometry"] is not None:
    try:
        geom = from_wkb(second_item_dict["geometry"])
        second_item_dict["geometry"] = shapely.geometry.mapping(geom)
    except Exception as e:
        print(f"Warning: Could not convert geometry: {e}")

# Save second item as JSON
output_path = "second_item.json"
with open(output_path, "w") as f:
    # Use custom encoder to handle numpy types
    json.dump(second_item_dict, f, indent=2, cls=NumpyEncoder)

print(f"Saved second item to {output_path}")
