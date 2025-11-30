"""
Schema Conversion Utilities for JSON, PyArrow, and Apache Iceberg
=================================================================

This module provides tools for converting table schemas between three formats:

    • JSON (canonical external representation)
    • PyArrow schemas
    • Apache Iceberg (PyIceberg) schemas

The JSON format acts as the single portable definition, while the module
materializes equivalent Arrow or Iceberg schemas as needed for processing,
storage, and interoperability.

---------------------------------------------------------------------------
JSON Schema Format
---------------------------------------------------------------------------

The JSON representation is simple and language-agnostic:

    {
      "table_properties": {
        "key": "value",
        ...
      },
      "fields": [
        {
          "name": "field_name",
          "type": "int32",
          "nullable": true,
          "metadata": {
            "unit": "m3/s",
            "description": "Discharge at the site"
          }
        },
        ...
      ]
    }

Only the "fields" section is required. "table_properties" is optional, and
field-level "metadata" is optional. When present they can be used to populate
Iceberg table properties and column-level documentation.

The `"type"` field uses canonical type names such as:

    "int32", "int64", "float32", "float64",
    "string", "bool", "timestamp[us]"

These names map directly to corresponding PyArrow and Iceberg types through a
central registry.

---------------------------------------------------------------------------
Available Conversions
---------------------------------------------------------------------------

    JSON → PyArrow schema
    JSON → Iceberg schema
    PyArrow schema → JSON
    PyArrow schema → Iceberg schema
    Iceberg schema → PyArrow schema

All conversions use the same canonical JSON type names to ensure consistency.

---------------------------------------------------------------------------
Example
---------------------------------------------------------------------------

    # Load from JSON
    arrow_schema = json_to_arrow("schema.json")
    iceberg_schema = json_to_iceberg("schema.json")

    # Convert in memory
    arrow_to_json(arrow_schema, "out.json")
    arrow_to_iceberg(arrow_schema)
    iceberg_to_arrow(iceberg_schema)

    # Load Iceberg schema + properties for table creation
    iceberg_schema, table_props = load_iceberg_schema_and_properties("schema.json")

---------------------------------------------------------------------------
Extensibility
---------------------------------------------------------------------------

New scalar types can be supported by adding entries to TYPE_REGISTRY.
Structured types (struct, list, map) can also be added in the future if needed.
"""

import json
from typing import Dict, Tuple

import pyarrow as pa
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Type registry: canonical type name -> {arrow, iceberg}
# ---------------------------------------------------------------------------

TYPE_REGISTRY = {
    "int32": {
        "arrow": pa.int32(),
        "iceberg": IntegerType(),
    },
    "int64": {
        "arrow": pa.int64(),
        "iceberg": LongType(),
    },
    "float32": {
        "arrow": pa.float32(),
        "iceberg": FloatType(),
    },
    "float64": {
        "arrow": pa.float64(),
        "iceberg": DoubleType(),
    },
    "string": {
        "arrow": pa.string(),
        "iceberg": StringType(),
    },
    "timestamp[us]": {
        "arrow": pa.timestamp("us"),
        "iceberg": TimestampType(),
    },
    "bool": {
        "arrow": pa.bool_(),
        "iceberg": BooleanType(),
    },
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_schema_config(json_schema_path: str) -> dict:
    """Load the raw JSON schema/config dict from disk."""
    with open(json_schema_path) as f:
        return json.load(f)


def _iceberg_type_name(iceberg_type) -> str:
    """Return canonical type name for an Iceberg type instance."""
    for name, spec in TYPE_REGISTRY.items():
        if isinstance(iceberg_type, type(spec["iceberg"])):
            return name
    raise NotImplementedError(f"Unsupported Iceberg type: {iceberg_type}")


def _arrow_type_name(pa_type: pa.DataType) -> str:
    """Return canonical type name for a PyArrow DataType."""
    name = str(pa_type)  # e.g. 'int32', 'timestamp[us]', 'string'
    if name not in TYPE_REGISTRY:
        raise NotImplementedError(f"Unsupported Arrow type: {pa_type}")
    return name


def _build_doc_from_metadata(metadata: Dict[str, object] | None) -> str | None:
    """
    Build a human-readable doc string from field metadata, if present.

    Convention:
        - if both description and unit are present:
              "Description [unit]"
        - else: description or unit or None
    """
    if not metadata:
        return None
    desc = metadata.get("description")
    unit = metadata.get("unit")
    if desc and unit:
        return f"{desc} [{unit}]"
    return desc or unit or None


# ---------------------------------------------------------------------------
# JSON <-> PyArrow
# ---------------------------------------------------------------------------


def json_to_arrow(json_schema_path: str) -> pa.Schema:
    """Convert a JSON schema file to a PyArrow schema."""
    schema_dict = _load_schema_config(json_schema_path)

    fields = []
    for field in schema_dict["fields"]:
        type_name = field["type"]
        if type_name not in TYPE_REGISTRY:
            raise NotImplementedError(f"Unsupported JSON type: {type_name}")

        pa_type = TYPE_REGISTRY[type_name]["arrow"]
        fields.append(pa.field(field["name"], pa_type, nullable=field["nullable"]))

    return pa.schema(fields)


def arrow_to_json(schema: pa.Schema, json_path: str) -> None:
    """Convert a PyArrow schema to a JSON schema file (fields only)."""
    fields = []
    for f in schema:
        type_name = _arrow_type_name(f.type)
        fields.append(
            {
                "name": f.name,
                "type": type_name,
                "nullable": f.nullable,
            }
        )

    with open(json_path, "w") as f:
        json.dump({"fields": fields}, f, indent=2)


# ---------------------------------------------------------------------------
# PyArrow <-> Iceberg
# ---------------------------------------------------------------------------


def arrow_to_iceberg(schema: pa.Schema) -> Schema:
    """Convert a PyArrow schema to an Iceberg schema."""
    fields = []
    for idx, field in enumerate(schema, start=1):
        type_name = _arrow_type_name(field.type)
        iceberg_type = TYPE_REGISTRY[type_name]["iceberg"]

        fields.append(
            NestedField(
                field_id=idx,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable,
            )
        )

    return Schema(*fields)


def iceberg_to_arrow(schema: Schema) -> pa.Schema:
    """Convert an Iceberg schema to a PyArrow schema."""
    fields = []
    for field in schema.fields:
        type_name = _iceberg_type_name(field.type)
        pa_type = TYPE_REGISTRY[type_name]["arrow"]

        fields.append(pa.field(field.name, pa_type, nullable=not field.required))

    return pa.schema(fields)


# ---------------------------------------------------------------------------
# JSON <-> Iceberg (plus metadata helpers)
# ---------------------------------------------------------------------------


def _dict_to_iceberg(schema_dict: dict) -> Schema:
    """
    Convert an in-memory JSON schema dict to an Iceberg Schema.

    Uses:
        - "fields" for types/nullability
        - per-field "metadata" (if present) to populate the Iceberg field doc
    """
    fields = []
    for idx, field in enumerate(schema_dict["fields"], start=1):
        type_name = field["type"]
        if type_name not in TYPE_REGISTRY:
            raise NotImplementedError(f"Unsupported JSON type: {type_name}")

        iceberg_type = TYPE_REGISTRY[type_name]["iceberg"]
        metadata = field.get("metadata") or {}
        doc = _build_doc_from_metadata(metadata)

        fields.append(
            NestedField(
                field_id=idx,
                name=field["name"],
                field_type=iceberg_type,
                required=not field["nullable"],
                doc=doc,
            )
        )

    return Schema(*fields)


def json_to_iceberg(json_schema_path: str) -> Schema:
    """Convert a JSON schema file to an Iceberg schema (fields + docs)."""
    schema_dict = _load_schema_config(json_schema_path)
    return _dict_to_iceberg(schema_dict)


def load_iceberg_schema_and_properties(
    json_schema_path: str,
    field_prefix: str = "field.",
) -> Tuple[Schema, Dict[str, str]]:
    """
    Load an Iceberg Schema and merged table properties from a JSON schema file.

    Returns:
        (schema, properties)

    Where `properties` contains:
        - the "table_properties" mapping from the JSON file (if any)
        - flattened per-field "metadata" entries, using `field_prefix`, e.g.:

              field.flow.unit = "cfs"
              field.flow.description = "Discharge at the site"
    """
    cfg = _load_schema_config(json_schema_path)

    schema = _dict_to_iceberg(cfg)
    table_props: Dict[str, str] = {k: str(v) for k, v in cfg.get("table_properties", {}).items()}

    # Flatten per-field metadata into table properties
    for field in cfg.get("fields", []):
        name = field["name"]
        metadata = field.get("metadata") or {}
        for key, value in metadata.items():
            table_props[f"{field_prefix}{name}.{key}"] = str(value)

    return schema, table_props


# ---------------------------------------------------------------------------
# Partition spec utility
# ---------------------------------------------------------------------------


def auto_partition_spec(schema: Schema, fields: list, next_id: int = 100) -> PartitionSpec:
    """
    Generate a simple Iceberg PartitionSpec with identity transforms on the
    given fields.

    Args:
        schema: The Iceberg schema object.
        fields: List of field names to include in the partition spec.
        next_id: Starting field ID for partition fields.

    Returns:
        PartitionSpec: The generated partition spec.

    Example:
        spec = auto_partition_spec(schema, ["event_id", "site_id"])
    """
    spec_fields = []

    for name in fields:
        source_id = schema.find_field(name).field_id
        spec_fields.append(
            PartitionField(
                source_id=source_id,
                field_id=next_id,
                transform=IdentityTransform(),
                name=name,
            )
        )
        next_id += 1

    return PartitionSpec(*spec_fields)
