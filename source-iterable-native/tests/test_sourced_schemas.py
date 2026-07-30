from typing import Any

import pytest
from jsonschema import Draft7Validator

from source_iterable_native.models import (
    CampaignMetrics,
    Campaigns,
    Channels,
    Events,
    ListUsers,
    Lists,
    MessageTypes,
    MetadataTables,
    Templates,
    UsersWithEmails,
    UsersWithIds,
    build_sourced_schema,
)


# Every stream paired with the collection key declared for it in resources.py.
# The pointers are duplicated here on purpose: a stream whose key fields aren't
# named in its sourced schema can have those fields squashed out of the inferred
# schema, silently dropping the key columns from downstream materializations.
STREAM_KEYS: list[tuple[type, list[str]]] = [
    (Channels, ["/_meta/row_id"]),
    (MessageTypes, ["/_meta/row_id"]),
    (MetadataTables, ["/_meta/row_id"]),
    (Templates, ["/_meta/row_id"]),
    (Lists, ["/_meta/row_id"]),
    (ListUsers, ["/list_id", "/user_id"]),
    (UsersWithEmails, ["/email"]),
    (UsersWithIds, ["/itblUserId"]),
    (Events, ["/_estuary_id", "/eventType"]),
    (Campaigns, ["/id"]),
    (CampaignMetrics, ["/id"]),
]

STREAM_IDS = [stream.__name__ for stream, _ in STREAM_KEYS]


def resolve_pointer(schema: dict[str, Any], pointer: str) -> dict[str, Any] | None:
    """Resolve a JSON pointer through a schema's `properties`, or None if absent."""
    node = schema
    for token in pointer.lstrip("/").split("/"):
        properties = node.get("properties", {})
        if token not in properties:
            return None
        node = properties[token]
    return node


def object_nodes(schema: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    """Every object node in the schema that declares properties, by pointer."""
    found = [("", schema)] if "properties" in schema else []
    for name, subschema in schema.get("properties", {}).items():
        found.extend(
            (f"/{name}{pointer}", node) for pointer, node in object_nodes(subschema)
        )
    return found


@pytest.mark.parametrize("stream,_keys", STREAM_KEYS, ids=STREAM_IDS)
def test_is_valid_json_schema(stream: type, _keys: list[str]):
    # Raises SchemaError if the schema is invalid.
    Draft7Validator.check_schema(build_sourced_schema(stream.KEY_PROPERTIES))


@pytest.mark.parametrize("stream,_keys", STREAM_KEYS, ids=STREAM_IDS)
def test_forbids_additional_properties(stream: type, _keys: list[str]):
    """Objects declaring properties must forbid additional properties.

    The runtime rejects a sourced schema that leaves any such object open
    (doc::Shape::inspect_closed), which fails the whole capture.
    """
    for pointer, node in object_nodes(build_sourced_schema(stream.KEY_PROPERTIES)):
        assert node.get("additionalProperties") is False, (
            f"{stream.__name__} allows additional properties at {pointer or '/'}"
        )


@pytest.mark.parametrize("stream,keys", STREAM_KEYS, ids=STREAM_IDS)
def test_names_every_key_field(stream: type, keys: list[str]):
    schema = build_sourced_schema(stream.KEY_PROPERTIES)

    for pointer in keys:
        node = resolve_pointer(schema, pointer)
        assert node is not None, f"{stream.__name__} key {pointer} is not named"
        assert "type" in node, f"{stream.__name__} key {pointer} declares no type"

    for pointer in keys:
        root_property = pointer.lstrip("/").split("/")[0]
        assert root_property in schema["required"], (
            f"{stream.__name__} does not require {root_property}"
        )


@pytest.mark.parametrize("stream,_keys", STREAM_KEYS, ids=STREAM_IDS)
def test_declares_bounds(stream: type, _keys: list[str]):
    """Every scalar must carry bounds.

    A union keeps a bound only when both sides have one, so a bound omitted here
    erases whatever bound schema inference had discovered for that field.
    """
    schema = build_sourced_schema(stream.KEY_PROPERTIES)

    for pointer, node in object_nodes(schema):
        for name, subschema in node["properties"].items():
            match subschema.get("type"):
                case "string":
                    required_bounds = ("minLength", "maxLength")
                case "integer" | "number":
                    required_bounds = ("minimum", "maximum")
                case _:
                    continue

            for bound in required_bounds:
                assert bound in subschema, (
                    f"{stream.__name__} omits {bound} at {pointer}/{name}"
                )


def test_meta_schema():
    """A representative check of the _meta block's declared values."""
    schema = build_sourced_schema({})

    assert schema["properties"]["_meta"]["properties"] == {
        "op": {
            "type": "string",
            "enum": ["c", "u", "d"],
            "minLength": 1,
            "maxLength": 1,
        },
        "row_id": {"type": "integer", "minimum": -1, "maximum": -1},
    }
