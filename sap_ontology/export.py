"""Exporters: Mermaid (for humans) and a Fabric IQ Ontology item definition (for the platform).

The Fabric IQ ontology format is in preview and documented only by example. The structure here
follows Microsoft's ontology-authoring skill (EntityTypes/{id}/definition.json, DataBindings,
RelationshipTypes). Before relying on field names, create one ontology by hand in your tenant,
fetch it with Get Item Definition, and diff against `fabric_iq_definition()` output.
"""
from __future__ import annotations

import base64
import hashlib
import json
import uuid

import networkx as nx

_TYPE_MAP = {
    "INT1": "BigInt", "INT2": "BigInt", "INT4": "BigInt", "INT8": "BigInt",
    "DEC": "Double", "CURR": "Double", "QUAN": "Double", "FLTP": "Double", "DF16_DEC": "Double", "DF34_DEC": "Double",
}


def _id64(*parts: str) -> int:
    """Deterministic positive int64 from names, so re-runs produce the same ids."""
    h = hashlib.sha256("|".join(parts).encode()).digest()
    return int.from_bytes(h[:8], "big") & 0x7FFFFFFFFFFFFFFF or 1


def value_type(sap_datatype: str) -> str:
    return _TYPE_MAP.get(str(sap_datatype).upper(), "String")


def mermaid(g: nx.DiGraph, max_edges: int = 200) -> str:
    lines = ["erDiagram"]
    for n, d in g.nodes(data=True):
        keys = ", ".join(d.get("keys", [])) or "-"
        text = (d.get("text") or "").replace('"', "'")
        lines.append(f'    {n} {{ string keys "{keys}" }}')
        if text:
            lines.append(f"    %% {n}: {text}")
    for i, (a, b, d) in enumerate(g.edges(data=True)):
        if i >= max_edges:
            lines.append(f"    %% ... {g.number_of_edges() - max_edges} more edges")
            break
        lines.append(f'    {a} }}o--|| {b} : "{d.get("field", "")}"')
    return "\n".join(lines)


def fabric_iq_definition(
    g: nx.DiGraph,
    display_name: str,
    workspace_id: str,
    lakehouse_id: str,
    schema: str = "dbo",
    namespace: str = "sap",
    table_name_map: dict[str, str] | None = None,
) -> dict:
    """Build the item-definition envelope for POST /v1/workspaces/{ws}/items.

    Each SAP table becomes an entity type with its key fields as keys, bound to the mirrored
    lakehouse table of the same name (override via table_name_map). Each DD08L foreign key
    becomes a relationship type.
    """
    table_name_map = table_name_map or {}
    parts: list[dict] = []

    def add(path: str, obj: dict) -> None:
        payload = base64.b64encode(json.dumps(obj, separators=(",", ":")).encode()).decode()
        parts.append({"path": path, "payload": payload, "payloadType": "InlineBase64"})

    add(".platform", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                      "metadata": {"type": "Ontology", "displayName": display_name},
                      "config": {"version": "2.0", "logicalId": str(uuid.uuid5(uuid.NAMESPACE_URL, display_name))}})
    add("definition.json", {})

    entity_ids: dict[str, int] = {}
    for table, d in g.nodes(data=True):
        eid = _id64("entity", table)
        entity_ids[table] = eid
        props = []
        for f in d.get("fields", []):
            props.append({
                "id": _id64("prop", table, f["FIELDNAME"]),
                "name": f["FIELDNAME"],
                "valueType": value_type(f.get("DATATYPE", "")),
            })
        keys = [p["id"] for p in props if p["name"] in set(d.get("keys", []))]
        display_prop = keys[-1] if keys else (props[0]["id"] if props else None)
        add(f"EntityTypes/{eid}/definition.json", {
            "id": eid,
            "name": table,
            "namespace": namespace,
            "description": d.get("text", ""),
            "keyPropertyIds": keys,
            "displayNamePropertyId": display_prop,
            "properties": props,
            "timeseriesProperties": [],
        })
        add(f"EntityTypes/{eid}/DataBindings/{uuid.uuid5(uuid.NAMESPACE_URL, table)}.json", {
            "dataBindingType": "NonTimeSeries",
            "source": {
                "type": "LakehouseTable",
                "workspaceId": workspace_id,
                "itemId": lakehouse_id,
                "sourceSchema": schema,
                "sourceTableName": table_name_map.get(table, table.lower()),
            },
            "propertyBindings": [{"propertyId": p["id"], "sourceColumnName": p["name"]} for p in props],
        })

    for a, b, d in g.edges(data=True):
        rid = _id64("rel", a, b, d.get("field", ""))
        add(f"RelationshipTypes/{rid}/definition.json", {
            "id": rid,
            "name": f"{a}_{d.get('field', 'ref')}_{b}",
            "namespace": namespace,
            "source": {"entityTypeId": entity_ids[a]},
            "target": {"entityTypeId": entity_ids[b]},
        })

    return {"displayName": display_name, "type": "Ontology", "definition": {"parts": parts}}
