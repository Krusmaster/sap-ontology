"""Export the same graph as a Fabric *API for GraphQL* item.

One graph, two consumers: the Ontology item feeds Fabric IQ (Copilot, agents); the API for GraphQL
item feeds apps (Lovable's Fabric connector, any GraphQL client). Both need the same thing — which
tables, which keys, which relationships — and that is what this package already derives.

The GraphQLApi item is GA and its definition is documented:
https://learn.microsoft.com/rest/api/fabric/articles/item-management/definitions/graphql-api-definition

Verified against a live item in a sandbox workspace (2026-10-21). Four things the docs do not say:
- no "format" key in the definition envelope (InvalidDefinitionFormat otherwise)
- relationships[].targetObject is the target's *sourceObject* ("dbo.i_product"), not its graphqlType
- fieldMappings must list every column the SQL endpoint has, the mirror's __sequence_number and
  __timestamp included; the GraphQL name drops the leading "__" (reserved in GraphQL)
- a relationship field named "items" collides with the connection's own `items` list (500); the
  header -> items field is therefore "<item>_lines"
Also: a Lakehouse is exposed through its SQL analytics endpoint (sourceItemId = the endpoint's id,
`sql_endpoint_id_for_lakehouse()`); lakehouse and mirrored sources are read-only. Query fields are
named i_<TYPE>s, e.g. i_CUSTOMERs. A failed create locks the name for a few minutes.
"""
from __future__ import annotations

import base64
import json
import re
import uuid

import networkx as nx

from .export import valid_item_name

SCHEMA_URL = "https://developer.microsoft.com/json-schemas/fabric/item/graphqlApi/definition/1.0.0/schema.json"
_GQL_NAME = re.compile(r"[^A-Za-z0-9_]")


def graphql_name(name: str) -> str:
    """GraphQL identifiers: letters, digits, underscore; must not start with a digit."""
    n = _GQL_NAME.sub("_", name)
    return n if not n[:1].isdigit() else f"_{n}"


def fabric_graphql_definition(
    g: nx.DiGraph,
    display_name: str,
    workspace_id: str,
    source_item_id: str,
    source_type: str = "SqlAnalyticsEndpoint",
    schema: str = "dbo",
    table_name_map: dict[str, str] | None = None,
    writable: bool = False,
    include_text_views: bool = True,
    include_header_items: bool = True,
) -> dict:
    """Build the GraphQLApi item envelope for POST /v1/workspaces/{ws}/graphQLApis.

    source_item_id: the SQL analytics endpoint id for a lakehouse (SqlAnalyticsEndpoint), or the
                    warehouse / SQL database item id (Warehouse, SqlDbNative).
    writable:       enable Create/Update/Delete (only meaningful for Warehouse / SqlDbNative;
                    lakehouse and mirrored sources are read-only through GraphQL).
    Each graph edge a -> b becomes a ManyToOne relationship on a (field = target type name,
    sourceFields = the referencing columns, targetFields = b's key) and the reverse OneToMany on b.
    Header/item pairs from suggest_business_objects (VBAK -> VBAP on VBELN) are not reference
    edges in the graph, but for an app they are the most useful relationship: with
    include_header_items they become OneToMany on the header ("items") and ManyToOne on the item
    ("header"), joined on the header's key.
    Entities without keys are skipped (no Query_by_pk possible, and no relationship target).
    """
    if not valid_item_name(display_name):
        raise ValueError(f"item name {display_name!r}: letters, digits and underscores only")
    if source_type not in ("SqlAnalyticsEndpoint", "Warehouse", "SqlDbNative", "AzureSql"):
        raise ValueError(f"unknown source_type {source_type}")
    table_name_map = table_name_map or {}

    def table_name(t: str) -> str:
        return table_name_map.get(t) or g.nodes[t].get("catalog_name") or t

    objects: dict[str, dict] = {}
    skipped: list[str] = []
    for table, d in g.nodes(data=True):
        if not d.get("keys"):
            skipped.append(table)
            continue
        # every column the endpoint has, technical ones included (else DataSourceMetadataKeyFieldInFieldMappings)
        cols = d.get("columns") or [f["FIELDNAME"] for f in d.get("fields", [])]
        actions = {"Query": "Enabled", "Query_by_pk": "Enabled"}
        if writable and source_type in ("Warehouse", "SqlDbNative", "AzureSql"):
            actions.update({"Create": "Enabled", "Update": "Enabled", "Delete": "Disabled"})
        objects[table] = {
            "graphqlType": graphql_name(table),
            "sourceObject": f"{schema}.{table_name(table)}",
            "sourceObjectType": "Table",
            "actions": actions,
            "fieldMappings": {c: graphql_name(c.lstrip("_")) for c in cols},
            "relationships": [],
        }

    for a, b, d in g.edges(data=True):
        if a not in objects or b not in objects:
            continue
        if d.get("kind") == "TEXT_OF" and not include_text_views:
            continue
        mapping = d.get("mapping") or [(k, k) for k in g.nodes[b]["keys"]]
        src_fields = [s for s, _ in mapping]
        tgt_fields = [t for _, t in mapping]
        objects[a]["relationships"].append({
            "field": graphql_name(b).lower() if d.get("kind") != "TEXT_OF" else "master",
            "cardinality": "ManyToOne",
            "targetObject": objects[b]["sourceObject"],
            "sourceFields": src_fields,
            "targetFields": tgt_fields,
        })
        objects[b]["relationships"].append({
            "field": graphql_name(a).lower() + ("_texts" if d.get("kind") == "TEXT_OF" else "s"),
            "cardinality": "OneToMany",
            "targetObject": objects[a]["sourceObject"],
            "sourceFields": tgt_fields,
            "targetFields": src_fields,
        })

    if include_header_items:
        from .dd import suggest_business_objects
        for bo in suggest_business_objects(g):
            head = bo["header"]
            if head not in objects:
                continue
            for item in bo["items"]:
                if item not in objects:
                    continue
                keys = bo["join_keys"]
                objects[head]["relationships"].append({
                    "field": graphql_name(item).lower() + "_lines",   # "items" collides with the connection's list
                    "cardinality": "OneToMany", "targetObject": objects[item]["sourceObject"],
                    "sourceFields": keys, "targetFields": keys,
                })
                objects[item]["relationships"].append({
                    "field": "header", "cardinality": "ManyToOne", "targetObject": objects[head]["sourceObject"],
                    "sourceFields": keys, "targetFields": keys,
                })

    definition = {
        "$schema": SCHEMA_URL,
        "datasources": [{
            "sourceItemId": source_item_id,
            "sourceWorkspaceId": workspace_id,
            "sourceType": source_type,
            "objects": list(objects.values()),
        }],
    }
    platform = {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                "metadata": {"type": "GraphQLApi", "displayName": display_name},
                "config": {"version": "2.0", "logicalId": str(uuid.uuid5(uuid.NAMESPACE_URL, "graphql|" + display_name))}}

    def b64(o: dict) -> str:
        return base64.b64encode(json.dumps(o, separators=(",", ":")).encode()).decode()

    env = {
        "displayName": display_name,
        "type": "GraphQLApi",
        "definition": {"parts": [
            {"path": "graphql-definition.json", "payload": b64(definition), "payloadType": "InlineBase64"},
            {"path": ".platform", "payload": b64(platform), "payloadType": "InlineBase64"},
        ]},
    }
    if skipped:
        env["skipped"] = skipped
    return env


def sql_endpoint_id_for_lakehouse(workspace_id: str, lakehouse_id: str, headers: dict) -> str:
    """A lakehouse is queried through its SQL analytics endpoint; GraphQL needs that endpoint's id."""
    import requests
    r = requests.get(f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
                     headers=headers, timeout=30)
    r.raise_for_status()
    props = r.json().get("properties", {}).get("sqlEndpointProperties", {})
    if not props.get("id"):
        raise RuntimeError("lakehouse has no SQL analytics endpoint yet (provisioning status: %s)" % props.get("provisioningStatus"))
    return props["id"]
