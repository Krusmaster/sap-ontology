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
import re
import uuid

import networkx as nx

_TYPE_MAP = {
    "INT1": "BigInt", "INT2": "BigInt", "INT4": "BigInt", "INT8": "BigInt",
    "DEC": "Double", "CURR": "Double", "QUAN": "Double", "FLTP": "Double", "DF16_DEC": "Double", "DF34_DEC": "Double",
    "DATS": "DateTime",   # NUMC stays String on purpose (leading zeros)
}


def _id64(*parts: str) -> str:
    """Deterministic positive int64 from names, AS A STRING: Fabric's importer refuses JSON numbers
    for id fields ("The JSON value could not be converted to System.String. Path: $.id")."""
    h = hashlib.sha256("|".join(parts).encode()).digest()
    return str(int.from_bytes(h[:8], "big") & 0x7FFFFFFFFFFFFFFF or 1)


def _guid(*parts: str) -> str:
    """Deterministic GUID for data bindings and contextualizations (Fabric expects GUID ids there)."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "|".join(parts)))


_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")


def valid_item_name(name: str) -> bool:
    """Fabric refuses Ontology item names with anything but letters, digits and underscores."""
    return bool(_NAME_RE.match(name))


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
    namespace: str = "usertypes",
    table_name_map: dict[str, str] | None = None,
) -> dict:
    """Build the item-definition envelope for POST /v1/workspaces/{ws}/items.

    Shape follows a definition Fabric accepted on create and returned via Get Item Definition
    (2026-10): all ids are STRINGS (int64 as text for entity/property/relationship ids, GUIDs for
    bindings and contextualizations); 2- and 3-part entityIdParts are accepted.
    - EntityTypes/<id>/definition.json: entityIdParts, namespace "usertypes", namespaceType "Custom",
      visibility "Visible", baseEntityTypeId null, properties[], timeseriesProperties[]
    - EntityTypes/<id>/DataBindings/<id>.json: {id, dataBindingConfiguration{dataBindingType,
      propertyBindings[{sourceColumnName, targetPropertyId}], sourceTableProperties{sourceType,
      workspaceId, itemId, sourceSchema, sourceTableName}}}
    - RelationshipTypes/<id>/definition.json + Contextualizations/<id>.json: the mapping table is the
      source entity's own table (its key columns -> source entity key, referencing column(s) ->
      target entity key).

    Table names: node attribute `catalog_name` if present (what the Spark catalog lists — lower case
    when Spark wrote the table, upper case on a mirror), else the node name; table_name_map overrides.
    Entities without keys (custom views with no custom_keys) are SKIPPED and listed in
    env["skipped"] — Fabric needs entityIdParts. Multi-part keys are emitted as-is; whether Fabric
    accepts 2–3 part keys is not yet verified.
    """
    skipped: list[str] = []
    if not valid_item_name(display_name):
        raise ValueError(f"item name {display_name!r}: Fabric allows only letters, digits and underscores (e.g. SAP_Sales)")
    table_name_map = table_name_map or {}
    parts: list[dict] = []

    def add(path: str, obj: dict) -> None:
        payload = base64.b64encode(json.dumps(obj, separators=(",", ":")).encode()).decode()
        parts.append({"path": path, "payload": payload, "payloadType": "InlineBase64"})

    def table_props(table: str) -> dict:
        name = table_name_map.get(table) or g.nodes[table].get("catalog_name") or table
        return {"sourceType": "LakehouseTable", "workspaceId": workspace_id, "itemId": lakehouse_id,
                "sourceSchema": schema, "sourceTableName": name}

    add(".platform", {"$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
                      "metadata": {"type": "Ontology", "displayName": display_name},
                      "config": {"version": "2.0", "logicalId": str(uuid.uuid5(uuid.NAMESPACE_URL, display_name))}})
    add("definition.json", {})

    entity_ids: dict[str, int] = {}
    prop_ids: dict[tuple[str, str], int] = {}
    for table, d in g.nodes(data=True):
        if not d.get("keys"):
            skipped.append(table)
            continue
        eid = _id64("entity", table)
        entity_ids[table] = eid
        props = []
        for f in d.get("fields", []):
            pid = _id64("prop", table, f["FIELDNAME"])
            prop_ids[(table, f["FIELDNAME"].upper())] = pid
            props.append({"id": pid, "name": f["FIELDNAME"], "valueType": value_type(f.get("DATATYPE", ""))})
        key_ids = [prop_ids[(table, k.upper())] for k in d.get("keys", []) if (table, k.upper()) in prop_ids]
        display_prop = key_ids[-1] if key_ids else (props[0]["id"] if props else None)
        add(f"EntityTypes/{eid}/definition.json", {
            "id": eid,
            "name": table,
            "namespace": namespace,
            "namespaceType": "Custom",
            "visibility": "Visible",
            "baseEntityTypeId": None,
            "description": d.get("text", ""),
            "entityIdParts": key_ids,
            "displayNamePropertyId": display_prop,
            "properties": props,
            "timeseriesProperties": [],
        })
        bid = _guid("binding", table)
        add(f"EntityTypes/{eid}/DataBindings/{bid}.json", {
            "id": bid,
            "dataBindingConfiguration": {
                "dataBindingType": "NonTimeSeries",
                "propertyBindings": [{"sourceColumnName": p["name"], "targetPropertyId": p["id"]} for p in props],
                "sourceTableProperties": table_props(table),
            },
        })

    for a, b, d in g.edges(data=True):
        if a not in entity_ids or b not in entity_ids:
            continue                                      # one end was skipped (no keys)
        field = d.get("field", "")
        rid = _id64("rel", a, b, field)
        add(f"RelationshipTypes/{rid}/definition.json", {
            "id": rid,
            "name": f"{a}_{field.replace(',', '_') or 'ref'}_{b}",
            "namespace": namespace,
            "namespaceType": "Custom",
            "source": {"entityTypeId": entity_ids[a]},
            "target": {"entityTypeId": entity_ids[b]},
        })
        # Contextualization: the source table maps its own key -> source entity, referencing col(s) -> target key.
        # Fabric requires len(targetKeyRefBindings) == len(target entityIdParts); check it here, by name.
        src_keys = g.nodes[a].get("keys", [])
        tgt_keys = [k.upper() for k in g.nodes[b].get("keys", [])]
        src_cols = {f["FIELDNAME"].upper() for f in g.nodes[a].get("fields", [])}
        mapping = d.get("mapping")
        if not mapping:
            ref_cols = [c.strip().upper() for c in field.split(",") if c.strip()] or tgt_keys
            by_name = {c: c for c in ref_cols if c in tgt_keys}
            rest = [c for c in ref_cols if c not in by_name]
            mapping = []
            for tk in tgt_keys:
                src = by_name.get(tk) or (rest.pop(0) if rest else (tk if tk in src_cols else None))
                if src is None:
                    raise ValueError(f"relationship {a} -> {b}: cannot map target key part {tk} from {a}'s columns")
                mapping.append((src, tk))
        mapping = [(src.upper(), tk.upper()) for src, tk in mapping]
        if len(mapping) != len(tgt_keys) or [tk for _, tk in mapping] != tgt_keys:
            raise ValueError(f"relationship {a} -> {b}: mapping {mapping} does not cover target key {tgt_keys}")
        bad = [src for src, _ in mapping if src not in src_cols]
        if bad:
            raise ValueError(f"relationship {a} -> {b}: column(s) {bad} not in {a}")
        cid = _guid("ctx", a, b, field)
        add(f"RelationshipTypes/{rid}/Contextualizations/{cid}.json", {
            "id": cid,
            "dataBindingTable": table_props(a),
            "sourceKeyRefBindings": [{"sourceColumnName": k, "targetPropertyId": prop_ids[(a, k.upper())]}
                                     for k in src_keys if (a, k.upper()) in prop_ids],
            "targetKeyRefBindings": [{"sourceColumnName": src, "targetPropertyId": prop_ids[(b, tk)]}
                                     for src, tk in mapping],
        })

    env = {"displayName": display_name, "type": "Ontology", "definition": {"parts": parts}}
    if skipped:
        env["skipped"] = skipped                           # strip before POST; deploy() does
    return env


# ---- Graph database exports -------------------------------------------------------
def graphml(g: nx.DiGraph) -> str:
    """GraphML for Gephi, yEd, Neo4j (apoc.import.graphml), NetworkX round-trip."""
    import io
    h = nx.DiGraph()
    for n, d in g.nodes(data=True):
        h.add_node(n, label=n, text=d.get("text", ""), keys=",".join(d.get("keys", [])), n_fields=len(d.get("fields", [])))
    for a, b, d in g.edges(data=True):
        h.add_edge(a, b, field=d.get("field", ""), kind=d.get("kind", ""))
    buf = io.BytesIO()
    nx.write_graphml(h, buf)
    return buf.getvalue().decode("utf-8")


def neo4j_csv(g: nx.DiGraph) -> tuple[str, str]:
    """(nodes.csv, rels.csv) for neo4j-admin import / LOAD CSV.

    nodes:  name:ID, text, keys, n_fields:int, :LABEL
    rels:   :START_ID, :END_ID, field, kind, :TYPE
    """
    import csv, io
    nb, rb = io.StringIO(), io.StringIO()
    nw = csv.writer(nb); nw.writerow(["name:ID", "text", "keys", "n_fields:int", ":LABEL"])
    for n, d in g.nodes(data=True):
        nw.writerow([n, d.get("text", ""), "|".join(d.get("keys", [])), len(d.get("fields", [])), "SapTable"])
    rw = csv.writer(rb); rw.writerow([":START_ID", ":END_ID", "field", "kind", ":TYPE"])
    for a, b, d in g.edges(data=True):
        rw.writerow([a, b, d.get("field", ""), d.get("kind", ""), "REFERENCES"])
    return nb.getvalue(), rb.getvalue()


def kql_graph(g: nx.DiGraph) -> str:
    """A KQL snippet that builds the same graph in Fabric Eventhouse with make-graph."""
    nodes = ",\n    ".join(f'dynamic({{"name":"{n}","keys":"{",".join(d.get("keys", []))}"}})' for n, d in g.nodes(data=True))
    edges = ",\n    ".join(f'dynamic({{"src":"{a}","dst":"{b}","field":"{d.get("field", "")}"}})' for a, b, d in g.edges(data=True))
    return (
        "let Nodes = datatable(n:dynamic)[\n    " + nodes + "\n] | extend name=tostring(n.name), keys=tostring(n.keys);\n"
        "let Edges = datatable(e:dynamic)[\n    " + edges + "\n] | extend src=tostring(e.src), dst=tostring(e.dst), field=tostring(e.field);\n"
        "Edges\n| make-graph src --> dst with Nodes on name\n"
        "| graph-match (header)-[r]->(ref)\n  project header=header.name, field=r.field, ref=ref.name\n"
    )
