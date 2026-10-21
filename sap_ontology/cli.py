"""sap-ontology: SAP Data Dictionary -> graph -> business-object suggestions -> Fabric IQ ontology.

    sap-ontology graph  ./dd --tables VBAK,VBAP,KNA1 --mermaid out.mmd --graphml out.graphml --neo4j ./neo4j --kql out.kql
    sap-ontology objects ./dd
    sap-ontology ontology ./dd --name "SAP Sales" --workspace <ws-id> --lakehouse <lh-id> --out ontology.json
    sap-ontology deploy ontology.json --workspace <ws-id>      # needs: az login

`deploy` calls the Fabric REST API with a token from `az account get-access-token`. Nothing is
sent anywhere unless you run `deploy`.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from .dd import build_graph, load_dd, suggest_business_objects
from .mirror import from_dd03nd, infer_edges, match_tables
from .graphql import fabric_graphql_definition
from .export import fabric_iq_definition, graphml, kql_graph, mermaid, neo4j_csv

FABRIC = "https://api.fabric.microsoft.com"


def _tables(arg: str | None) -> list[str] | None:
    return [t.strip().upper() for t in arg.split(",")] if arg else None


def cmd_graph(a: argparse.Namespace) -> int:
    g = build_graph(load_dd(a.dd), _tables(a.tables))
    print(f"{g.number_of_nodes()} tables, {g.number_of_edges()} foreign keys")
    if a.mermaid:
        Path(a.mermaid).write_text(mermaid(g), encoding="utf-8"); print(f"wrote {a.mermaid}")
    if a.graphml:
        Path(a.graphml).write_text(graphml(g), encoding="utf-8"); print(f"wrote {a.graphml}")
    if a.neo4j:
        nodes, rels = neo4j_csv(g); d = Path(a.neo4j); d.mkdir(parents=True, exist_ok=True)
        (d / "nodes.csv").write_text(nodes, encoding="utf-8"); (d / "rels.csv").write_text(rels, encoding="utf-8")
        print(f"wrote {d}/nodes.csv, {d}/rels.csv")
    if a.kql:
        Path(a.kql).write_text(kql_graph(g), encoding="utf-8"); print(f"wrote {a.kql}")
    return 0


def cmd_objects(a: argparse.Namespace) -> int:
    g = build_graph(load_dd(a.dd), _tables(a.tables))
    for s in suggest_business_objects(g):
        text = f" — {s['text']}" if s["text"] else ""
        print(f"{s['object']}{text}")
        print(f"    header: {s['header']}  items: {', '.join(s['items'])}  join on: {', '.join(s['join_keys'])}")
    return 0


def cmd_ontology(a: argparse.Namespace) -> int:
    g = build_graph(load_dd(a.dd), _tables(a.tables))
    env = fabric_iq_definition(g, a.name, a.workspace, a.lakehouse, schema=a.schema)
    Path(a.out).write_text(json.dumps(env, indent=2), encoding="utf-8")
    print(f"wrote {a.out}: {g.number_of_nodes()} entity types, {g.number_of_edges()} relationship types")
    return 0


def token_from_az() -> str:
    """Local use: az login first."""
    r = subprocess.run(["az", "account", "get-access-token", "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()


def token_from_fabric() -> str:
    """Inside a Fabric notebook: the running user's token via notebookutils."""
    import notebookutils  # type: ignore
    return notebookutils.credentials.getToken(FABRIC)


def default_token_provider():
    try:
        import notebookutils  # type: ignore  # noqa: F401
        return token_from_fabric
    except Exception:
        return token_from_az


def find_item(workspace: str, display_name: str, headers: dict, item_type: str = "Ontology") -> str | None:
    """Return the id of an existing item with this name and type in the workspace, else None."""
    import requests
    url = f"{FABRIC}/v1/workspaces/{workspace}/items?type={item_type}"
    while url:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
        body = r.json()
        for it in body.get("value", []):
            if it.get("displayName") == display_name:
                return it["id"]
        url = body.get("continuationUri")
    return None


def deploy(definition: dict, workspace: str, token_provider=None, poll_seconds: int = 3, max_polls: int = 60,
           update_existing: bool = True) -> dict:
    """Create the Ontology item, or UPDATE it if one with the same displayName already exists.

    Creating is a POST /items; every create also adds a Lakehouse (+ SQL endpoint) and a GraphModel
    beside the ontology — even when the import fails. Re-running therefore uses
    POST /items/{id}/updateDefinition, which replaces the whole definition and creates nothing new.
    Set update_existing=False to force a new item.

    token_provider: callable returning a bearer token. Defaults to notebookutils inside Fabric, az locally.
    Returns the final operation status dict, with "mode": "created" | "updated".
    """
    import requests  # optional dependency, only for deploy

    definition = {k: v for k, v in definition.items() if k != "skipped"}
    token = (token_provider or default_token_provider())()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    item_type = definition.get("type", "Ontology")
    existing = find_item(workspace, definition["displayName"], headers, item_type) if update_existing else None
    if existing:
        r = requests.post(f"{FABRIC}/v1/workspaces/{workspace}/items/{existing}/updateDefinition",
                          headers=headers, json={"definition": definition["definition"]}, timeout=60)
        mode = "updated"
    else:
        route = {"GraphQLApi": "graphQLApis"}.get(item_type, "items")
        body = definition if route == "items" else {k: v for k, v in definition.items() if k != "type"}
        r = requests.post(f"{FABRIC}/v1/workspaces/{workspace}/{route}", headers=headers, json=body, timeout=60)
        mode = "created"

    if r.status_code not in (200, 201, 202):
        return {"status": "Failed", "mode": mode, "http": r.status_code, "body": r.text}
    op = r.headers.get("x-ms-operation-id")
    if not op:
        return {"status": "Succeeded", "mode": mode, "id": existing or (r.json().get("id") if r.text else None)}
    for _ in range(max_polls):
        s = requests.get(f"{FABRIC}/v1/operations/{op}", headers=headers, timeout=30).json()
        if s.get("status") in ("Succeeded", "Failed"):
            s["mode"] = mode
            return s
        time.sleep(poll_seconds)
    return {"status": "Timeout", "mode": mode, "operation": op}


def cmd_graphql(a: argparse.Namespace) -> int:
    """Classic DD input -> API for GraphQL item definition (mirror input: use the library, see README)."""
    g = build_graph(load_dd(a.dd), _tables(a.tables))
    env = fabric_graphql_definition(g, a.name, a.workspace, a.source_item, source_type=a.source_type, schema=a.schema,
                                    writable=a.writable)
    for t in env.get("skipped", []):
        print(f"  skipped (no keys): {t}")
    Path(a.out).write_text(json.dumps(env, indent=2), encoding="utf-8")
    n = len(json.loads(__import__("base64").b64decode(env["definition"]["parts"][0]["payload"]))["datasources"][0]["objects"])
    print(f"wrote {a.out}: {n} types")
    return 0


def cmd_deploy(a: argparse.Namespace) -> int:
    env = json.loads(Path(a.definition).read_text(encoding="utf-8"))
    s = deploy(env, a.workspace)
    print(s.get("status"), json.dumps({k: v for k, v in s.items() if k != "status"}, indent=2, default=str))
    return 0 if s.get("status") == "Succeeded" else 1


def cmd_mirror(a: argparse.Namespace) -> int:
    """Build from a DD03ND export + a list of lakehouse table names (local dry run of from_lakehouse)."""
    import pandas as pd
    import yaml
    dd03nd = pd.read_csv(a.dd03nd, dtype=str)
    tables = [t.strip() for t in Path(a.tables).read_text(encoding="utf-8").split() if t.strip()]
    name_map = yaml.safe_load(Path(a.name_map).read_text()) if a.name_map else None
    custom_keys = yaml.safe_load(Path(a.custom_keys).read_text()) if a.custom_keys else None
    g, rep = from_dd03nd(dd03nd, tables, custom_keys=custom_keys, strip_suffix=a.strip_suffix, name_map=name_map)
    conf = yaml.safe_load(Path(a.relationships).read_text()) if a.relationships else None
    h, amb = infer_edges(g, confirmed=conf, include_text_key_edges=a.include_text_key_edges)
    print("match:", rep.summary())
    for t in rep.unmatched_tables:
        print(f"  unmatched table: {t}{'  (custom Z/Y)' if t in rep.custom_views else ''}")
    for v in rep.multi_node_views:
        print(f"  warning: {v} has more than one nodename in DD03ND")
    kinds = {}
    for _, _, d in h.edges(data=True):
        kinds[d.get("kind", "?")] = kinds.get(d.get("kind", "?"), 0) + 1
    print(f"graph: {h.number_of_nodes()} tables, {h.number_of_edges()} edges " + ", ".join(f"{k.lower()} {v}" for k, v in sorted(kinds.items())))
    for x in amb:
        print(f"  ambiguous: {x['column']} is the key of {', '.join(x['owners'])} — add a relationships YAML entry")
    for s_ in suggest_business_objects(h):
        print(f"  object {s_['header']} + {', '.join(s_['items'])} on {', '.join(s_['join_keys'])}")
    if a.mermaid:
        Path(a.mermaid).write_text(mermaid(h), encoding="utf-8"); print(f"wrote {a.mermaid}")
    if a.out:
        if not (a.workspace and a.lakehouse):
            print("--out needs --workspace and --lakehouse", file=sys.stderr); return 2
        env = fabric_iq_definition(h, a.name or "SAP_Mirror", a.workspace, a.lakehouse)
        for t in env.get("skipped", []):
            print(f"  skipped (no keys, add to --custom-keys): {t}")
        Path(a.out).write_text(json.dumps(env, indent=2), encoding="utf-8"); print(f"wrote {a.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="sap-ontology", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("graph", help="build the FK graph, optionally write Mermaid")
    p.add_argument("dd"); p.add_argument("--tables"); p.add_argument("--mermaid")
    p.add_argument("--graphml", help="write GraphML (Gephi, yEd, Neo4j apoc)")
    p.add_argument("--neo4j", help="directory for nodes.csv + rels.csv (neo4j-admin import)")
    p.add_argument("--kql", help="write a KQL make-graph snippet for Fabric Eventhouse")
    p.set_defaults(fn=cmd_graph)

    p = sub.add_parser("objects", help="suggest header/item business objects")
    p.add_argument("dd"); p.add_argument("--tables")
    p.set_defaults(fn=cmd_objects)

    p = sub.add_parser("ontology", help="write a Fabric IQ ontology item definition")
    p.add_argument("dd"); p.add_argument("--tables")
    p.add_argument("--name", required=True); p.add_argument("--workspace", required=True)
    p.add_argument("--lakehouse", required=True); p.add_argument("--schema", default="dbo")
    p.add_argument("--out", default="ontology.json")
    p.set_defaults(fn=cmd_ontology)

    p = sub.add_parser("mirror", help="SAP Mirror input: DD03ND.csv + a file listing lakehouse table names")
    p.add_argument("dd03nd"); p.add_argument("tables", help="text file, one mirrored table name per line")
    p.add_argument("--strip-suffix", action="store_true", help="match I_VIEW_P01 to I_VIEW (opt-in)")
    p.add_argument("--name-map", help="YAML {table: strucobjn}")
    p.add_argument("--custom-keys", help="YAML {table: [key columns]} for Z/Y views")
    p.add_argument("--relationships", help="YAML list of {from,to,field,action}")
    p.add_argument("--include-text-key-edges", action="store_true", help="keep edges to LANGUAGE-style tables (noise by default)")
    p.add_argument("--mermaid"); p.add_argument("--out"); p.add_argument("--name")
    p.add_argument("--workspace"); p.add_argument("--lakehouse")
    p.set_defaults(fn=cmd_mirror)

    p = sub.add_parser("graphql", help="write a Fabric API for GraphQL item definition")
    p.add_argument("dd"); p.add_argument("--tables")
    p.add_argument("--name", required=True); p.add_argument("--workspace", required=True)
    p.add_argument("--source-item", required=True, help="SQL analytics endpoint id (lakehouse) or warehouse/SQL db item id")
    p.add_argument("--source-type", default="SqlAnalyticsEndpoint", choices=["SqlAnalyticsEndpoint", "Warehouse", "SqlDbNative", "AzureSql"])
    p.add_argument("--schema", default="dbo"); p.add_argument("--writable", action="store_true")
    p.add_argument("--out", default="graphql.json")
    p.set_defaults(fn=cmd_graphql)

    p = sub.add_parser("deploy", help="POST the definition to a Fabric workspace (notebookutils in Fabric, az locally)")
    p.add_argument("definition"); p.add_argument("--workspace", required=True)
    p.set_defaults(fn=cmd_deploy)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
