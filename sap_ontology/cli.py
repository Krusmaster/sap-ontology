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


def deploy(definition: dict, workspace: str, token_provider=None, poll_seconds: int = 3, max_polls: int = 60) -> dict:
    """POST the ontology definition and wait for the long-running operation. Returns the final status dict.

    token_provider: callable returning a bearer token. Defaults to notebookutils inside Fabric, az locally.
    Note: creating an Ontology item can also create companion items (Eventhouse, KQL database,
    graph model) in the workspace. Use a sandbox workspace first.
    """
    import requests  # optional dependency, only for deploy

    token = (token_provider or default_token_provider())()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    r = requests.post(f"{FABRIC}/v1/workspaces/{workspace}/items", headers=headers, json=definition, timeout=60)
    if r.status_code not in (200, 201, 202):
        return {"status": "Failed", "http": r.status_code, "body": r.text}
    op = r.headers.get("x-ms-operation-id")
    if not op:
        return {"status": "Succeeded", "id": r.json().get("id")}
    for _ in range(max_polls):
        s = requests.get(f"{FABRIC}/v1/operations/{op}", headers=headers, timeout=30).json()
        if s.get("status") in ("Succeeded", "Failed"):
            return s
        time.sleep(poll_seconds)
    return {"status": "Timeout", "operation": op}


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
        env = fabric_iq_definition(h, a.name or "SAP Mirror", a.workspace, a.lakehouse)
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

    p = sub.add_parser("deploy", help="POST the definition to a Fabric workspace (notebookutils in Fabric, az locally)")
    p.add_argument("definition"); p.add_argument("--workspace", required=True)
    p.set_defaults(fn=cmd_deploy)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
