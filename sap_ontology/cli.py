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


def _token() -> str:
    r = subprocess.run(["az", "account", "get-access-token", "--resource", FABRIC, "--query", "accessToken", "-o", "tsv"],
                       capture_output=True, text=True, check=True)
    return r.stdout.strip()


def cmd_deploy(a: argparse.Namespace) -> int:
    import requests  # optional dependency, only for deploy

    env = json.loads(Path(a.definition).read_text(encoding="utf-8"))
    headers = {"Authorization": f"Bearer {_token()}", "Content-Type": "application/json"}
    r = requests.post(f"{FABRIC}/v1/workspaces/{a.workspace}/items", headers=headers, json=env, timeout=60)
    if r.status_code not in (200, 201, 202):
        print(r.status_code, r.text, file=sys.stderr)
        return 1
    op = r.headers.get("x-ms-operation-id")
    if not op:
        print("created", r.json().get("id"))
        return 0
    for _ in range(60):
        s = requests.get(f"{FABRIC}/v1/operations/{op}", headers=headers, timeout=30).json()
        if s.get("status") in ("Succeeded", "Failed"):
            print(s.get("status"), json.dumps(s.get("error", ""), indent=2) if s.get("error") else "")
            return 0 if s["status"] == "Succeeded" else 1
        time.sleep(3)
    print("timed out waiting for Fabric", file=sys.stderr)
    return 1


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

    p = sub.add_parser("deploy", help="POST the definition to a Fabric workspace (az login first)")
    p.add_argument("definition"); p.add_argument("--workspace", required=True)
    p.set_defaults(fn=cmd_deploy)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
