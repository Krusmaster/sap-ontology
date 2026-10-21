import base64, json
from pathlib import Path
import pytest
from sap_ontology import build_graph, fabric_graphql_definition, graphql_name, infer_edges, load_dd
from sap_ontology.mirror import from_dd03nd
import pandas as pd

DD = Path(__file__).resolve().parents[1] / "examples" / "dd"
MIRROR = Path(__file__).resolve().parents[1] / "examples" / "mirror"


def _parts(env):
    return {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in env["definition"]["parts"]}


def test_graphql_name():
    assert graphql_name("I_SALESDOCUMENT") == "I_SALESDOCUMENT"
    assert graphql_name("1ABC") == "_1ABC" and graphql_name("A-B C") == "A_B_C"


def test_definition_shape_from_classic_dd():
    g = build_graph(load_dd(DD))
    env = fabric_graphql_definition(g, "SAP_GraphQL", "ws", "sqlep")
    assert env["type"] == "GraphQLApi" and env["definition"]["format"] == "GraphQLApiV1"
    parts = _parts(env)
    d = parts["graphql-definition.json"]
    assert d["$schema"].endswith("/graphqlApi/definition/1.0.0/schema.json")
    src = d["datasources"][0]
    assert src["sourceType"] == "SqlAnalyticsEndpoint" and src["sourceItemId"] == "sqlep" and src["sourceWorkspaceId"] == "ws"
    objs = {o["graphqlType"]: o for o in src["objects"]}
    assert set(objs) == set(g.nodes)
    vbap = objs["VBAP"]
    assert vbap["sourceObject"] == "dbo.VBAP" and vbap["sourceObjectType"] == "Table"
    assert vbap["actions"] == {"Query": "Enabled", "Query_by_pk": "Enabled"}       # lakehouse: read-only
    assert "MANDT" not in vbap["fieldMappings"] and "VBELN" in vbap["fieldMappings"]
    rel = next(r for r in vbap["relationships"] if r["targetObject"] == "VBAK")
    assert rel == {"field": "vbak", "cardinality": "ManyToOne", "targetObject": "VBAK", "sourceFields": ["VBELN"], "targetFields": ["VBELN"]}
    back = next(r for r in objs["VBAK"]["relationships"] if r["targetObject"] == "VBAP")
    assert back["cardinality"] == "OneToMany" and back["field"] == "vbaps"
    assert parts[".platform"]["metadata"]["type"] == "GraphQLApi"


def test_writable_only_for_sql_stores():
    g = build_graph(load_dd(DD))
    ro = _parts(fabric_graphql_definition(g, "t", "ws", "x", writable=True))["graphql-definition.json"]
    assert "Create" not in ro["datasources"][0]["objects"][0]["actions"]                      # SqlAnalyticsEndpoint stays read-only
    rw = _parts(fabric_graphql_definition(g, "t", "ws", "x", source_type="Warehouse", writable=True))["graphql-definition.json"]
    assert rw["datasources"][0]["objects"][0]["actions"]["Create"] == "Enabled"


def test_mirror_graph_text_views_and_multipart_mapping():
    dd = pd.read_csv(MIRROR / "DD03ND.csv", dtype=str)
    g, _ = from_dd03nd(dd, ["I_SALESDOCUMENT", "I_SALESDOCUMENTITEM", "I_PRODUCT", "I_PURCHASEORDER", "I_PURCHASEORDERITEM", "I_SUPPLIER"])
    h, _ = infer_edges(g, confirmed=[{"from": "I_SALESDOCUMENTITEM", "to": "I_PRODUCT", "field": "MATERIAL", "action": "add"}])
    env = fabric_graphql_definition(h, "Mirror_GraphQL", "ws", "sqlep")
    objs = {o["graphqlType"]: o for o in _parts(env)["graphql-definition.json"]["datasources"][0]["objects"]}
    # role-named mapping carried through: MATERIAL -> PRODUCT
    rel = next(r for r in objs["I_SALESDOCUMENTITEM"]["relationships"] if r["targetObject"] == "I_PRODUCT")
    assert rel["sourceFields"] == ["MATERIAL"] and rel["targetFields"] == ["PRODUCT"]
    # header/item pairs become header <-> items relationships for apps
    hdr = next(r for r in objs["I_SALESDOCUMENTITEM"]["relationships"] if r["targetObject"] == "I_SALESDOCUMENT")
    assert hdr == {"field": "header", "cardinality": "ManyToOne", "targetObject": "I_SALESDOCUMENT",
                   "sourceFields": ["SALESDOCUMENT"], "targetFields": ["SALESDOCUMENT"]}
    items = next(r for r in objs["I_SALESDOCUMENT"]["relationships"] if r["targetObject"] == "I_SALESDOCUMENTITEM")
    assert items["cardinality"] == "OneToMany" and items["field"] == "items"


def test_keyless_skipped_and_name_rule():
    g = build_graph(load_dd(DD))
    g.add_node("Z_NOKEY", keys=[], fields=[{"FIELDNAME": "A"}], text="")
    env = fabric_graphql_definition(g, "t", "ws", "x")
    assert env["skipped"] == ["Z_NOKEY"]
    with pytest.raises(ValueError):
        fabric_graphql_definition(g, "SAP GraphQL", "ws", "x")
