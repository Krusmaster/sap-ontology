import base64, json
from pathlib import Path

from sap_ontology import build_graph, fabric_iq_definition, load_dd, mermaid, suggest_business_objects

DD = Path(__file__).resolve().parents[1] / "examples" / "dd"


def test_graph_has_tables_and_fks():
    g = build_graph(load_dd(DD))
    assert set(g.nodes) == {"VBAK", "VBAP", "VBEP", "KNA1", "MARA", "EKKO", "EKPO", "LFA1"}  # INTTAB excluded
    assert g.has_edge("VBAP", "VBAK") and g.has_edge("EKPO", "MARA")
    assert g.nodes["VBAP"]["keys"] == ["VBELN", "POSNR"]  # MANDT dropped


def test_business_object_suggestions():
    s = {x["object"]: x for x in suggest_business_objects(build_graph(load_dd(DD)))}
    assert s["VBAK"]["items"] == ["VBAP"] and s["VBAK"]["join_keys"] == ["VBELN"]
    assert s["VBAP"]["items"] == ["VBEP"]
    assert s["EKKO"]["items"] == ["EKPO"]
    assert "KNA1" not in s


def test_table_filter():
    g = build_graph(load_dd(DD), tables=["VBAK", "VBAP"])
    assert set(g.nodes) == {"VBAK", "VBAP"}


def test_mermaid_renders():
    m = mermaid(build_graph(load_dd(DD)))
    assert m.startswith("erDiagram") and "VBAP }o--|| VBAK" in m


def test_fabric_iq_envelope():
    g = build_graph(load_dd(DD))
    env = fabric_iq_definition(g, "SAP test", "ws-1", "lh-1")
    assert env["type"] == "Ontology"
    paths = [p["path"] for p in env["definition"]["parts"]]
    assert ".platform" in paths and "definition.json" in paths
    assert sum(p.startswith("EntityTypes/") and p.endswith("definition.json") for p in paths) == 8
    assert sum(p.startswith("RelationshipTypes/") and p.endswith("definition.json") for p in paths) == 7
    assert sum("/Contextualizations/" in p for p in paths) == 7
    parts = {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in env["definition"]["parts"]}
    ent = next(o for k, o in parts.items() if k.startswith("EntityTypes/") and k.endswith("definition.json") and o["name"] == "VBAP")
    names = {p["name"]: p["valueType"] for p in ent["properties"]}
    assert names["KWMENG"] == "Double" and names["VBELN"] == "String"
    assert len(ent["entityIdParts"]) == 2 and all(i > 0 for i in ent["entityIdParts"])
    assert ent["namespace"] == "usertypes" and ent["namespaceType"] == "Custom" and ent["visibility"] == "Visible" and ent["baseEntityTypeId"] is None
    binding = next(o for k, o in parts.items() if k.startswith(f"EntityTypes/{ent['id']}/DataBindings/"))
    cfg = binding["dataBindingConfiguration"]
    assert cfg["dataBindingType"] == "NonTimeSeries" and cfg["sourceTableProperties"]["sourceType"] == "LakehouseTable"
    assert cfg["sourceTableProperties"]["sourceTableName"] == "VBAP"          # real name, not lower-cased
    assert {b["sourceColumnName"] for b in cfg["propertyBindings"]} == set(names)
    ctx = next(o for k, o in parts.items() if "/Contextualizations/" in k and o["dataBindingTable"]["sourceTableName"] == "VBAP")
    assert ctx["sourceKeyRefBindings"][0]["sourceColumnName"] == "VBELN"
    assert len(ctx["targetKeyRefBindings"]) == 1


def test_ids_are_deterministic():
    g = build_graph(load_dd(DD))
    a = fabric_iq_definition(g, "x", "ws", "lh")["definition"]["parts"]
    b = fabric_iq_definition(g, "x", "ws", "lh")["definition"]["parts"]
    assert [p["path"] for p in a] == [p["path"] for p in b]


def test_graph_db_exports():
    from sap_ontology import graphml, kql_graph, neo4j_csv
    g = build_graph(load_dd(DD))
    x = graphml(g)
    assert "<graphml" in x and 'VBAP' in x
    nodes, rels = neo4j_csv(g)
    assert nodes.splitlines()[0] == "name:ID,text,keys,n_fields:int,:LABEL" and "VBAP,VBAK," in rels
    k = kql_graph(g)
    assert "make-graph src --> dst" in k and '"name":"VBAK"' in k
