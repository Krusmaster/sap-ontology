from pathlib import Path
import pandas as pd
import pytest
import yaml

from sap_ontology import from_dd03nd, infer_edges, match_tables, normalise_dd03nd, suggest_business_objects
from sap_ontology.export import fabric_iq_definition, value_type

DD = Path(__file__).resolve().parents[1] / "examples" / "mirror"
LAKEHOUSE = ["I_SALESDOCUMENT", "I_SALESDOCUMENTITEM", "I_CUSTOMER", "I_PRODUCT",
             "I_PURCHASEORDER_P01", "I_PURCHASEORDERITEM", "I_SUPPLIER", "ZC_SALESKPI", "I_NOTINDD"]
SPARK = {"ZC_SALESKPI": {"SalesDocument": "string", "KpiValue": "decimal(15,2)", "__sequence_number": "decimal(38,0)", "__timestamp": "timestamp"}}


def dd():
    return pd.read_csv(DD / "DD03ND.csv", dtype=str)


def test_normalise_keeps_active_and_intifies():
    d = normalise_dd03nd(dd())
    assert "I_OLDVIEW" not in set(d["STRUCOBJN"])              # as4local D dropped
    row = d[(d.STRUCOBJN == "I_SALESDOCUMENTITEM") & (d.FIELDNAME == "SALESDOCUMENTITEM")].iloc[0]
    assert row.LENG == 6 and row.DECIMALS == 0 and row.KEYFLAG is True or row.KEYFLAG == True


def test_match_report_both_directions():
    rep = match_tables(LAKEHOUSE, normalise_dd03nd(dd()), strip_suffix=True)
    assert "I_SALESDOCUMENT" in rep.matched
    assert rep.via_suffix == {"I_PURCHASEORDER_P01": "I_PURCHASEORDER"}
    assert set(rep.unmatched_tables) == {"ZC_SALESKPI", "I_NOTINDD"}
    assert rep.custom_views == ["ZC_SALESKPI"]
    assert rep.multi_node_views == ["I_TWONODES"]
    assert "2 unmatched (1 custom" in rep.summary()


def test_suffix_rule_is_opt_in():
    rep = match_tables(LAKEHOUSE, normalise_dd03nd(dd()), strip_suffix=False)
    assert "I_PURCHASEORDER_P01" in rep.unmatched_tables


def test_name_map():
    rep = match_tables(["SOMETHING_ELSE"], normalise_dd03nd(dd()), name_map={"SOMETHING_ELSE": "I_SUPPLIER"})
    assert rep.via_map == {"SOMETHING_ELSE": "I_SUPPLIER"}


def test_from_dd03nd_nodes_keys_types_and_custom():
    g, rep = from_dd03nd(dd(), LAKEHOUSE, spark_schemas=SPARK, custom_keys={"ZC_SALESKPI": ["SalesDocument"]}, strip_suffix=True)
    assert g.nodes["I_SALESDOCUMENTITEM"]["keys"] == ["SALESDOCUMENT", "SALESDOCUMENTITEM"]
    assert g.nodes["I_PURCHASEORDER_P01"]["view"] == "I_PURCHASEORDER"
    types = {f["FIELDNAME"]: f["DATATYPE"] for f in g.nodes["I_SALESDOCUMENT"]["fields"]}
    assert value_type(types["CREATIONDATE"]) == "DateTime" and value_type(types["TOTALNETAMOUNT"]) == "Double"
    assert value_type(dict((f["FIELDNAME"], f["DATATYPE"]) for f in g.nodes["I_SALESDOCUMENTITEM"]["fields"])["SALESDOCUMENTITEM"]) == "String"
    z = g.nodes["ZC_SALESKPI"]
    assert z["source"] == "spark" and z["keys"] == ["SALESDOCUMENT"]
    assert all(f["FIELDNAME"] not in ("__SEQUENCE_NUMBER", "__TIMESTAMP") for f in z["fields"])
    assert "I_NOTINDD" not in g.nodes                             # unknown and no schema -> reported, not guessed
    assert g.number_of_edges() == 0                                # no DD08L: no edges until inferred


def test_header_item_still_works_without_edges():
    g, _ = from_dd03nd(dd(), LAKEHOUSE, strip_suffix=True)
    s = {x["object"]: x for x in suggest_business_objects(g)}
    assert s["I_SALESDOCUMENT"]["items"] == ["I_SALESDOCUMENTITEM"]
    assert s["I_PURCHASEORDER_P01"]["items"] == ["I_PURCHASEORDERITEM"]


def test_infer_edges_single_key_and_ambiguity():
    g, _ = from_dd03nd(dd(), LAKEHOUSE, spark_schemas=SPARK, custom_keys={"ZC_SALESKPI": ["SalesDocument"]}, strip_suffix=True)
    h, amb = infer_edges(g)
    # Name-based inference: SOLDTOPARTY is not called CUSTOMER, so no edge without YAML. Honest.
    assert not h.has_edge("I_SALESDOCUMENT", "I_CUSTOMER")
    # SUPPLIER is the single-column key of I_SUPPLIER and a column of I_PURCHASEORDER_P01 -> inferred
    assert h.has_edge("I_PURCHASEORDER_P01", "I_SUPPLIER") and h.edges["I_PURCHASEORDER_P01", "I_SUPPLIER"]["inferred"]
    # ZC_SALESKPI carries SALESDOCUMENT = key of I_SALESDOCUMENT -> inferred (its own key is the same, prefix rule applies)
    assert not h.has_edge("ZC_SALESKPI", "I_SALESDOCUMENT")        # prefix rule: treated as header/item, not reference
    # header/item pairs must NOT become reference edges
    assert not h.has_edge("I_SALESDOCUMENTITEM", "I_SALESDOCUMENT")
    # ZC_SALESKPI shares I_SALESDOCUMENT's single-column key -> reported as ambiguous, never guessed
    assert amb == [{"column": "SALESDOCUMENT", "owners": ["I_SALESDOCUMENT", "ZC_SALESKPI"],
                    "reason": "single-column key of several tables"}]


def test_infer_edges_reports_ambiguous_key():
    g, _ = from_dd03nd(dd(), ["I_CUSTOMER", "I_SUPPLIER", "I_PURCHASEORDER"], strip_suffix=False)
    # make SUPPLIER ambiguous: pretend I_CUSTOMER's key is also SUPPLIER
    g.nodes["I_CUSTOMER"]["keys"] = ["SUPPLIER"]
    h, amb = infer_edges(g)
    assert amb and amb[0]["column"] == "SUPPLIER" and set(amb[0]["owners"]) == {"I_CUSTOMER", "I_SUPPLIER"}
    assert not h.has_edge("I_PURCHASEORDER", "I_SUPPLIER")


def test_relationships_yaml_add_and_drop():
    g, _ = from_dd03nd(dd(), LAKEHOUSE, strip_suffix=True)
    conf = yaml.safe_load((DD / "relationships.yaml").read_text())
    conf.append({"from": "I_PURCHASEORDER_P01", "to": "I_SUPPLIER", "action": "drop"})
    h, _ = infer_edges(g, confirmed=conf)
    assert h.has_edge("I_SALESDOCUMENTITEM", "I_PRODUCT") and h.edges["I_SALESDOCUMENTITEM", "I_PRODUCT"]["kind"] == "ADD"
    assert not h.has_edge("I_PURCHASEORDER_P01", "I_SUPPLIER")


def test_ontology_from_mirror_graph():
    g, _ = from_dd03nd(dd(), LAKEHOUSE, strip_suffix=True)
    h, _ = infer_edges(g, confirmed=yaml.safe_load((DD / "relationships.yaml").read_text()))
    env = fabric_iq_definition(h, "mirror_test", "ws", "lh")
    paths = [p["path"] for p in env["definition"]["parts"]]
    assert sum(p.startswith("RelationshipTypes/") and p.endswith("definition.json") for p in paths) == h.number_of_edges() >= 3
    assert sum("/Contextualizations/" in p for p in paths) == h.number_of_edges()


def _g(nodes):
    """nodes: {table: (keys, extra_cols)} -> graph with fields."""
    import networkx as nx
    g = nx.DiGraph()
    for t, (keys, extra) in nodes.items():
        fields = [{"FIELDNAME": c, "POSITION": i, "KEYFLAG": "X" if c in keys else "", "DATATYPE": "CHAR", "LENG": 10, "DECIMALS": 0}
                  for i, c in enumerate(list(keys) + list(extra), 1)]
        g.add_node(t, text="", keys=list(keys), fields=fields)
    return g


def test_reference_in_composite_key_keeps_edge_and_is_not_an_item():
    g = _g({
        "I_PLANT": (["PLANT"], ["PLANTNAME"]),
        "I_STORAGELOCATION": (["PLANT", "STORAGELOCATION"], []),
        "I_PRODUCT": (["PRODUCT"], []),
        "ZC_STOCK": (["PLANT", "STORAGELOCATION", "PRODUCT"], ["QUANTITY"]),
    })
    h, amb = infer_edges(g)
    assert h.has_edge("ZC_STOCK", "I_PLANT")              # PLANT is a reference even though it leads ZC_STOCK's key
    assert h.has_edge("ZC_STOCK", "I_PRODUCT")            # PRODUCT too — it is another table's key, not an item number
    assert not h.has_edge("I_STORAGELOCATION", "I_PLANT")  # that one IS header/item -> business object, not a reference
    assert amb == []
    s = {x["object"]: x for x in suggest_business_objects(h)}
    assert "ZC_STOCK" not in s.get("I_STORAGELOCATION", {}).get("items", [])   # PRODUCT is a reference, so not an item
    assert s["I_PLANT"]["items"] == ["I_STORAGELOCATION"]                     # STORAGELOCATION is a real item number


def test_multi_column_key_ambiguity_reported():
    g = _g({"A": (["K1", "K2"], []), "B": (["K1", "K2"], ["X"]), "C": ([], ["K1", "K2", "Y"])})
    h, amb = infer_edges(g)
    assert amb and amb[0]["column"] == "K1,K2" and set(amb[0]["owners"]) == {"A", "B"}
    assert h.number_of_edges() == 0


def test_multi_column_keys_can_be_switched_off():
    g = _g({"A": (["K1", "K2"], []), "C": (["Z"], ["K1", "K2"])})
    h, _ = infer_edges(g, include_multi_column_keys=False)
    assert h.number_of_edges() == 0
    h, _ = infer_edges(g)
    assert h.has_edge("C", "A")


def test_text_view_direction_and_language_noise():
    g = _g({
        "I_LANGUAGE": (["LANGUAGE"], ["LANGUAGENAME"]),
        "I_COSTCENTER": (["COSTCENTER"], ["LANGUAGE", "VALIDTO"]),          # master view that happens to carry LANGUAGE
        "I_COSTCENTERTEXT": (["COSTCENTER", "LANGUAGE"], ["COSTCENTERNAME"]),
        "I_PRODUCT": (["PRODUCT"], ["LANGUAGE"]),
    })
    h, amb = infer_edges(g)
    assert h.has_edge("I_COSTCENTERTEXT", "I_COSTCENTER") and h.edges["I_COSTCENTERTEXT", "I_COSTCENTER"]["kind"] == "TEXT_OF"
    assert not h.has_edge("I_COSTCENTER", "I_COSTCENTERTEXT")                # never the false direction
    assert not h.has_edge("I_COSTCENTER", "I_LANGUAGE") and not h.has_edge("I_PRODUCT", "I_LANGUAGE")  # noise dropped
    assert amb == []
    h2, _ = infer_edges(g, include_text_key_edges=True)
    assert h2.has_edge("I_COSTCENTER", "I_LANGUAGE")                        # opt-in keeps them
    s = {x["object"]: x for x in suggest_business_objects(h)}
    assert "I_COSTCENTERTEXT" not in s.get("I_COSTCENTER", {}).get("items", [])  # text view is not an item


def test_text_views_real_sap_key_orders():
    g = _g({
        "I_LANGUAGE": (["LANGUAGE"], []),
        "I_COSTCENTER": (["CONTROLLINGAREA", "COSTCENTER", "VALIDITYENDDATE"], ["LANGUAGE", "BUSINESSAREA", "PROFITCENTER"]),
        "I_COSTCENTERTEXT": (["COSTCENTER", "CONTROLLINGAREA", "LANGUAGE", "VALIDITYENDDATE"], ["COSTCENTERNAME"]),   # text key in the middle, different order
        "I_PROFITCENTER": (["CONTROLLINGAREA", "PROFITCENTER", "VALIDITYENDDATE"], ["LANGUAGE"]),
        "I_PROFITCENTERTEXT": (["LANGUAGE", "CONTROLLINGAREA", "PROFITCENTER", "VALIDITYENDDATE"], []),            # text key first
        "I_BUSINESSAREA": (["BUSINESSAREA"], []),
        "I_BUSINESSAREATEXT": (["BUSINESSAREA", "LANGUAGE"], []),
        "I_PLANT": (["PLANT"], ["LANGUAGE", "SALESORGANIZATION", "DIVISION"]),
        "I_DIVISION": (["DIVISION"], []),
        "I_DIVISIONTEXT": (["DIVISION", "LANGUAGE"], []),
        "I_SALESORGANIZATION": (["SALESORGANIZATION"], []),
        "I_SALESORGANIZATIONTEXT": (["SALESORGANIZATION", "LANGUAGE"], []),
    })
    h, amb = infer_edges(g)
    kinds = {(a, b): d["kind"] for a, b, d in h.edges(data=True)}
    # (1) text views found regardless of key order / position of LANGUAGE
    assert kinds[("I_COSTCENTERTEXT", "I_COSTCENTER")] == "TEXT_OF"
    assert kinds[("I_PROFITCENTERTEXT", "I_PROFITCENTER")] == "TEXT_OF"
    assert kinds[("I_BUSINESSAREATEXT", "I_BUSINESSAREA")] == "TEXT_OF"
    # no reverse edges
    assert ("I_COSTCENTER", "I_COSTCENTERTEXT") not in kinds and ("I_PROFITCENTER", "I_PROFITCENTERTEXT") not in kinds
    # (2) masters carrying LANGUAGE as a non-key column never point at other entities' text views
    for src in ("I_PLANT", "I_COSTCENTER", "I_PROFITCENTER"):
        assert not any(b.endswith("TEXT") for a, b in kinds if a == src), f"{src} points at a text view"
    # but their real references to masters remain
    assert kinds[("I_PLANT", "I_DIVISION")] == "INFERRED" and kinds[("I_PLANT", "I_SALESORGANIZATION")] == "INFERRED"
    assert kinds[("I_COSTCENTER", "I_BUSINESSAREA")] == "INFERRED"
    # (3) multi-column key compared as a set: I_COSTCENTER -> I_PROFITCENTER (carries CONTROLLINGAREA, PROFITCENTER, VALIDITYENDDATE)
    assert kinds[("I_COSTCENTER", "I_PROFITCENTER")] == "INFERRED"
    assert amb == []
    # (4) no text view is ever proposed as an item
    s = {x["object"]: x for x in suggest_business_objects(h)}
    assert "I_LANGUAGETEXT" not in s.get("I_LANGUAGE", {}).get("items", [])
    assert all(not i.endswith("TEXT") for x in s.values() for i in x["items"])


def test_keyless_entity_is_skipped_and_catalog_name_used():
    g = _g({"I_PRODUCT": (["PRODUCT"], ["PRODUCTTYPE"]), "Z_CUSTOM_VIEW": ([], ["A", "B"])})
    g.nodes["I_PRODUCT"]["catalog_name"] = "i_product"          # Spark-written lakehouse lists lower case
    g.add_edge("Z_CUSTOM_VIEW", "I_PRODUCT", field="PRODUCT", kind="INFERRED")
    env = fabric_iq_definition(g, "t", "ws", "lh")
    assert env["skipped"] == ["Z_CUSTOM_VIEW"]
    import base64, json
    parts = {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in env["definition"]["parts"]}
    assert sum(k.startswith("EntityTypes/") and k.endswith("definition.json") for k in parts) == 1
    assert not any(k.startswith("RelationshipTypes/") for k in parts)        # edge to a skipped entity dropped
    b = next(v for k, v in parts.items() if "/DataBindings/" in k)
    assert b["dataBindingConfiguration"]["sourceTableProperties"]["sourceTableName"] == "i_product"


def test_text_views_never_source_or_target_even_without_master():
    g = _g({
        "I_CONTROLLINGAREA": (["CONTROLLINGAREA"], []),
        "I_COSTCENTER": (["CONTROLLINGAREA", "COSTCENTER", "VALIDITYENDDATE"], []),
        "I_COSTCENTERTEXT": (["COSTCENTER", "CONTROLLINGAREA", "LANGUAGE", "VALIDITYENDDATE"], []),
        "I_PRODUCTDESCRIPTION": (["PRODUCT", "LANGUAGE"], []),          # master I_PRODUCT not replicated
        "I_SALESDOCUMENTITEM": (["SALESDOCUMENT", "SALESDOCUMENTITEM"], ["PRODUCT"]),
    })
    h, amb = infer_edges(g)
    edges = {(a, b) for a, b in h.edges}
    assert ("I_COSTCENTERTEXT", "I_COSTCENTER") in edges                   # TEXT_OF stays
    assert ("I_COSTCENTERTEXT", "I_CONTROLLINGAREA") not in edges           # text view is never a reference source
    assert ("I_COSTCENTER", "I_CONTROLLINGAREA") in edges                   # the master keeps it
    assert not any(b == "I_PRODUCTDESCRIPTION" for _, b in edges)          # never a target, even with no master
    assert not any(a == "I_PRODUCTDESCRIPTION" for a, _ in edges)
    assert amb == []


def test_standard_relationships_file_loads_and_ignores_absent_tables():
    from sap_ontology import standard_relationships
    conf = standard_relationships()
    assert len(conf) >= 40 and all({"from", "to", "field", "action"} <= set(c) for c in conf)
    g, _ = from_dd03nd(dd(), LAKEHOUSE, strip_suffix=True)
    before = set(g.nodes)
    h, _ = infer_edges(g, confirmed=conf)
    assert set(h.nodes) == before                              # no nodes invented for absent tables
    assert h.has_edge("I_SALESDOCUMENTITEM", "I_PRODUCT")      # present pair gets its edge
    assert not h.has_node("I_COMPANYCODE")


def test_confirmed_entry_fills_multi_part_target_key_or_is_reported():
    from sap_ontology import resolve_mapping
    g = _g({
        "I_PLANT": (["PLANT"], []),
        "I_STORAGELOCATION": (["PLANT", "STORAGELOCATION"], []),
        "I_SALESDOCUMENTITEM": (["SALESDOCUMENT", "SALESDOCUMENTITEM"], ["PLANT", "STORAGELOCATION", "PRODUCT"]),
        "I_PRODUCT": (["PRODUCT"], []),
        "ZC_NOPLANT": (["X"], ["STORAGELOCATION"]),
    })
    conf = [
        {"from": "I_SALESDOCUMENTITEM", "to": "I_STORAGELOCATION", "field": "STORAGELOCATION", "action": "add"},  # PLANT filled by name
        {"from": "I_SALESDOCUMENTITEM", "to": "I_PRODUCT", "field": "MATERIAL", "action": "add"},               # MATERIAL not a column -> not applied
        {"from": "ZC_NOPLANT", "to": "I_STORAGELOCATION", "field": "STORAGELOCATION", "action": "add"},         # PLANT missing -> not applied
    ]
    h, _ = infer_edges(g, confirmed=conf)
    assert h.edges["I_SALESDOCUMENTITEM", "I_STORAGELOCATION"]["mapping"] == [("PLANT", "PLANT"), ("STORAGELOCATION", "STORAGELOCATION")]
    assert h.edges["I_SALESDOCUMENTITEM", "I_PRODUCT"]["kind"] == "INFERRED"   # found via PRODUCT; the MATERIAL entry was not applied
    assert not h.has_edge("ZC_NOPLANT", "I_STORAGELOCATION")
    reasons = {(x["from"], x["to"]): x["reason"] for x in h.graph["not_applied"]}
    assert "MATERIAL" in reasons[("I_SALESDOCUMENTITEM", "I_PRODUCT")]
    assert "PLANT" in reasons[("ZC_NOPLANT", "I_STORAGELOCATION")]
    # role-named single column onto a single-part key, positional
    assert resolve_mapping("SOLDTOPARTY", {"SOLDTOPARTY", "X"}, ["CUSTOMER"]) == ([("SOLDTOPARTY", "CUSTOMER")], None)
    # every contextualization in the definition covers the full target key
    import base64, json
    env = fabric_iq_definition(h, "t", "ws", "lh")
    parts = {p["path"]: json.loads(base64.b64decode(p["payload"])) for p in env["definition"]["parts"]}
    ents = {o["id"]: o for k, o in parts.items() if k.startswith("EntityTypes/") and k.endswith("definition.json")}
    rels = {k.split("/")[1]: o for k, o in parts.items() if k.startswith("RelationshipTypes/") and k.endswith("definition.json")}
    for k, ctx in parts.items():
        if "/Contextualizations/" in k:
            rel = rels[k.split("/")[1]]
            assert len(ctx["targetKeyRefBindings"]) == len(ents[rel["target"]["entityTypeId"]]["entityIdParts"])


def test_export_refuses_bad_contextualization():
    import pytest
    g = _g({"A": (["K1", "K2"], []), "B": (["Z"], ["K1"])})
    g.add_edge("B", "A", field="K1", kind="INFERRED")               # B lacks K2: cannot cover A's key
    with pytest.raises(ValueError, match="cannot map target key part K2"):
        fabric_iq_definition(g, "t", "ws", "lh")
