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
    env = fabric_iq_definition(h, "mirror test", "ws", "lh")
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
