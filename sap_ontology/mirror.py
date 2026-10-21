"""Adapter for Fabric SAP Mirror lakehouses: DD03ND + mirrored tables -> graph.

An SAP Mirror (S/4HANA CDS views -> Datasphere replication flow -> Fabric mirrored database)
does not carry DD02L/DD03L/DD08L. It carries:

- the replicated CDS views as Delta tables (upper-case names), with technical columns
  `__sequence_number` and `__timestamp`, and
- one reference dictionary table, DD03ND, describing CDS views' fields, keys and types:
  strucobjn, nodename, fieldname, as4local, position, keyflag, datatype, leng, decimals.

DD03ND is a reference list: it describes ~100k views, most not replicated; mirrored table names
may carry a system suffix; custom Z/Y views are not in it at all. There are no foreign keys and
no texts. This module builds the same graph input as `dd.build_graph` from that reality, and
`infer_edges` derives relationships from key structure since DD08L is absent.

Everything here is pure pandas. `from_lakehouse` is the only Spark entry point and it filters
DD03ND to the lakehouse's tables *before* `toPandas()`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import networkx as nx
import pandas as pd

TECH_COLS = {"__SEQUENCE_NUMBER", "__TIMESTAMP", "__ROWMARKER__", "MANDT", "CLIENT"}
SUFFIX_RE = re.compile(r"_[A-Z][0-9]{2}$")          # opt-in: strip _<SID> like _P01

_DD_TYPE = {  # DD datatype -> simple type used by export.value_type
    "INT1": "INT1", "INT2": "INT2", "INT4": "INT4", "INT8": "INT8",
    "DEC": "DEC", "CURR": "CURR", "QUAN": "QUAN", "FLTP": "FLTP",
    "DATS": "DATS", "TIMS": "TIMS",
}


@dataclass
class MatchReport:
    matched: dict[str, str] = field(default_factory=dict)        # table -> strucobjn
    via_suffix: dict[str, str] = field(default_factory=dict)     # table -> strucobjn (suffix stripped)
    via_map: dict[str, str] = field(default_factory=dict)        # table -> strucobjn (from YAML)
    unmatched_tables: list[str] = field(default_factory=list)    # in lakehouse, not in DD03ND
    custom_views: list[str] = field(default_factory=list)        # unmatched AND starts with Z/Y
    multi_node_views: list[str] = field(default_factory=list)    # DD03ND views with >1 nodename

    def summary(self) -> str:
        return (f"{len(self.matched)} exact, {len(self.via_suffix)} via suffix, {len(self.via_map)} via map, "
                f"{len(self.unmatched_tables)} unmatched ({len(self.custom_views)} custom Z/Y)")


def normalise_dd03nd(df: pd.DataFrame) -> pd.DataFrame:
    """Upper-case column names, keep active rows, int-ify leng/decimals/position, warn on multi-node."""
    d = df.copy()
    d.columns = [c.upper() for c in d.columns]
    need = {"STRUCOBJN", "FIELDNAME", "POSITION", "KEYFLAG", "DATATYPE"}
    missing = need - set(d.columns)
    if missing:
        raise ValueError(f"DD03ND is missing columns {sorted(missing)}")
    if "AS4LOCAL" in d.columns:
        d = d[d["AS4LOCAL"].astype(str).str.upper() == "A"]
    for c in ("POSITION", "LENG", "DECIMALS"):
        if c in d.columns:
            d[c] = pd.to_numeric(d[c].astype(str).str.lstrip("0").replace("", "0"), errors="coerce").fillna(0).astype(int)
    d["STRUCOBJN"] = d["STRUCOBJN"].astype(str).str.upper()
    d["FIELDNAME"] = d["FIELDNAME"].astype(str).str.upper()
    d["KEYFLAG"] = d["KEYFLAG"].astype(str).str.upper().eq("X")
    return d


def match_tables(
    lakehouse_tables: list[str],
    dd03nd: pd.DataFrame,
    strip_suffix: bool = False,
    name_map: dict[str, str] | None = None,
) -> MatchReport:
    """Exact match first, then optional _<SID> suffix rule, then explicit name map."""
    views = set(dd03nd["STRUCOBJN"].unique())
    name_map = {k.upper(): v.upper() for k, v in (name_map or {}).items()}
    rep = MatchReport()
    if "NODENAME" in dd03nd.columns:
        n = dd03nd.groupby("STRUCOBJN")["NODENAME"].nunique()
        rep.multi_node_views = sorted(n[n > 1].index.tolist())
    for t in sorted({t.upper() for t in lakehouse_tables}):
        if t in views:
            rep.matched[t] = t
        elif strip_suffix and SUFFIX_RE.search(t) and SUFFIX_RE.sub("", t) in views:
            rep.via_suffix[t] = SUFFIX_RE.sub("", t)
        elif t in name_map and name_map[t] in views:
            rep.via_map[t] = name_map[t]
        else:
            rep.unmatched_tables.append(t)
            if t[:1] in ("Z", "Y"):
                rep.custom_views.append(t)
    return rep


def from_dd03nd(
    dd03nd: pd.DataFrame,
    lakehouse_tables: list[str],
    spark_schemas: dict[str, dict[str, str]] | None = None,
    custom_keys: dict[str, list[str]] | None = None,
    strip_suffix: bool = False,
    name_map: dict[str, str] | None = None,
) -> tuple[nx.DiGraph, MatchReport]:
    """Build the node graph (no edges yet) for the tables that exist in the lakehouse.

    spark_schemas: {table: {column: spark_type}} — used for custom views (not in DD03ND) and as a
                   fallback type source. Technical columns are dropped.
    custom_keys:   {table: [key columns]} — keys for custom views come ONLY from here, never guessed.
    """
    dd = normalise_dd03nd(dd03nd)
    rep = match_tables(lakehouse_tables, dd, strip_suffix, name_map)
    raw_columns = {k.upper(): list(v.keys()) for k, v in (spark_schemas or {}).items()}   # as the endpoint has them
    spark_schemas = {k.upper(): {c.upper(): t for c, t in v.items()} for k, v in (spark_schemas or {}).items()}
    custom_keys = {k.upper(): [c.upper() for c in v] for k, v in (custom_keys or {}).items()}

    g = nx.DiGraph()
    for table, view in {**rep.matched, **rep.via_suffix, **rep.via_map}.items():
        rows = dd[dd["STRUCOBJN"] == view].sort_values("POSITION")
        fields = []
        for r in rows.itertuples(index=False):
            if r.FIELDNAME in TECH_COLS:
                continue
            fields.append({
                "FIELDNAME": r.FIELDNAME, "POSITION": int(r.POSITION), "KEYFLAG": "X" if r.KEYFLAG else "",
                "DATATYPE": _DD_TYPE.get(str(r.DATATYPE).upper(), "CHAR"),
                "LENG": int(getattr(r, "LENG", 0) or 0), "DECIMALS": int(getattr(r, "DECIMALS", 0) or 0),
            })
        keys = [f["FIELDNAME"] for f in fields if f["KEYFLAG"] == "X"]
        g.add_node(table, text="", keys=keys, fields=fields, source="dd03nd", view=view, columns=raw_columns.get(table))

    for table in rep.unmatched_tables:
        schema = spark_schemas.get(table)
        if not schema:
            continue                                   # nothing known about it — skip, it is reported
        fields = [{"FIELDNAME": c, "POSITION": i, "KEYFLAG": "X" if c in custom_keys.get(table, []) else "",
                   "DATATYPE": _spark_to_dd(t), "LENG": 0, "DECIMALS": 0}
                  for i, (c, t) in enumerate(schema.items(), 1) if c not in TECH_COLS]
        g.add_node(table, text="", keys=custom_keys.get(table, []), fields=fields, source="spark", view=None,
                   columns=raw_columns.get(table))
    return g, rep


def from_lakehouse(
    spark, schema: str = "dbo", dd03nd_table: str = "DD03ND", custom_keys=None, strip_suffix=False, name_map=None,
) -> tuple[nx.DiGraph, MatchReport]:
    """Fabric entry point. Filters DD03ND to the lakehouse's tables in Spark before toPandas()."""
    from pyspark.sql import functions as F  # type: ignore

    catalog_names = {t.name.upper(): t.name for t in spark.catalog.listTables(schema) if t.name.upper() != dd03nd_table.upper()}
    tables = list(catalog_names)
    candidates = set(tables)
    if strip_suffix:
        candidates |= {SUFFIX_RE.sub("", t) for t in tables}
    if name_map:
        candidates |= {v.upper() for v in name_map.values()}
    dd = (spark.table(f"{schema}.{dd03nd_table}")
          .withColumn("_s", F.upper(F.col("strucobjn")))
          .filter(F.col("_s").isin(list(candidates)))
          .drop("_s", "__sequence_number", "__timestamp")
          .toPandas())
    schemas = {t: {f.name: f.dataType.simpleString() for f in spark.table(f"{schema}.{catalog_names[t]}").schema.fields} for t in tables}
    g, rep = from_dd03nd(dd, tables, spark_schemas=schemas, custom_keys=custom_keys, strip_suffix=strip_suffix, name_map=name_map)
    for t in g.nodes:                                   # bind to the name the catalog lists (Spark writes lower case)
        g.nodes[t]["catalog_name"] = catalog_names.get(t, t)
    return g, rep


TEXT_KEYS = {"LANGUAGE", "SPRAS", "LANGU", "LANGUAGECODE"}


def is_text_view(a_keys: list[str], b_keys: list[str], text_keys: set[str]) -> bool:
    """A is the text view of B when A's key contains a text key and, text keys removed, equals B's key as a set."""
    ka, kb = set(a_keys), set(b_keys)
    return bool(ka & text_keys) and (ka - text_keys) == kb and kb and not (kb & text_keys)


def is_item_of(b_keys: list[str], a_keys: list[str], other_single_keys: set[str], text_keys: set[str] = frozenset()) -> bool:
    """B is an item table of A when B's key = A's key + exactly one column, and that column is neither
    the single-column key of another table (reference) nor a text key (text view)."""
    if len(b_keys) != len(a_keys) + 1 or b_keys[: len(a_keys)] != a_keys:
        return False
    extra = b_keys[-1]
    return extra not in other_single_keys and extra not in text_keys


def infer_edges(
    g: nx.DiGraph,
    confirmed: list[dict] | None = None,
    include_multi_column_keys: bool = True,
    text_keys: set[str] | None = None,
    include_text_key_edges: bool = False,
) -> tuple[nx.DiGraph, list[dict]]:
    """Derive relationships from key structure when no foreign-key catalogue exists.

    Rule: B -> A when B carries ALL of A's key columns (by name, case-insensitive, order ignored).
    Header/item pairs (B's key = A's key + one non-reference, non-text column) are NOT reference
    edges; they are left to suggest_business_objects. A reference column that happens to be part
    of B's key still yields an edge.

    Text views: A is the text view of B when A's key contains a text key (LANGUAGE, SPRAS, ...)
    and, text keys removed, equals B's key as a set — any order, text key anywhere. Edge A -> B
    with kind TEXT_OF. No reference edge ever targets a text view: references point to the master.
    Edges to the language table itself are dropped unless include_text_key_edges=True.

    Multi-column keys are compared as sets. Every inferred edge is marked inferred=True with a
    reason. A key owned by two or more tables is AMBIGUOUS: no edge, reported instead.

    confirmed: list of {"from": B, "to": A, "field": col, "action": "confirm"|"override"|"add"|"drop"}
               from a relationships YAML. Applied after inference; "drop" removes an inferred edge.
    """
    text_keys = {t.upper() for t in (text_keys if text_keys is not None else TEXT_KEYS)}
    keys = {n: [k.upper() for k in d.get("keys", [])] for n, d in g.nodes(data=True)}
    cols = {n: {f["FIELDNAME"].upper() for f in d.get("fields", [])} for n, d in g.nodes(data=True)}
    ambiguous: list[dict] = []

    owners: dict[frozenset, list[str]] = {}
    for n, k in keys.items():
        if k:
            owners.setdefault(frozenset(k), []).append(n)
    single_keys = {next(iter(k)) for k in owners if len(k) == 1}

    h = g.copy()

    # (1) text views, any key order. Any table whose key holds a text key IS a text view, with or
    #     without a master in the lakehouse: never the source or the target of a reference edge.
    text_views: set[str] = {a for a, ka in keys.items() if set(ka) & text_keys and len(ka) > 1}
    for a, ka in keys.items():
        if a not in text_views:
            continue
        masters = [b for b, kb in keys.items() if b != a and is_text_view(ka, kb, text_keys)]
        if len(masters) == 1:
            b = masters[0]
            text_views.add(a)
            h.add_edge(a, b, field=",".join(k for k in ka if k not in text_keys), kind="TEXT_OF",
                       inferred=True, reason=f"{a} is the text view of {b}")
        elif len(masters) > 1:
            ambiguous.append({"column": ",".join(ka), "owners": sorted(masters), "reason": f"text view {a} matches several masters"})

    # (2)+(3) reference edges, keys as sets, never into a text view
    for key, tabs in sorted(owners.items(), key=lambda kv: len(kv[0])):
        if len(key) > 1 and not include_multi_column_keys:
            continue
        if len(key) == 1 and next(iter(key)) in text_keys and not include_text_key_edges:
            continue
        if len(tabs) > 1:
            ambiguous.append({"column": ",".join(sorted(key)), "owners": sorted(tabs),
                              "reason": f"{'single' if len(key) == 1 else 'multi'}-column key of several tables"})
            continue
        a = tabs[0]
        if a in text_views:
            continue                                      # references point to the master, not its text
        ka = keys[a]
        for b in g.nodes:
            if b == a or b in text_views or not key <= cols[b]:
                continue                                  # a text view is never the source of a reference
            if frozenset(keys[b]) == key:
                continue                                  # same key: 1:1 extension, not a reference
            if is_item_of(keys[b], ka, single_keys - set(ka), text_keys):
                continue                                  # header/item, handled elsewhere
            if not h.has_edge(b, a):
                h.add_edge(b, a, field=",".join(ka), kind="INFERRED", inferred=True,
                           reason=f"{b} carries key of {a} ({','.join(ka)})")

    # inferred edges carry their mapping explicitly: source column -> target key column (same names)
    for a, b, d in h.edges(data=True):
        if "mapping" not in d:
            d["mapping"] = [(k, k) for k in keys[b]]

    not_applied: list[dict] = []
    for c in confirmed or []:
        f, t = c["from"].upper(), c["to"].upper()
        act = c.get("action", "confirm")
        if f not in h.nodes or t not in h.nodes:
            continue                                      # table not in this lakehouse: entry ignored
        if act == "drop":
            if h.has_edge(f, t):
                h.remove_edge(f, t)
            continue
        mapping, problem = resolve_mapping(c.get("field", ""), cols[f], keys[t])
        if problem:
            not_applied.append({"from": f, "to": t, "field": c.get("field", ""), "reason": problem})
            continue
        h.add_edge(f, t, field=",".join(src for src, _ in mapping), mapping=mapping,
                   kind="CONFIRMED" if act == "confirm" else act.upper(), inferred=False, reason="yaml")
    h.graph["not_applied"] = not_applied
    return h, ambiguous


def resolve_mapping(field: str | list[str], source_cols: set[str], target_keys: list[str]) -> tuple[list[tuple[str, str]], str | None]:
    """Map referencing column(s) in the source table onto the target's key parts, in full.

    `field` names the referencing column(s) (string, comma-separated, or list). They are matched
    to target key parts by name first, else by position; remaining key parts are filled with
    same-named columns of the source table. Returns (mapping, None) or ([], reason) when any part
    cannot be filled or a column does not exist in the source — Fabric requires
    len(targetKeyRefBindings) == len(entityIdParts).
    """
    given = [x.strip().upper() for x in (field if isinstance(field, list) else str(field).split(",")) if x and x.strip()]
    target_keys = [k.upper() for k in target_keys]
    if not target_keys:
        return [], "target has no key"
    mapping: dict[str, str] = {}                           # target key col -> source col
    unplaced = []
    for g_ in given:
        if g_ in target_keys and g_ not in mapping:
            mapping[g_] = g_
        else:
            unplaced.append(g_)
    for tk in target_keys:                                 # positional fill for role-named columns
        if tk not in mapping and unplaced:
            mapping[tk] = unplaced.pop(0)
    for tk in target_keys:                                 # same-named fill for the rest
        if tk not in mapping and tk in source_cols:
            mapping[tk] = tk
    missing = [tk for tk in target_keys if tk not in mapping]
    if missing:
        return [], f"cannot fill target key part(s) {','.join(missing)} from source columns"
    absent = [src for src in mapping.values() if src not in source_cols]
    if absent:
        return [], f"column(s) {','.join(absent)} not in source table"
    if unplaced:
        return [], f"field(s) {','.join(unplaced)} do not fit the target key {','.join(target_keys)}"
    return [(mapping[tk], tk) for tk in target_keys], None


def standard_relationships() -> list[dict]:
    """The bundled standard S/4 relationships list (role-named CDS keys name inference cannot see)."""
    import yaml
    from importlib.resources import files
    return yaml.safe_load(files("sap_ontology").joinpath("data/relationships_standard.yaml").read_text(encoding="utf-8"))


def _spark_to_dd(t: str) -> str:
    t = t.lower()
    if t.startswith("decimal"): return "DEC"
    if t in ("int", "smallint", "tinyint"): return "INT4"
    if t == "bigint": return "INT8"
    if t == "double" or t == "float": return "FLTP"
    if t == "date": return "DATS"
    if t.startswith("timestamp"): return "TIMS"
    return "CHAR"
