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
        g.add_node(table, text="", keys=keys, fields=fields, source="dd03nd", view=view)

    for table in rep.unmatched_tables:
        schema = spark_schemas.get(table)
        if not schema:
            continue                                   # nothing known about it — skip, it is reported
        fields = [{"FIELDNAME": c, "POSITION": i, "KEYFLAG": "X" if c in custom_keys.get(table, []) else "",
                   "DATATYPE": _spark_to_dd(t), "LENG": 0, "DECIMALS": 0}
                  for i, (c, t) in enumerate(schema.items(), 1) if c not in TECH_COLS]
        g.add_node(table, text="", keys=custom_keys.get(table, []), fields=fields, source="spark", view=None)
    return g, rep


def from_lakehouse(
    spark, schema: str = "dbo", dd03nd_table: str = "DD03ND", custom_keys=None, strip_suffix=False, name_map=None,
) -> tuple[nx.DiGraph, MatchReport]:
    """Fabric entry point. Filters DD03ND to the lakehouse's tables in Spark before toPandas()."""
    from pyspark.sql import functions as F  # type: ignore

    tables = [t.name.upper() for t in spark.catalog.listTables(schema) if t.name.upper() != dd03nd_table.upper()]
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
    schemas = {t: {f.name: f.dataType.simpleString() for f in spark.table(f"{schema}.{t}").schema.fields} for t in tables}
    return from_dd03nd(dd, tables, spark_schemas=schemas, custom_keys=custom_keys, strip_suffix=strip_suffix, name_map=name_map)


def infer_edges(
    g: nx.DiGraph,
    confirmed: list[dict] | None = None,
    single_key_first: bool = True,
) -> tuple[nx.DiGraph, list[dict]]:
    """Derive relationships from key structure when no foreign-key catalogue exists.

    Rule: table B -> table A when B carries ALL of A's key columns (by name, case-insensitive)
    and A's key is not a prefix of B's own key (that is header/item, handled elsewhere).
    Single-column keys are tried first; multi-column keys only when single_key_first=False or
    after singles. Every inferred edge gets inferred=True and a reason. A column that is the
    key of two or more tables is AMBIGUOUS: no edge, reported instead.

    confirmed: list of {"from": B, "to": A, "field": col, "action": "confirm"|"override"|"add"|"drop"}
               from a relationships YAML. Applied after inference; "drop" removes an inferred edge.
    """
    keys = {n: [k.upper() for k in d.get("keys", [])] for n, d in g.nodes(data=True)}
    cols = {n: {f["FIELDNAME"].upper() for f in d.get("fields", [])} for n, d in g.nodes(data=True)}
    ambiguous: list[dict] = []

    # who owns which single-column key?
    owners: dict[str, list[str]] = {}
    for n, k in keys.items():
        if len(k) == 1:
            owners.setdefault(k[0], []).append(n)

    h = g.copy()
    for col, tabs in owners.items():
        if len(tabs) > 1:
            ambiguous.append({"column": col, "owners": sorted(tabs), "reason": "single-column key of several tables"})
            continue
        a = tabs[0]
        for b in g.nodes:
            if b == a or col not in cols[b]:
                continue
            if keys[b][: len(keys[a])] == keys[a]:
                continue                                  # header/item prefix — not a reference edge
            h.add_edge(b, a, field=col, kind="INFERRED", inferred=True, reason=f"{b}.{col} = key of {a}")

    if not single_key_first or True:                      # multi-column keys, after singles
        for a, ka in keys.items():
            if len(ka) < 2:
                continue
            for b in g.nodes:
                if b == a or not set(ka) <= cols[b] or keys[b][: len(ka)] == ka:
                    continue
                if not h.has_edge(b, a):
                    h.add_edge(b, a, field=",".join(ka), kind="INFERRED", inferred=True,
                               reason=f"{b} carries full key of {a}")

    for c in confirmed or []:
        f, t = c["from"].upper(), c["to"].upper()
        act = c.get("action", "confirm")
        if act == "drop":
            if h.has_edge(f, t):
                h.remove_edge(f, t)
        else:
            h.add_edge(f, t, field=c.get("field", ""), kind="CONFIRMED" if act == "confirm" else act.upper(), inferred=False, reason="yaml")
    return h, ambiguous


def _spark_to_dd(t: str) -> str:
    t = t.lower()
    if t.startswith("decimal"): return "DEC"
    if t in ("int", "smallint", "tinyint"): return "INT4"
    if t == "bigint": return "INT8"
    if t == "double" or t == "float": return "FLTP"
    if t == "date": return "DATS"
    if t.startswith("timestamp"): return "TIMS"
    return "CHAR"
