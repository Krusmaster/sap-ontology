"""Read SAP Data Dictionary tables (DD02L, DD02T, DD03L, DD08L, DD05S) into a graph.

Input can be CSV files (SE16 export), Parquet, or any pandas-readable source. In Fabric,
pass the mirrored Delta tables as DataFrames directly.

Only the columns listed in REQUIRED are used; everything else is ignored.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import networkx as nx
import pandas as pd

REQUIRED = {
    "DD02L": ["TABNAME", "TABCLASS"],                       # tables; TABCLASS=TRANSP for real tables
    "DD03L": ["TABNAME", "FIELDNAME", "POSITION", "KEYFLAG", "ROLLNAME", "DOMNAME", "DATATYPE", "LENG", "CHECKTABLE"],
    "DD08L": ["TABNAME", "FIELDNAME", "CHECKTABLE", "FRKART"],  # foreign key headers
}
OPTIONAL = {
    "DD02T": ["TABNAME", "DDLANGUAGE", "DDTEXT"],          # table descriptions
    "DD05S": ["TABNAME", "FIELDNAME", "FORTABLE", "FORKEY", "CHECKTABLE", "CHECKFIELD", "PRIMPOS"],  # FK field map
}

TECH_FIELDS = {"MANDT", ".INCLUDE", ".APPEND"}


@dataclass
class Table:
    name: str
    text: str = ""
    keys: list[str] = field(default_factory=list)
    fields: list[dict] = field(default_factory=list)


def _load(path: Path, name: str) -> pd.DataFrame | None:
    for ext in (".csv", ".parquet"):
        p = path / f"{name}{ext}"
        if p.exists():
            df = pd.read_csv(p, dtype=str) if ext == ".csv" else pd.read_parquet(p)
            df.columns = [c.upper() for c in df.columns]
            return df
    return None


def load_dd(path: str | Path) -> dict[str, pd.DataFrame]:
    """Load DD tables from a folder of CSV/Parquet files named after the table (DD03L.csv ...)."""
    path = Path(path)
    out: dict[str, pd.DataFrame] = {}
    for name, cols in REQUIRED.items():
        df = _load(path, name)
        if df is None:
            raise FileNotFoundError(f"{name} not found in {path} (need {', '.join(REQUIRED)})")
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"{name} is missing columns {missing}")
        out[name] = df
    for name in OPTIONAL:
        df = _load(path, name)
        if df is not None:
            out[name] = df
    return out


def build_graph(dd: dict[str, pd.DataFrame], tables: list[str] | None = None, language: str = "E") -> nx.DiGraph:
    """Tables become nodes, DD08L foreign keys become directed edges (table -> check table)."""
    dd02l = dd["DD02L"]
    dd03l = dd["DD03L"].copy()
    dd08l = dd["DD08L"]

    real = set(dd02l.loc[dd02l["TABCLASS"].str.upper() == "TRANSP", "TABNAME"])
    if tables:
        real &= set(tables)

    texts: dict[str, str] = {}
    if "DD02T" in dd:
        t = dd["DD02T"]
        t = t[t["DDLANGUAGE"].str.upper() == language.upper()]
        texts = dict(zip(t["TABNAME"], t["DDTEXT"].fillna("")))

    dd03l["POSITION"] = pd.to_numeric(dd03l["POSITION"], errors="coerce")
    dd03l = dd03l[dd03l["TABNAME"].isin(real) & ~dd03l["FIELDNAME"].isin(TECH_FIELDS)]
    dd03l = dd03l.sort_values(["TABNAME", "POSITION"])

    g = nx.DiGraph()
    for name, grp in dd03l.groupby("TABNAME", sort=False):
        fields = grp.to_dict("records")
        keys = [f["FIELDNAME"] for f in fields if str(f.get("KEYFLAG", "")).upper() == "X"]
        g.add_node(name, text=texts.get(name, ""), keys=keys, fields=fields)

    fk = dd08l[dd08l["TABNAME"].isin(real) & dd08l["CHECKTABLE"].isin(real)]
    for r in fk.itertuples(index=False):
        if r.TABNAME == r.CHECKTABLE:
            continue
        g.add_edge(r.TABNAME, r.CHECKTABLE, field=r.FIELDNAME, kind=str(getattr(r, "FRKART", "") or ""))
    return g


def suggest_business_objects(g: nx.DiGraph) -> list[dict]:
    """Header/item detection: B is an item table of A when A's keys are a strict prefix of B's keys.

    Classic SAP: VBAK(VBELN) -> VBAP(VBELN, POSNR); EKKO(EBELN) -> EKPO(EBELN, EBELP).
    Returns one suggestion per header table with its item tables, longest chains first.
    """
    keys = {n: tuple(d["keys"]) for n, d in g.nodes(data=True) if d.get("keys")}
    single_keys = {k[0] for k in keys.values() if len(k) == 1}
    suggestions = []
    for head, hk in keys.items():
        if not hk:
            continue
        # item = header key + exactly one column that is NOT another table's key (else it is a reference)
        items = [t for t, tk in keys.items()
                 if t != head and len(tk) == len(hk) + 1 and tk[: len(hk)] == hk
                 and tk[-1] not in (single_keys - set(hk))]
        if items:
            suggestions.append({
                "object": head,
                "text": g.nodes[head].get("text", ""),
                "header": head,
                "items": sorted(items),
                "join_keys": list(hk),
            })
    return sorted(suggestions, key=lambda s: (-len(s["items"]), s["object"]))
