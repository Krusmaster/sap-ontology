# sap-ontology

**The problem.** Every SAP system already describes itself: which tables exist, which fields they
have, which keys bind them, which tables are headers and which are items. That description lives
in the Data Dictionary (DD02L, DD03L, DD08L) and almost nobody reads it. So the same questions get
answered by hand, in meetings, for every data product: *which tables belong together? which is the
header? what do I join on?* Weeks of translation before any engineering starts.

**What this does.** Reads the Data Dictionary and answers those questions automatically:

- a **graph** of tables and foreign keys (Mermaid ER export),
- **business-object suggestions** — header/item pairs found from key structure
  (VBAK→VBAP→VBEP, EKKO→EKPO) with the join keys, the starting point for a silver layer,
- **graph-database exports** — GraphML (Gephi, yEd, Neo4j), Neo4j CSV for `neo4j-admin import`,
  and a KQL `make-graph` snippet for Fabric Eventhouse,
- a **Fabric IQ Ontology** item definition — entity types, keys, typed properties,
  relationships, data bindings to the mirrored lakehouse tables — ready to create with one command.

```
sap-ontology objects  ./dd          # which header/item business objects exist?
sap-ontology graph    ./dd --mermaid sap.mmd --graphml sap.graphml --neo4j ./neo4j --kql sap.kql
sap-ontology ontology ./dd --name "SAP Sales" --workspace <ws-id> --lakehouse <lh-id>
sap-ontology deploy   ontology.json --workspace <ws-id>     # az login first
```

```
VBAK — Sales Document: Header Data
    header: VBAK  items: VBAP  join on: VBELN
VBAP — Sales Document: Item Data
    header: VBAP  items: VBEP  join on: VBELN, POSNR
EKKO — Purchasing Document Header
    header: EKKO  items: EKPO  join on: EBELN
```

## What it reads

Standard SAP Data Dictionary tables — the same in ECC and S/4, since the 90s:

| Table | Gives | Required |
| --- | --- | --- |
| DD02L | which tables exist (TABCLASS = TRANSP) | yes |
| DD03L | fields, key flags, data types, check tables | yes |
| DD08L | foreign key definitions | yes |
| DD02T | table descriptions | no |
| DD05S | foreign key field mappings | no (v0.2) |
| DD03ND | CDS view fields/keys/types (SAP Mirror) | alternative input, see below |

Get them as CSV from SE16, or point the library at the mirrored Delta tables in Fabric. Nothing
customer-specific is needed and nothing leaves your machine until you run `deploy`.

## Two kinds of input

**Classic Data Dictionary** (DD02L, DD03L, DD08L — ECC/S/4 tables exported with SE16, or any
mirror that replicates them): `load_dd` + `build_graph`. Foreign keys come from DD08L.

**Fabric SAP Mirror** (S/4 CDS views → Datasphere replication flow → mirrored database): there is
no DD02L/DD03L/DD08L. The mirror holds the CDS views as upper-case Delta tables and one reference
table, **DD03ND** (fields, keys, types per view — no foreign keys, no texts). Use the `mirror`
adapter:

```python
import sap_ontology as so
g, report = so.from_lakehouse(spark, schema="dbo", strip_suffix=True,
                              custom_keys={"ZC_SALESKPI": ["SalesDocument"]})
print(report.summary())            # exact / via suffix / via map / unmatched (custom Z/Y)
h, ambiguous = so.infer_edges(g, confirmed=yaml.safe_load(open("relationships.yaml")))
print(so.suggest_business_objects(h))
env = so.fabric_iq_definition(h, "SAP Sales", workspace_id, lakehouse_id)
```

What the adapter does, and does not do:

- Filters DD03ND to the lakehouse's tables **in Spark before `toPandas()`** (DD03ND is ~1.8M rows).
- Matches names exactly, then — opt-in — after stripping a `_<SID>` suffix (`I_VIEW_P01` → `I_VIEW`),
  then via an explicit `name_map`. Every unmatched table is reported in both directions.
- Custom Z/Y views are not in DD03ND: columns come from the Spark schema, keys **only** from
  `custom_keys`. Nothing is guessed.
- Keeps `as4local = 'A'`, ignores `nodename`, warns if a view has more than one node.
- `DATS` → DateTime, `NUMC` stays String (leading zeros), `CURR/QUAN/DEC` → Double.
- **Relationships are inferred**, since DD08L is absent: B → A when B carries A's complete key
  (single-column keys first). Every inferred edge is marked `inferred=True` with a reason.
  A column that is the key of two tables is reported as ambiguous, not guessed. A
  `relationships.yaml` can confirm, add, override or drop edges — see `examples/mirror/`.

Local dry run of the same logic on a CSV export:

```
sap-ontology mirror DD03ND.csv tables.txt --strip-suffix --custom-keys keys.yaml --relationships rel.yaml --mermaid out.mmd
```

## Deploying — read this first

`deploy` posts the definition with the running user's token: `notebookutils.credentials.getToken`
inside a Fabric notebook, `az account get-access-token` locally. Pass your own `token_provider`
to `sap_ontology.cli.deploy()` for anything else.

**Creating an Ontology item can also create companion items** in the workspace — an Eventhouse,
a KQL database and a graph model were observed. Deploy to a sandbox workspace first, never
straight into a shared DEV.

**Open question:** whether a data binding may point at a *mirrored database's* tables directly
or needs a lakehouse shortcut. Not verified yet; the binding currently targets a lakehouse item id.

IDs are deterministic (hashed from names), so re-running updates the ontology instead of
duplicating it.

## Status

v0.1. The Fabric IQ ontology item is in preview and documented by example only. Field names
here follow Microsoft's ontology-authoring guidance; before trusting them, create one ontology by
hand in your tenant, fetch it with *Get Item Definition*, and diff. Report mismatches as issues.

## Works on

| Platform | Status |
| --- | --- |
| Fabric | yes — all features, incl. the Fabric IQ ontology export |
| Databricks | yes — graph, business objects, GraphML/Neo4j/KQL from Spark tables or CSV |
| Snowflake | yes — graph, business objects, GraphML/Neo4j from CSV or a pandas DataFrame |
| Local Python | yes — from SE16 CSV exports |

## License

MIT
