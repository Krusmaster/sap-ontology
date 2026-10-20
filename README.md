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

Get them as CSV from SE16, or point the library at the mirrored Delta tables in Fabric. Nothing
customer-specific is needed and nothing leaves your machine until you run `deploy`.

## In a Fabric notebook

```python
import sap_ontology as so
dd = {t: spark.table(f"sap_mirror.{t.lower()}").toPandas() for t in ["DD02L", "DD03L", "DD08L", "DD02T"]}
g = so.build_graph(dd, tables=["VBAK", "VBAP", "VBEP", "KNA1", "MARA"])
print(so.suggest_business_objects(g))
env = so.fabric_iq_definition(g, "SAP Sales", workspace_id, lakehouse_id)
```

IDs are deterministic (hashed from names), so re-running updates the ontology instead of
duplicating it.

## Status

v0.1. The Fabric IQ ontology item is in preview and documented by example only. Field names
here follow Microsoft's ontology-authoring guidance; before trusting them, create one ontology by
hand in your tenant, fetch it with *Get Item Definition*, and diff. Report mismatches as issues.

## License

MIT
