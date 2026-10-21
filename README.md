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
  relationships, data bindings to the mirrored lakehouse tables — ready to create with one command,
- an **API for GraphQL** item definition from the same graph, so apps (e.g. Lovable's Fabric
  connector) get the tables *with* relationships without configuring them by hand.

```
sap-ontology objects  ./dd          # which header/item business objects exist?
sap-ontology graph    ./dd --mermaid sap.mmd --graphml sap.graphml --neo4j ./neo4j --kql sap.kql
sap-ontology ontology ./dd --name SAP_Sales --workspace <ws-id> --lakehouse <lh-id>
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

## Quickstart — Fabric SAP Mirror, one notebook

Run in a Fabric notebook attached to the lakehouse (or a lakehouse with shortcuts to the mirrored
tables). Use a **sandbox workspace**: creating an Ontology item also creates a Lakehouse and a
GraphModel beside it.

```python
%pip install git+https://github.com/Krusmaster/sap-ontology
```

```python
import yaml, sap_ontology as so
from sap_ontology.cli import deploy

WORKSPACE_ID = "<sandbox workspace id>"
LAKEHOUSE_ID = "<lakehouse id that holds the mirrored tables>"
my_relationships = []     # or yaml.safe_load(open("relationships.yaml")) for your own Z/Y views

# 1. Tables + keys from DD03ND. Custom Z/Y views get keys only from custom_keys.
g, report = so.from_lakehouse(spark, schema="dbo", strip_suffix=True,
                              custom_keys={"ZC_SALESKPI": ["SalesDocument"]})
print(report.summary()); print("unmatched:", report.unmatched_tables)

# 2. Relationships: inferred from keys + the bundled standard S/4 list (role-named keys) + your own.
h, ambiguous = so.infer_edges(g, confirmed=so.standard_relationships() + my_relationships)
print(h.number_of_nodes(), "entities,", h.number_of_edges(), "relationships")
print("ambiguous:", ambiguous); print("not applied:", h.graph["not_applied"])   # YAML entries that could not map a full key

# 3. Definition + create. Name: letters, digits, underscores only.
env = so.fabric_iq_definition(h, "SAP_Mirror_Ontology", WORKSPACE_ID, LAKEHOUSE_ID)
print("skipped (no keys):", env.get("skipped", []))
print(deploy(env, WORKSPACE_ID))                                    # uses notebookutils token
```

Then open the Ontology item in the workspace and check that relationships are queryable.
**Re-running updates the existing item** (same `displayName` → `updateDefinition`), so a second run
creates no new Lakehouse or GraphModel; ids are deterministic, so entities keep their identity.

*Status (v0.2): this cell ran end-to-end in a Fabric sandbox on synthetic fixtures, including the
`updateDefinition` path on re-run. The inference rules were cross-checked against a live S/4
mirror's metadata; the package itself has not run against a live mirror.*

## Same graph, second consumer: API for GraphQL (for apps)

The Ontology item feeds Fabric IQ. Apps — Lovable's Fabric connector, any GraphQL client — use a
Fabric **API for GraphQL** item instead, and that item needs the same configuration: which tables,
which keys, which relationships. One more call gives you both from one graph:

```python
from sap_ontology.cli import deploy, default_token_provider
headers = {"Authorization": f"Bearer {default_token_provider()()}"}
sql_ep = so.sql_endpoint_id_for_lakehouse(WORKSPACE_ID, LAKEHOUSE_ID, headers)   # GraphQL reads a lakehouse via its SQL endpoint

gql = so.fabric_graphql_definition(h, "SAP_Mirror_GraphQL", WORKSPACE_ID, sql_ep)
print(deploy(gql, WORKSPACE_ID))
```

What it emits: one `graphqlType` per table (technical columns dropped), `Query` + `Query_by_pk`
(lakehouse and mirrored sources are read-only through GraphQL; `writable=True` adds
Create/Update on Warehouse/SQL sources), a `ManyToOne` relationship per graph edge plus its
`OneToMany` reverse, `header`/`items` relationships for header/item pairs, and `master`/`*_texts`
for text views. Entities without keys are skipped.

Then in Lovable: Integrations → Microsoft Fabric → the item's `/graphql` URL + a service principal
with *Run Queries and Mutations* on the item and introspection enabled.

*Status: the GraphQLApi definition is GA and documented, and this output follows the documented
schema — but it has not been created in a workspace yet. Three things to verify in a sandbox:
(1) `SqlAnalyticsEndpoint` + the endpoint id is the right way to expose a lakehouse,
(2) `relationships[].targetObject` takes the `graphqlType` name, (3) `updateDefinition` works
for GraphQLApi as it does for Ontology.*

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
env = so.fabric_iq_definition(h, "SAP_Sales", workspace_id, lakehouse_id)
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
  **`so.standard_relationships()`** returns ~50 bundled standard S/4 relationships that name
  inference cannot see because CDS keys are role-named (`SoldToParty` → `I_Customer`). Entries
  for tables not in your lakehouse are ignored. An entry is applied only if every part of the
  target's key can be mapped from the source table — by the named field, then by same-named
  columns (`STORAGELOCATION` onto `(PLANT, STORAGELOCATION)` fills `PLANT` from the source);
  otherwise it lands in `h.graph["not_applied"]` with the reason. Fabric requires exactly one
  binding per target key part, and `fabric_iq_definition` checks that before you reach Fabric.
- **Text views**: A is the text view of B when A's key contains a text key (`LANGUAGE`, `SPRAS`,
  …) and, text keys removed, equals B's key *as a set* — any order, text key anywhere
  (`I_COSTCENTERTEXT (COSTCENTER, CONTROLLINGAREA, LANGUAGE, VALIDITYENDDATE)` →
  `I_COSTCENTER (CONTROLLINGAREA, COSTCENTER, VALIDITYENDDATE)`). Edge `TEXT_OF` to the master,
  never the reverse, and **no reference edge ever targets a text view** — a master that carries
  `LANGUAGE` as a plain column does not point at other entities' texts. Multi-column keys are
  compared as sets. Edges to the language table itself are dropped by default
  (`--include-text-key-edges` to keep them) — on a real S/4 mirror they were a large share of all edges.
  A text view whose master is not replicated (`I_PRODUCTDESCRIPTION` with no `I_PRODUCT`) is still
  a text view: it is never the source or the target of a reference edge.
- **Entities without keys** (custom views with no `custom_keys`) are skipped from the ontology and
  listed under `skipped`; Fabric needs `entityIdParts`. 2- and 3-part keys are accepted.
- **Bindings use the name the Spark catalog lists** (`catalog_name`): upper case on a mirror,
  lower case when Spark wrote the table. Override per table with `table_name_map`.
- **Custom Z/Y views**: name inference finds almost nothing there. `custom_keys` +
  `relationships.yaml` are the primary source for them, and will stay so until CDS associations
  reach the mirror (DD03ND carries none today).

Local dry run of the same logic on a CSV export:

```
sap-ontology mirror DD03ND.csv tables.txt --strip-suffix --custom-keys keys.yaml --relationships rel.yaml --mermaid out.mmd
```

## Deploying — read this first

`deploy` posts the definition with the running user's token: `notebookutils.credentials.getToken`
inside a Fabric notebook, `az account get-access-token` locally. Pass your own `token_provider`
to `sap_ontology.cli.deploy()` for anything else.

**Every create — including a failed import — creates companion items** in the workspace: a
Lakehouse (`<name>_lh_<guid>`) with its SQL endpoint, and a GraphModel (`<name>_graph_<guid>`).
No Eventhouse or KQL database (an earlier note said so; corrected after testing). A failed
import leaves the three behind, so deploy to a sandbox workspace and clean up after failures.

**Item name:** letters, digits and underscores only (`SAP_Sales`, not `SAP Sales`).
`fabric_iq_definition` refuses anything else before you hit Fabric's `BadArtifactCreateRequest`.

**Open question:** whether a data binding may point at a *mirrored database's* tables directly
or needs a lakehouse shortcut. Not verified yet; the binding currently targets a lakehouse item id.

IDs are deterministic (hashed from names) and `deploy()` updates an item of the same name instead
of creating a second one.

## Status

v0.2. The Fabric IQ ontology item is in preview. The definition shape now follows one
that Fabric **accepted and returned via Get Item Definition** (October 2026): `entityIdParts`,
namespace `usertypes` / `namespaceType Custom`, `dataBindingConfiguration` with
`sourceTableProperties`, and a `Contextualizations/` file per relationship type mapping source
keys and referencing columns. Without contextualizations, relationship types link nothing.
**Verified 2026-10-21 on synthetic fixtures in a Fabric sandbox:** an Ontology item was created
and then updated in place from this output — 11 entities (2- and 3-part keys accepted), 11
bindings, 9 relationship types with 9 contextualizations (a 3-column source key onto a 2-column
target accepted). The inference rules were cross-checked against a live S/4 mirror's metadata;
the package itself has not run against a live mirror yet. Lessons from the failed attempts: ids
must be JSON strings, item names may not contain spaces, and every contextualization must bind
exactly one source column per target key part.

## Works on

| Platform | Status |
| --- | --- |
| Fabric | yes — all features, incl. the Fabric IQ ontology export |
| Databricks | yes — graph, business objects, GraphML/Neo4j/KQL from Spark tables or CSV |
| Snowflake | yes — graph, business objects, GraphML/Neo4j from CSV or a pandas DataFrame |
| Local Python | yes — from SE16 CSV exports |

## License

MIT
