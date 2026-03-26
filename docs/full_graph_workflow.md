# Full Graph Workflow

This repository can now build and export the **entire offline lineage graph**
from existing `bq-extraction` results. The full graph is intended for tools
that are designed for large graphs, such as **Gephi** and **Cytoscape**.

This workflow does **not** require any new data from Fintual. It only uses the
extracted artifacts already present in:

- `datasets.json`
- `tables.json`
- `tables.ddls.json`
- `jobs.query_logs.json`
- `jobs.frequent_queries.json`

## Why use a desktop graph tool?

The current graph can be large enough that a browser or notebook cannot render
it usefully as a single interactive canvas. In current real runs, the graph is
on the order of tens of thousands of nodes and over one hundred thousand edges.

Desktop graph tools are a better fit for:

- full connected-component exploration
- large-force layouts
- graph metrics and centrality
- filtering and styling at scale

## Generate the graph exports

First install the analysis extra in this standalone repo:

```bash
cd /Users/alen/Documents/Development/bq-extraction
uv sync --extra analysis
```

Then run the lineage build script against the results you already have:

```bash
uv run python scripts/build_lineage.py \
  --results-dir ../velum-extraction-results \
  --output-dir output/lineage
```

By default this now writes:

- `output/lineage/neutral/`
  - `lineage_nodes.json`
  - `lineage_edges.json`
  - `lineage_queries.json`
  - `lineage_issues.json`
- `output/lineage/atlas/`
  - Atlas-compatible JSON exports
- `output/lineage/atlas.db`
  - Atlas-compatible SQLite DB
- `output/lineage/full-graph/`
  - `lineage.graphml`
  - `lineage_assets_only.graphml`
  - `lineage_assets_logical.graphml`
  - `lineage_nodes.ndjson`
  - `lineage_edges.ndjson`
  - `lineage_nodes_chunks/*.json`
  - `lineage_edges_chunks/*.json`
  - `manifest.json`

Optional flags:

```bash
uv run python scripts/build_lineage.py \
  --results-dir ../velum-extraction-results \
  --output-dir output/lineage \
  --gexf \
  --chunk-size 25000
```

## Export formats

### GraphML

Best first choice for:

- Gephi
- Cytoscape
- graph interchange

Files:

- `output/lineage/full-graph/lineage.graphml`
- `output/lineage/full-graph/lineage_assets_only.graphml`
- `output/lineage/full-graph/lineage_assets_logical.graphml`

Recommended:

- open `lineage_assets_only.graphml` if you want a graph that represents only
  **tables**, **views**, and **materialized views**
- open `lineage_assets_logical.graphml` if you want the same asset-only graph
  but with timestamped temp tables collapsed into canonical logical temp nodes
- open `lineage.graphml` if you want the full mixed graph, including consumers

### GEXF

Optional alternative for graph tools that prefer it.

Enable with:

```bash
--gexf
```

### NDJSON

Best for:

- streaming pipelines
- grep / jq workflows
- incremental downstream processing

Files:

- `lineage_nodes.ndjson`
- `lineage_edges.ndjson`

### Chunked compact JSON

Best for:

- very large payload handling
- browser/server ingestion with chunked loading
- avoiding one huge pretty-printed JSON file

Files:

- `lineage_nodes_chunks/*.json`
- `lineage_edges_chunks/*.json`

## Metadata flattening strategy

Graph tools generally do not handle nested dicts and lists well. For GraphML
and GEXF, metadata is flattened as follows:

- scalar metadata becomes flat attributes such as `meta_project_id`
- nested objects are flattened recursively, e.g. `meta_nested_table_id`
- arrays of scalar values are pipe-joined, e.g. `meta_provenance_sources`
- all original metadata is also preserved losslessly in `meta_json`

That means:

- graph tools can filter/sort on flat fields
- no information is lost because the full JSON is still available

## Opening in Gephi

1. Open Gephi.
2. Choose `Open Graph File`.
3. Select:
   - `output/lineage/full-graph/lineage_assets_logical.graphml` for the
     cleanest warehouse dependency graph
   - `output/lineage/full-graph/lineage_assets_only.graphml` for the raw
     physical asset graph
   - `output/lineage/full-graph/lineage.graphml` for the full mixed graph
4. In the import dialog:
   - keep it as a directed graph
   - enable edge metadata import
5. Once loaded:
   - run `Statistics -> Average Degree`
   - run `Layout -> ForceAtlas 2` or `OpenOrd`
   - color nodes by `node_type`
   - color edges by `edge_type`
   - filter on `meta_cross_project`, `meta_project_id`, or `meta_dataset_id`

Useful fields:

- nodes:
  - `node_type`
  - `name`
  - `meta_project_id`
  - `meta_dataset_id`
  - `meta_table_id`
  - `meta_consumer_kind`
- edges:
  - `edge_type`
  - `meta_query_count`
  - `meta_cross_project`
  - `meta_statement_type`
  - `meta_query_sources`

In the asset-only GraphML:

- nodes are limited to `table`, `view`, and `materialized_view`
- edges are limited to `writes` and `view_depends_on`

In the logical asset-only GraphML:

- the same asset-only node and edge rules apply
- timestamped temp tables in `temp_incremental_tables` are collapsed into one
  canonical temp node per base name
- the temp layer is still visible, but much less noisy

## Opening in Cytoscape

1. Open Cytoscape.
2. Import:
   - `lineage_assets_logical.graphml` for the cleaner logical asset graph
   - `lineage_assets_only.graphml` for the raw physical asset graph
   - `lineage.graphml` for the full mixed graph
3. Create styles:
   - node fill color by `node_type`
   - edge color by `edge_type`
   - node label by `name`
4. Use filters on:
   - `meta_project_id`
   - `meta_dataset_id`
   - `meta_cross_project`
   - `edge_type`

## Practical recommendations

- Use the notebook for **bounded local exploration**.
- Use Gephi or Cytoscape for the **entire graph**.
- Prefer `lineage_assets_logical.graphml` when you want a warehouse dependency
  graph without consumers and without thousands of timestamped temp duplicates.
- Use `lineage_assets_only.graphml` if you need raw physical fidelity.
- Use `atlas.db` if you want Atlas-compatible storage and querying.
- Use NDJSON or chunked JSON if you want to feed the graph into another service.

## Current limitations

- GraphML and GEXF exports preserve all metadata, but some nested fields are
  stringified into `meta_json` and flat string attributes.
- Write lineage is still heuristic for some BigQuery statements because the
  extraction data does not currently include all destination-table metadata.
- The full graph export is designed for analysis tools, not for loading
  directly into the current notebook or browser UI at full scale.
