from __future__ import annotations

import json

from bq_extraction.atlas_export import build_atlas_export
from bq_extraction.lineage import build_lineage_graph

from .test_lineage import make_runs


def test_build_atlas_export_maps_graph_to_atlas_records() -> None:
    graph = build_lineage_graph(make_runs())
    atlas = build_atlas_export(graph, source_id="offline")

    asset_ids = {row["id"] for row in atlas.assets}
    edge_ids = {row["id"] for row in atlas.edges}
    consumer_ids = {row["id"] for row in atlas.consumers}
    query_fingerprints = {row["fingerprint"] for row in atlas.queries}

    assert "offline::proj-a.raw.orders" in asset_ids
    assert "offline::proj-a.analytics.orders_view" in asset_ids
    assert "offline::query::analyst@example.com" in asset_ids

    assert "consumer:analyst@example.com" in consumer_ids

    assert "offline::proj-a.raw.orders:offline::query::analyst@example.com:reads" in edge_ids
    assert "offline::proj-a.raw.orders:offline::proj-a.analytics.daily_orders:writes" in edge_ids
    assert "offline::proj-a.raw.accounts:offline::proj-a.analytics.accounts_view:depends_on" in edge_ids

    assert "offline:proj-a:hash-1" in query_fingerprints

    consumer_assets = {
        (row["consumer_id"], row["asset_id"])
        for row in atlas.consumer_assets
    }
    assert ("consumer:analyst@example.com", "offline::proj-a.raw.orders") in consumer_assets

    snapshot = next(
        row for row in atlas.schema_snapshots if row["asset_id"] == "offline::proj-a.raw.orders"
    )
    columns = json.loads(snapshot["columns"])
    assert columns[0]["name"] == "id"
    assert columns[0]["nullable"] is False
