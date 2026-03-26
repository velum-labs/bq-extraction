from __future__ import annotations

import json

import networkx as nx

from bq_extraction.full_graph_export import (
    flatten_graph_attributes,
    to_asset_only_multidigraph,
    to_external_multidigraph,
    to_logical_asset_only_multidigraph,
    write_full_graph_exports,
)
from bq_extraction.lineage import LineageEdge, LineageGraph, LineageNode, build_lineage_graph

from .test_lineage import make_runs


def test_flatten_graph_attributes_preserves_json_and_scalars() -> None:
    flattened = flatten_graph_attributes(
        {
            "project_id": "proj-a",
            "query_count": 3,
            "cross_project": True,
            "provenance_sources": ["query_logs", "ddl"],
            "nested": {"dataset_id": "raw", "table_id": "orders"},
        }
    )

    assert flattened["meta_project_id"] == "proj-a"
    assert flattened["meta_query_count"] == 3
    assert flattened["meta_cross_project"] is True
    assert flattened["meta_provenance_sources"] == "query_logs|ddl"
    assert flattened["meta_nested_dataset_id"] == "raw"
    assert json.loads(flattened["meta_json"])["nested"]["table_id"] == "orders"


def test_to_external_multidigraph_and_write_full_graph_exports(tmp_path) -> None:
    graph = build_lineage_graph(make_runs())
    nx_graph = to_external_multidigraph(graph)
    asset_graph = to_asset_only_multidigraph(graph)

    assert isinstance(nx_graph, nx.MultiDiGraph)
    assert nx_graph.number_of_nodes() > 0
    assert nx_graph.number_of_edges() > 0
    assert set(data["node_type"] for _, data in asset_graph.nodes(data=True)) <= {
        "table",
        "view",
        "materialized_view",
    }
    assert set(data["edge_type"] for _, _, _, data in asset_graph.edges(keys=True, data=True)) <= {
        "writes",
        "view_depends_on",
    }

    first_edge = next(iter(nx_graph.edges(keys=True, data=True)))
    _, _, _, edge_data = first_edge
    assert "meta_json" in edge_data
    assert "edge_type" in edge_data

    artifacts = write_full_graph_exports(
        graph,
        output_dir=tmp_path,
        write_graphml=True,
        write_gexf=True,
        write_ndjson=True,
        write_chunked_json=True,
        chunk_size=2,
    )

    assert artifacts.graphml_path is not None and artifacts.graphml_path.exists()
    assert artifacts.gexf_path is not None and artifacts.gexf_path.exists()
    assert artifacts.asset_graphml_path is not None and artifacts.asset_graphml_path.exists()
    assert artifacts.asset_gexf_path is not None and artifacts.asset_gexf_path.exists()
    assert artifacts.logical_asset_graphml_path is not None and artifacts.logical_asset_graphml_path.exists()
    assert artifacts.logical_asset_gexf_path is not None and artifacts.logical_asset_gexf_path.exists()
    assert artifacts.nodes_ndjson_path is not None and artifacts.nodes_ndjson_path.exists()
    assert artifacts.edges_ndjson_path is not None and artifacts.edges_ndjson_path.exists()
    assert artifacts.nodes_chunk_dir is not None and artifacts.nodes_chunk_dir.exists()
    assert artifacts.edges_chunk_dir is not None and artifacts.edges_chunk_dir.exists()
    assert artifacts.manifest_path.exists()

    graphml_graph = nx.read_graphml(artifacts.graphml_path)
    assert len(graphml_graph.nodes) > 0
    asset_graphml_graph = nx.read_graphml(artifacts.asset_graphml_path)
    assert len(asset_graphml_graph.nodes) > 0
    logical_asset_graphml_graph = nx.read_graphml(artifacts.logical_asset_graphml_path)
    assert len(logical_asset_graphml_graph.nodes) > 0

    manifest = json.loads(artifacts.manifest_path.read_text())
    assert manifest["graph_stats"]["nodes"] == len(graph.nodes)
    assert manifest["graph_stats"]["edges"] == len(graph.edges)
    assert manifest["graph_stats"]["asset_only_nodes"] == asset_graph.number_of_nodes()
    assert manifest["graph_stats"]["asset_only_edges"] == asset_graph.number_of_edges()
    assert manifest["graph_stats"]["logical_asset_only_nodes"] == logical_asset_graphml_graph.number_of_nodes()
    assert manifest["graph_stats"]["collapsed_temp_source_nodes"] >= 0

    node_chunks = sorted(artifacts.nodes_chunk_dir.glob("*.json"))
    edge_chunks = sorted(artifacts.edges_chunk_dir.glob("*.json"))
    assert node_chunks
    assert edge_chunks


def test_to_logical_asset_only_multidigraph_collapses_temp_families() -> None:
    graph = LineageGraph(
        nodes=[
            LineageNode(
                id="proj.temp_incremental_tables.goal_orders_temp_100",
                node_type="table",
                name="goal_orders_temp_100",
                metadata={
                    "project_id": "proj",
                    "dataset_id": "temp_incremental_tables",
                    "table_id": "goal_orders_temp_100",
                },
            ),
            LineageNode(
                id="proj.temp_incremental_tables.goal_orders_temp_200",
                node_type="table",
                name="goal_orders_temp_200",
                metadata={
                    "project_id": "proj",
                    "dataset_id": "temp_incremental_tables",
                    "table_id": "goal_orders_temp_200",
                },
            ),
            LineageNode(
                id="proj.airflow_staging_multiregion.goal_orders",
                node_type="table",
                name="goal_orders",
                metadata={
                    "project_id": "proj",
                    "dataset_id": "airflow_staging_multiregion",
                    "table_id": "goal_orders",
                },
            ),
            LineageNode(
                id="proj.heroku_views.goal_orders",
                node_type="view",
                name="goal_orders",
                metadata={
                    "project_id": "proj",
                    "dataset_id": "heroku_views",
                    "table_id": "goal_orders",
                },
            ),
        ],
        edges=[
            LineageEdge(
                id="e1",
                source="proj.temp_incremental_tables.goal_orders_temp_100",
                target="proj.airflow_staging_multiregion.goal_orders",
                edge_type="writes",
                metadata={"query_count": 1, "provenance_sources": {"sql_heuristic"}},
            ),
            LineageEdge(
                id="e2",
                source="proj.temp_incremental_tables.goal_orders_temp_200",
                target="proj.airflow_staging_multiregion.goal_orders",
                edge_type="writes",
                metadata={"query_count": 1, "provenance_sources": {"sql_heuristic"}},
            ),
            LineageEdge(
                id="e3",
                source="proj.airflow_staging_multiregion.goal_orders",
                target="proj.heroku_views.goal_orders",
                edge_type="view_depends_on",
                metadata={"query_count": 1, "provenance_sources": {"ddl"}},
            ),
        ],
    )

    logical_graph = to_logical_asset_only_multidigraph(graph)

    assert "proj.temp_incremental_tables.goal_orders" in logical_graph.nodes
    assert "proj.temp_incremental_tables.goal_orders_temp_100" not in logical_graph.nodes
    assert "proj.temp_incremental_tables.goal_orders_temp_200" not in logical_graph.nodes

    node_data = logical_graph.nodes["proj.temp_incremental_tables.goal_orders"]
    assert node_data["meta_logical_node"] is True
    assert node_data["meta_collapsed_temp_count"] == 2
    assert "goal_orders_temp_100" in node_data["meta_collapsed_physical_ids"]

    edge_data = logical_graph.get_edge_data(
        "proj.temp_incremental_tables.goal_orders",
        "proj.airflow_staging_multiregion.goal_orders",
    )
    assert edge_data is not None
    only_edge = next(iter(edge_data.values()))
    assert only_edge["edge_type"] == "writes"
    assert only_edge["meta_query_count"] == 2.0
