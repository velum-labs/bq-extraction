from __future__ import annotations

from types import SimpleNamespace

from bq_extraction.lineage import (
    build_lineage_graph,
    extract_bigquery_tables,
    extract_lineage_subgraph,
    extract_write_target,
    to_networkx_digraph,
)


def make_runs() -> SimpleNamespace:
    return SimpleNamespace(
        datasets=[
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "dataset_id": "raw",
                "location": "US",
                "label_producer": "",
                "label_maturity": "",
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "dataset_id": "analytics",
                "location": "US",
                "label_producer": "copy-dag",
                "label_maturity": "contract",
            },
        ],
        tables=[
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "dataset_id": "raw",
                "table_id": "orders",
                "table_type": "TABLE",
                "location": "US",
                "num_bytes": 10,
                "num_rows": 3,
                "column_count": 2,
                "schema_fields": [
                    {"name": "id", "type": "INT64", "mode": "REQUIRED"},
                    {"name": "value", "type": "STRING", "mode": "NULLABLE"},
                ],
                "view_query": "",
                "view_use_legacy_sql": False,
                "label_maturity": "",
                "label_producer": "",
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "dataset_id": "analytics",
                "table_id": "orders_view",
                "table_type": "VIEW",
                "location": "US",
                "num_bytes": None,
                "num_rows": None,
                "column_count": 2,
                "schema_fields": [
                    {"name": "id", "type": "INT64", "mode": "REQUIRED"},
                    {"name": "value", "type": "STRING", "mode": "NULLABLE"},
                ],
                "view_query": (
                    "SELECT o.id, c.name "
                    "FROM `proj-a.raw.orders` o "
                    "JOIN `proj-b.shared.customers` c ON o.id = c.id"
                ),
                "view_use_legacy_sql": False,
                "label_maturity": "",
                "label_producer": "",
            },
        ],
        ddls=[
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "dataset": "analytics",
                "table_name": "accounts_view",
                "table_type": "VIEW",
                "ddl": (
                    "CREATE VIEW `proj-a.analytics.accounts_view` AS "
                    "SELECT * FROM `proj-a.raw.accounts`"
                ),
                "location": "US",
            }
        ],
        query_logs=[
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-1",
                "user_email": "analyst@example.com",
                "query": "SELECT * FROM `proj-a.raw.orders`",
                "statement_type": "SELECT",
                "creation_time": "2026-03-01T00:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 100,
                "total_bytes_billed": 100,
                "total_slot_ms": 10,
                "cache_hit": False,
                "query_source": "ad_hoc",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-a", "dataset_id": "raw", "table_id": "orders"}
                ],
                "is_probe": False,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-2",
                "user_email": "analyst@example.com",
                "query": (
                    "SELECT * FROM `proj-a.raw.orders` "
                    "JOIN `proj-b.shared.customers` USING(id)"
                ),
                "statement_type": "SELECT",
                "creation_time": "2026-03-02T00:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 50,
                "total_bytes_billed": 50,
                "total_slot_ms": 5,
                "cache_hit": False,
                "query_source": "ad_hoc",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-a", "dataset_id": "raw", "table_id": "orders"},
                    {"project_id": "proj-b", "dataset_id": "shared", "table_id": "customers"},
                ],
                "is_probe": False,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-probe",
                "user_email": "analyst@example.com",
                "query": "SELECT * FROM `proj-a`.`region-us`.INFORMATION_SCHEMA.TABLES",
                "statement_type": "SELECT",
                "creation_time": "2026-03-02T00:00:01",
                "duration_seconds": 1,
                "total_bytes_processed": 1,
                "total_bytes_billed": 1,
                "total_slot_ms": 1,
                "cache_hit": False,
                "query_source": "ad_hoc",
                "location": "US",
                "referenced_tables": [
                    {
                        "project_id": "proj-a",
                        "dataset_id": "region-us",
                        "table_id": "INFORMATION_SCHEMA.TABLES",
                    }
                ],
                "is_probe": True,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-3",
                "user_email": "writer@example.com",
                "query": (
                    "INSERT INTO `proj-a.analytics.daily_orders` "
                    "SELECT * FROM `proj-a.raw.orders`"
                ),
                "statement_type": "INSERT",
                "creation_time": "2026-03-03T00:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 30,
                "total_bytes_billed": 30,
                "total_slot_ms": 3,
                "cache_hit": False,
                "query_source": "service_account",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-a", "dataset_id": "raw", "table_id": "orders"}
                ],
                "is_probe": False,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-4",
                "user_email": "writer@example.com",
                "query": (
                    "CREATE OR REPLACE TABLE `proj-a.analytics.daily_customers` "
                    "AS SELECT * FROM `proj-b.shared.customers`"
                ),
                "statement_type": "CREATE_TABLE_AS_SELECT",
                "creation_time": "2026-03-03T01:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 60,
                "total_bytes_billed": 60,
                "total_slot_ms": 6,
                "cache_hit": False,
                "query_source": "service_account",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-b", "dataset_id": "shared", "table_id": "customers"}
                ],
                "is_probe": False,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-5",
                "user_email": "writer@example.com",
                "query": (
                    "MERGE `proj-a.analytics.customer_dim` t "
                    "USING `proj-b.shared.customers` s "
                    "ON t.id = s.id "
                    "WHEN MATCHED THEN UPDATE SET name = s.name"
                ),
                "statement_type": "MERGE",
                "creation_time": "2026-03-03T02:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 90,
                "total_bytes_billed": 90,
                "total_slot_ms": 9,
                "cache_hit": False,
                "query_source": "service_account",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-b", "dataset_id": "shared", "table_id": "customers"},
                    {"project_id": "proj-a", "dataset_id": "analytics", "table_id": "customer_dim"},
                ],
                "is_probe": False,
            },
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "job_id": "job-6",
                "user_email": "writer@example.com",
                "query": "CREATE TABLE AS SELECT * FROM `proj-a.raw.orders`",
                "statement_type": "CREATE_TABLE_AS_SELECT",
                "creation_time": "2026-03-03T03:00:00",
                "duration_seconds": 1,
                "total_bytes_processed": 10,
                "total_bytes_billed": 10,
                "total_slot_ms": 1,
                "cache_hit": False,
                "query_source": "service_account",
                "location": "US",
                "referenced_tables": [
                    {"project_id": "proj-a", "dataset_id": "raw", "table_id": "orders"}
                ],
                "is_probe": False,
            },
        ],
        frequent_queries=[
            {
                "run_id": "run-a",
                "project_id": "proj-a",
                "query_hash": "hash-1",
                "execution_count": 12,
                "sample_query": (
                    "CREATE OR REPLACE TABLE `proj-a.analytics.daily_customers` "
                    "AS SELECT * FROM `proj-b.shared.customers`"
                ),
                "users": ["writer@example.com"],
                "avg_bytes": 25,
                "avg_slot_ms": 7,
                "location": "US",
            }
        ],
        table_access=[],
        user_stats=[],
    )


def test_extract_bigquery_tables_and_write_targets() -> None:
    refs = extract_bigquery_tables(
        "WITH cte AS (SELECT * FROM `proj.raw.orders`) SELECT * FROM cte JOIN `proj.shared.customers` USING(id)",
        default_project_id="proj",
    )
    assert [ref.full_name for ref in refs] == ["proj.raw.orders", "proj.shared.customers"]

    assert (
        extract_write_target(
            "INSERT INTO `proj.analytics.daily_orders` SELECT * FROM `proj.raw.orders`",
            default_project_id="proj",
        ).full_name
        == "proj.analytics.daily_orders"
    )
    assert (
        extract_write_target(
            "CREATE OR REPLACE TABLE `proj.analytics.daily_orders` AS SELECT * FROM `proj.raw.orders`",
            default_project_id="proj",
        ).full_name
        == "proj.analytics.daily_orders"
    )
    assert (
        extract_write_target(
            "MERGE `proj.analytics.daily_orders` t USING `proj.raw.orders` s ON t.id = s.id WHEN MATCHED THEN UPDATE SET id = s.id",
            default_project_id="proj",
        ).full_name
        == "proj.analytics.daily_orders"
    )


def test_build_lineage_graph_aggregates_reads_and_cross_project_edges() -> None:
    graph = build_lineage_graph(make_runs())
    edge_map = {(edge.source, edge.target, edge.edge_type): edge for edge in graph.edges}

    local_read = edge_map[("proj-a.raw.orders", "consumer:analyst@example.com", "reads")]
    assert local_read.metadata["query_count"] == 2.0
    assert local_read.metadata["cross_project"] is False
    assert local_read.metadata["total_bytes_processed"] == 150.0

    external_read = edge_map[("proj-b.shared.customers", "consumer:analyst@example.com", "reads")]
    assert external_read.metadata["query_count"] == 1.0
    assert external_read.metadata["cross_project"] is True

    node_ids = {node.id for node in graph.nodes}
    assert "proj-a.raw.orders" in node_ids
    assert "proj-b.shared.customers" in node_ids
    assert "consumer:analyst@example.com" in node_ids
    assert "proj-b.shared" in node_ids


def test_build_lineage_graph_includes_write_heuristics_and_issues() -> None:
    graph = build_lineage_graph(make_runs())
    edge_map = {(edge.source, edge.target, edge.edge_type): edge for edge in graph.edges}

    assert ("proj-a.raw.orders", "proj-a.analytics.daily_orders", "writes") in edge_map
    assert ("proj-b.shared.customers", "proj-a.analytics.daily_customers", "writes") in edge_map
    assert ("proj-b.shared.customers", "proj-a.analytics.customer_dim", "writes") in edge_map

    merge_edge = edge_map[("proj-b.shared.customers", "proj-a.analytics.customer_dim", "writes")]
    assert merge_edge.metadata["cross_project"] is True
    assert merge_edge.metadata["statement_type"] == "MERGE"

    issue_types = {issue.issue_type for issue in graph.issues}
    assert "unresolved_write_target" in issue_types


def test_build_lineage_graph_derives_view_dependencies_from_view_sql_and_ddl() -> None:
    graph = build_lineage_graph(make_runs())
    edge_map = {(edge.source, edge.target, edge.edge_type): edge for edge in graph.edges}

    view_edge = edge_map[("proj-a.raw.orders", "proj-a.analytics.orders_view", "view_depends_on")]
    assert "view_query" in view_edge.metadata["provenance_sources"]
    assert view_edge.metadata["cross_project"] is False

    cross_project_view = edge_map[("proj-b.shared.customers", "proj-a.analytics.orders_view", "view_depends_on")]
    assert cross_project_view.metadata["cross_project"] is True

    ddl_edge = edge_map[("proj-a.raw.accounts", "proj-a.analytics.accounts_view", "view_depends_on")]
    assert "ddl" in ddl_edge.metadata["provenance_sources"]


def test_build_lineage_graph_builds_grouped_query_patterns() -> None:
    graph = build_lineage_graph(make_runs())
    assert len(graph.queries) == 1

    query = graph.queries[0]
    assert query.fingerprint == "hash-1"
    assert query.execution_count == 12
    assert query.source_asset_ids == ["proj-b.shared.customers"]
    assert query.metadata["target_asset_id"] == "proj-a.analytics.daily_customers"


def test_to_networkx_digraph_and_edge_type_filtering() -> None:
    graph = build_lineage_graph(make_runs())
    nx_graph = to_networkx_digraph(graph, edge_types=["reads"])

    assert nx_graph.has_node("proj-a.raw.orders")
    assert nx_graph.has_node("consumer:analyst@example.com")
    assert all(data["edge_type"] == "reads" for _, _, data in nx_graph.edges(data=True))


def test_extract_lineage_subgraph_for_consumer_reads() -> None:
    graph = build_lineage_graph(make_runs())
    subgraph = extract_lineage_subgraph(
        graph,
        consumer="analyst@example.com",
        edge_types=["reads"],
        hop_depth=1,
        max_nodes=10,
    )

    assert subgraph.truncated is False
    assert set(subgraph.seed_node_ids) == {"consumer:analyst@example.com"}

    node_ids = set(subgraph.graph.nodes)
    assert "consumer:analyst@example.com" in node_ids
    assert "proj-a.raw.orders" in node_ids
    assert "proj-b.shared.customers" in node_ids

    edge_ids = {(u, v, data["edge_type"]) for u, v, data in subgraph.graph.edges(data=True)}
    assert ("proj-a.raw.orders", "consumer:analyst@example.com", "reads") in edge_ids
    assert ("proj-b.shared.customers", "consumer:analyst@example.com", "reads") in edge_ids


def test_extract_lineage_subgraph_for_dataset_relations() -> None:
    graph = build_lineage_graph(make_runs())
    subgraph = extract_lineage_subgraph(
        graph,
        dataset_id="proj-a.analytics",
        edge_types=["writes", "view_depends_on"],
        hop_depth=1,
        max_nodes=20,
    )

    node_ids = set(subgraph.graph.nodes)
    assert "proj-a.analytics" in node_ids
    assert "proj-a.analytics.orders_view" in node_ids
    assert "proj-a.analytics.daily_orders" in node_ids

    edge_ids = {(u, v, data["edge_type"]) for u, v, data in subgraph.graph.edges(data=True)}
    assert ("proj-a.raw.orders", "proj-a.analytics.daily_orders", "writes") in edge_ids
    assert ("proj-a.raw.orders", "proj-a.analytics.orders_view", "view_depends_on") in edge_ids


def test_extract_lineage_subgraph_trims_deterministically() -> None:
    graph = build_lineage_graph(make_runs())
    subgraph = extract_lineage_subgraph(
        graph,
        project_id="proj-a",
        edge_types=["reads", "writes", "view_depends_on"],
        hop_depth=2,
        max_nodes=4,
    )
    subgraph_again = extract_lineage_subgraph(
        graph,
        project_id="proj-a",
        edge_types=["reads", "writes", "view_depends_on"],
        hop_depth=2,
        max_nodes=4,
    )

    assert subgraph.truncated is True
    assert subgraph.omitted_nodes > 0
    assert len(subgraph.graph.nodes) == 4
    assert set(subgraph.graph.nodes) == set(subgraph_again.graph.nodes)
    assert "proj-a.raw.orders" in subgraph.graph.nodes
