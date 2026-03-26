"""Offline lineage graph construction for bq-extraction results.

Builds a neutral lineage graph from extracted BigQuery metadata and job history,
then exposes Atlas-friendly metadata for later export.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping

import sqlglot
from sqlglot import exp

__all__ = [
    "EvidenceOverlay",
    "GraphBundle",
    "LineageEdge",
    "LineageGraph",
    "LineageIssue",
    "LineageNode",
    "LineageQuery",
    "LineageSubgraph",
    "Provenance",
    "SqlTableRef",
    "asset_node_id",
    "atlas_query_fingerprint",
    "build_lineage_graph",
    "consumer_node_id",
    "dataset_node_id",
    "extract_bigquery_tables",
    "extract_lineage_subgraph",
    "extract_write_target",
    "query_pattern_node_id",
    "to_networkx_digraph",
]


@dataclass(frozen=True)
class LineageNode:
    """A neutral lineage graph node."""

    id: str
    node_type: str
    name: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageEdge:
    """A neutral lineage graph edge."""

    id: str
    source: str
    target: str
    edge_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageQuery:
    """A grouped query pattern suitable for overlay or export use."""

    id: str
    fingerprint: str
    project_id: str
    sample_sql: str
    source_asset_ids: list[str]
    execution_count: int
    user_emails: list[str]
    avg_bytes: float | None = None
    avg_slot_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class LineageIssue:
    """A lineage extraction issue that should remain auditable."""

    issue_type: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Provenance:
    """Provenance for graph bundles and evidence overlays."""

    source_adapter: str
    extracted_at: str
    source_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class EvidenceOverlay:
    """Behavioral or analytical data attached to a lineage graph."""

    overlay_type: str
    entries: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)
    provenance: Provenance | None = None


@dataclass
class LineageGraph:
    """A lineage graph plus exportable query patterns and issues."""

    nodes: list[LineageNode]
    edges: list[LineageEdge]
    queries: list[LineageQuery] = field(default_factory=list)
    issues: list[LineageIssue] = field(default_factory=list)

    def node_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": node.id,
                "node_type": node.node_type,
                "name": node.name,
                "metadata": node.metadata,
            }
            for node in self.nodes
        ]

    def edge_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": edge.id,
                "source": edge.source,
                "target": edge.target,
                "edge_type": edge.edge_type,
                "metadata": edge.metadata,
            }
            for edge in self.edges
        ]

    def query_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": query.id,
                "fingerprint": query.fingerprint,
                "project_id": query.project_id,
                "sample_sql": query.sample_sql,
                "source_asset_ids": query.source_asset_ids,
                "execution_count": query.execution_count,
                "user_emails": query.user_emails,
                "avg_bytes": query.avg_bytes,
                "avg_slot_ms": query.avg_slot_ms,
                "metadata": query.metadata,
            }
            for query in self.queries
        ]

    def issue_records(self) -> list[dict[str, Any]]:
        return [
            {
                "issue_type": issue.issue_type,
                "metadata": issue.metadata,
            }
            for issue in self.issues
        ]

    def to_dataframes(self) -> tuple[Any, Any, Any, Any]:
        import pandas as pd

        return (
            pd.DataFrame(self.node_records()),
            pd.DataFrame(self.edge_records()),
            pd.DataFrame(self.query_records()),
            pd.DataFrame(self.issue_records()),
        )

    def write_json(self, output_dir: str) -> None:
        from pathlib import Path

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        payloads = {
            "lineage_nodes.json": self.node_records(),
            "lineage_edges.json": self.edge_records(),
            "lineage_queries.json": self.query_records(),
            "lineage_issues.json": self.issue_records(),
        }
        for filename, payload in payloads.items():
            (output_path / filename).write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )


@dataclass(frozen=True)
class LineageSubgraph:
    """A filtered, bounded lineage subgraph for interactive visualization."""

    graph: Any
    seed_node_ids: list[str]
    node_limit: int
    truncated: bool = False
    omitted_nodes: int = 0
    filters: dict[str, Any] = field(default_factory=dict)

    def node_records(self) -> list[dict[str, Any]]:
        return [
            {
                "id": node_id,
                **dict(self.graph.nodes[node_id]),
                "degree": int(self.graph.degree(node_id)),
                "in_degree": int(self.graph.in_degree(node_id)),
                "out_degree": int(self.graph.out_degree(node_id)),
            }
            for node_id in sorted(self.graph.nodes)
        ]

    def edge_records(self) -> list[dict[str, Any]]:
        return [
            {
                "source": source,
                "target": target,
                **dict(data),
            }
            for source, target, data in sorted(self.graph.edges(data=True))
        ]

    def to_dataframes(self) -> tuple[Any, Any]:
        import pandas as pd

        return pd.DataFrame(self.node_records()), pd.DataFrame(self.edge_records())


@dataclass(frozen=True)
class GraphBundle:
    """Structural lineage graph plus overlays and provenance."""

    graph: LineageGraph
    overlays: list[EvidenceOverlay] = field(default_factory=list)
    provenance: Provenance | None = None
    transforms_applied: list[str] = field(default_factory=list)


def dataset_node_id(project_id: str, dataset_id: str) -> str:
    return f"{project_id}.{dataset_id}"


def asset_node_id(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


def consumer_node_id(user_email: str) -> str:
    return f"consumer:{(user_email or 'unknown').lower()}"


def query_pattern_node_id(project_id: str, fingerprint: str) -> str:
    return f"query:{project_id}:{fingerprint}"

_SKIP_NAMES: frozenset[str] = frozenset(
    {
        "select",
        "where",
        "and",
        "or",
        "on",
        "as",
        "set",
        "values",
        "into",
        "limit",
        "order",
        "group",
        "having",
        "case",
        "when",
        "then",
        "else",
        "end",
        "not",
        "null",
        "true",
        "false",
        "is",
        "in",
        "between",
        "like",
        "exists",
        "all",
        "any",
        "some",
        "lateral",
        "unnest",
        "generate_series",
        "dual",
    }
)
_SKIP_SCHEMAS: frozenset[str] = frozenset({"information_schema"})
_WRITE_PREFIX_RE = re.compile(r"^\s*(INSERT|CREATE|MERGE)\b", re.IGNORECASE)
_BIGQUERY_TABLE_RE = re.compile(
    r"(?:FROM|JOIN|USING)\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?",
    re.IGNORECASE,
)
_INSERT_TARGET_RE = re.compile(
    r"INSERT\s+INTO\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?",
    re.IGNORECASE,
)
_CREATE_TARGET_RE = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+TABLE\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?",
    re.IGNORECASE,
)
_CREATE_VIEW_TARGET_RE = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+(?:MATERIALIZED\s+)?VIEW\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?",
    re.IGNORECASE,
)
_MERGE_TARGET_RE = re.compile(
    r"MERGE(?:\s+INTO)?\s+`?(?P<name>[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+){0,2})`?",
    re.IGNORECASE,
)
_SET_FIELDS: frozenset[str] = frozenset(
    {
        "run_ids",
        "projects",
        "observed_projects",
        "locations",
        "query_sources",
        "statement_types",
        "provenance_sources",
        "user_emails",
        "sample_queries",
    }
)
_SUM_FIELDS: frozenset[str] = frozenset(
    {
        "query_count",
        "execution_count",
        "total_bytes_processed",
        "total_bytes_billed",
        "total_slot_ms",
    }
)
_MIN_FIELDS: frozenset[str] = frozenset({"first_seen"})
_MAX_FIELDS: frozenset[str] = frozenset({"last_seen"})
_BOOL_OR_FIELDS: frozenset[str] = frozenset({"cross_project", "unresolved_write_target"})


@dataclass(frozen=True)
class SqlTableRef:
    """A resolved BigQuery table reference."""

    project_id: str
    dataset_id: str
    table_id: str

    @property
    def full_name(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"


def build_lineage_graph(runs: object, *, include_query_patterns: bool = True) -> LineageGraph:
    """Build a neutral lineage graph from extracted run artifacts."""

    builder = _LineageGraphBuilder()
    _add_inventory_nodes(builder, runs)
    _add_read_lineage(builder, runs)
    _add_write_lineage(builder, runs)
    _add_view_lineage(builder, runs)
    queries = _build_query_patterns(runs) if include_query_patterns else []
    return builder.build(queries)


def to_networkx_digraph(
    lineage_graph: LineageGraph,
    *,
    edge_types: list[str] | None = None,
) -> Any:
    """Convert a lineage graph into a `networkx.DiGraph`."""

    import networkx as nx

    allowed_edge_types = set(edge_types or [])
    graph = nx.DiGraph()

    for node in lineage_graph.nodes:
        graph.add_node(
            node.id,
            node_type=node.node_type,
            name=node.name,
            metadata=node.metadata,
        )

    for edge in lineage_graph.edges:
        if allowed_edge_types and edge.edge_type not in allowed_edge_types:
            continue
        graph.add_edge(
            edge.source,
            edge.target,
            id=edge.id,
            edge_type=edge.edge_type,
            metadata=edge.metadata,
        )

    return graph


def extract_lineage_subgraph(
    lineage_graph: LineageGraph,
    *,
    focus_node_ids: list[str] | None = None,
    project_id: str | None = None,
    dataset_id: str | None = None,
    consumer: str | None = None,
    asset_id: str | None = None,
    edge_types: list[str] | None = None,
    hop_depth: int = 1,
    max_nodes: int = 120,
) -> LineageSubgraph:
    """Build a filtered lineage subgraph around a focused neighborhood."""

    import networkx as nx

    if hop_depth < 0:
        raise ValueError("hop_depth must be >= 0")
    if max_nodes <= 0:
        raise ValueError("max_nodes must be > 0")

    graph = to_networkx_digraph(lineage_graph, edge_types=edge_types)
    seed_ids = _resolve_seed_nodes(
        graph,
        focus_node_ids=focus_node_ids or [],
        project_id=project_id,
        dataset_id=dataset_id,
        consumer=consumer,
        asset_id=asset_id,
    )
    if not seed_ids:
        return LineageSubgraph(
            graph=graph.__class__(),
            seed_node_ids=[],
            node_limit=max_nodes,
            truncated=False,
            omitted_nodes=0,
            filters={
                "project_id": project_id or "",
                "dataset_id": dataset_id or "",
                "consumer": consumer or "",
                "asset_id": asset_id or "",
                "edge_types": edge_types or [],
                "hop_depth": hop_depth,
            },
        )

    candidate_nodes = set(seed_ids)
    frontier = set(seed_ids)
    for _ in range(hop_depth):
        next_frontier: set[str] = set()
        for node_id in frontier:
            if node_id not in graph:
                continue
            next_frontier.update(graph.predecessors(node_id))
            next_frontier.update(graph.successors(node_id))
        next_frontier -= candidate_nodes
        candidate_nodes.update(next_frontier)
        frontier = next_frontier

    truncated = False
    omitted_nodes = 0
    if len(candidate_nodes) > max_nodes:
        ranked_nodes = sorted(
            candidate_nodes,
            key=lambda node_id: _subgraph_rank_key(graph, node_id, seed_ids),
        )
        kept_nodes = set(ranked_nodes[:max_nodes])
        omitted_nodes = len(candidate_nodes) - len(kept_nodes)
        candidate_nodes = kept_nodes
        truncated = True

    subgraph = nx.DiGraph(graph.subgraph(candidate_nodes).copy())
    return LineageSubgraph(
        graph=subgraph,
        seed_node_ids=sorted(seed_ids),
        node_limit=max_nodes,
        truncated=truncated,
        omitted_nodes=omitted_nodes,
        filters={
            "project_id": project_id or "",
            "dataset_id": dataset_id or "",
            "consumer": consumer or "",
            "asset_id": asset_id or "",
            "edge_types": edge_types or [],
            "hop_depth": hop_depth,
        },
    )


def extract_bigquery_tables(
    sql: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
) -> list[SqlTableRef]:
    """Extract source table references from BigQuery SQL."""

    if not sql or not sql.strip():
        return []

    try:
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return _regex_extract_bigquery(
            sql,
            default_project_id=default_project_id,
            default_dataset_id=default_dataset_id,
        )

    cte_aliases = _collect_cte_aliases(parsed)
    refs: dict[str, SqlTableRef] = {}
    for table_expr in parsed.find_all(exp.Table):
        ref = _table_ref_from_expression(
            table_expr,
            default_project_id=default_project_id,
            default_dataset_id=default_dataset_id,
        )
        if ref is None:
            continue
        if ref.table_id.lower() in cte_aliases:
            continue
        refs[ref.full_name.lower()] = ref
    return [refs[key] for key in sorted(refs)]


def extract_write_target(
    sql: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
) -> SqlTableRef | None:
    """Extract a write target from BigQuery SQL when one is declared explicitly."""

    if not sql or not sql.strip():
        return None

    try:
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return _regex_extract_declared_target(
            sql,
            default_project_id=default_project_id,
            default_dataset_id=default_dataset_id,
            include_views=False,
        )
    return _extract_declared_target_from_ast(
        parsed,
        default_project_id=default_project_id,
        default_dataset_id=default_dataset_id,
        include_views=False,
    )


def atlas_query_fingerprint(sql: str) -> str:
    """Return the Atlas-style normalized SQL fingerprint."""

    normalized = re.sub(r"\s+", " ", sql.strip().lower())
    return hashlib.sha256(normalized.encode()).hexdigest()[:16]


def _extract_declared_target_from_sql(
    sql: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
    include_views: bool,
) -> SqlTableRef | None:
    if not sql or not sql.strip():
        return None

    try:
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
    except (sqlglot.errors.ParseError, sqlglot.errors.TokenError):
        return _regex_extract_declared_target(
            sql,
            default_project_id=default_project_id,
            default_dataset_id=default_dataset_id,
            include_views=include_views,
        )
    return _extract_declared_target_from_ast(
        parsed,
        default_project_id=default_project_id,
        default_dataset_id=default_dataset_id,
        include_views=include_views,
    )


def _extract_declared_target_from_ast(
    parsed: exp.Expression,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
    include_views: bool,
) -> SqlTableRef | None:
    target_expr: exp.Table | None = None
    if isinstance(parsed, exp.Insert):
        target_expr = parsed.this if isinstance(parsed.this, exp.Table) else parsed.find(exp.Table)
    elif isinstance(parsed, exp.Create):
        kind = str(parsed.args.get("kind") or "").upper()
        if kind == "TABLE" or (include_views and kind in {"VIEW", "MATERIALIZED VIEW"}):
            target_expr = parsed.this if isinstance(parsed.this, exp.Table) else parsed.find(exp.Table)
    elif isinstance(parsed, exp.Merge):
        target_expr = parsed.this if isinstance(parsed.this, exp.Table) else parsed.find(exp.Table)

    if target_expr is None:
        return None
    return _table_ref_from_expression(
        target_expr,
        default_project_id=default_project_id,
        default_dataset_id=default_dataset_id,
    )


class _LineageGraphBuilder:
    def __init__(self) -> None:
        self._nodes: dict[str, LineageNode] = {}
        self._edges: dict[tuple[str, str, str], dict[str, Any]] = {}
        self._issues: list[LineageIssue] = []

    def add_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        if node_id in self._nodes:
            existing = self._nodes[node_id]
            merged = _merge_metadata(existing.metadata, metadata or {})
            self._nodes[node_id] = LineageNode(
                id=existing.id,
                node_type=existing.node_type,
                name=existing.name,
                metadata=merged,
            )
            return
        self._nodes[node_id] = LineageNode(
            id=node_id,
            node_type=node_type,
            name=name,
            metadata=dict(metadata or {}),
        )

    def add_edge(
        self,
        source: str,
        target: str,
        edge_type: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        key = (source, target, edge_type)
        if key in self._edges:
            self._edges[key] = _merge_metadata(self._edges[key], metadata or {})
            return
        self._edges[key] = dict(metadata or {})

    def add_issue(self, issue_type: str, metadata: Mapping[str, Any] | None = None) -> None:
        self._issues.append(LineageIssue(issue_type=issue_type, metadata=dict(metadata or {})))

    def build(self, queries: list[LineageQuery]) -> LineageGraph:
        edges = [
            LineageEdge(
                id=f"{source}:{target}:{edge_type}",
                source=source,
                target=target,
                edge_type=edge_type,
                metadata=_normalize_metadata(metadata),
            )
            for (source, target, edge_type), metadata in sorted(self._edges.items())
        ]
        nodes = [self._nodes[key] for key in sorted(self._nodes)]
        return LineageGraph(nodes=nodes, edges=edges, queries=queries, issues=self._issues)


def _add_inventory_nodes(builder: _LineageGraphBuilder, runs: object) -> None:
    for row in _records(getattr(runs, "datasets", None)):
        project_id = _str_value(row.get("project_id"))
        dataset_id = _str_value(row.get("dataset_id"))
        if not project_id or not dataset_id:
            continue
        builder.add_node(
            dataset_node_id(project_id, dataset_id),
            "dataset",
            dataset_id,
            {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "location": _str_value(row.get("location")),
                "label_producer": _str_value(row.get("label_producer")),
                "label_maturity": _str_value(row.get("label_maturity")),
                "labels_raw": row.get("labels_raw"),
            },
        )

    for row in _records(getattr(runs, "tables", None)):
        project_id = _str_value(row.get("project_id"))
        dataset_id = _str_value(row.get("dataset_id"))
        table_id = _str_value(row.get("table_id"))
        if not project_id or not dataset_id or not table_id:
            continue
        builder.add_node(
            dataset_node_id(project_id, dataset_id),
            "dataset",
            dataset_id,
            {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "location": _str_value(row.get("location")),
            },
        )
        builder.add_node(
            asset_node_id(project_id, dataset_id, table_id),
            _node_type_from_table_type(_str_value(row.get("table_type"))),
            table_id,
            {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "table_type": _str_value(row.get("table_type")),
                "location": _str_value(row.get("location")),
                "num_bytes": row.get("num_bytes"),
                "num_rows": row.get("num_rows"),
                "column_count": row.get("column_count"),
                "schema_fields": row.get("schema_fields"),
                "view_query": _str_value(row.get("view_query")),
                "view_use_legacy_sql": row.get("view_use_legacy_sql"),
                "label_maturity": _str_value(row.get("label_maturity")),
                "label_producer": _str_value(row.get("label_producer")),
            },
        )


def _add_read_lineage(builder: _LineageGraphBuilder, runs: object) -> None:
    query_logs = _records(getattr(runs, "query_logs", None))
    for row in query_logs:
        if row.get("is_probe"):
            continue

        consumer_email = _str_value(row.get("user_email")) or "unknown"
        consumer_id = consumer_node_id(consumer_email)
        builder.add_node(
            consumer_id,
            "consumer",
            consumer_email,
            {
                "consumer_kind": _consumer_kind(consumer_email),
                "user_email": consumer_email,
                "project_id": _str_value(row.get("project_id")),
            },
        )

        seen_refs: set[str] = set()
        for ref in row.get("referenced_tables") or []:
            ref_obj = _sql_ref_from_mapping(ref, default_project_id=_str_value(row.get("project_id")))
            if ref_obj is None or _is_system_ref(ref_obj):
                continue
            if ref_obj.full_name in seen_refs:
                continue
            seen_refs.add(ref_obj.full_name)

            _ensure_asset_nodes(builder, ref_obj, discovered_via="query_logs")
            builder.add_edge(
                ref_obj.full_name,
                consumer_id,
                "reads",
                {
                    "run_ids": {_str_value(row.get("run_id"))},
                    "projects": {_str_value(row.get("project_id")), ref_obj.project_id},
                    "observed_projects": {_str_value(row.get("project_id"))},
                    "locations": {_str_value(row.get("location"))},
                    "query_sources": {_str_value(row.get("query_source"))},
                    "statement_types": {_str_value(row.get("statement_type"))},
                    "user_emails": {consumer_email},
                    "query_count": 1,
                    "first_seen": _timestamp_string(row.get("creation_time")),
                    "last_seen": _timestamp_string(row.get("creation_time")),
                    "total_bytes_processed": _float_value(row.get("total_bytes_processed")) or 0.0,
                    "total_bytes_billed": _float_value(row.get("total_bytes_billed")) or 0.0,
                    "total_slot_ms": _float_value(row.get("total_slot_ms")) or 0.0,
                    "cross_project": ref_obj.project_id != _str_value(row.get("project_id")),
                    "provenance_sources": {"query_logs"},
                    "sample_queries": {_compact_sql(_str_value(row.get("query")))},
                    "statement_type": _str_value(row.get("statement_type")),
                },
            )


def _add_write_lineage(builder: _LineageGraphBuilder, runs: object) -> None:
    query_logs = _records(getattr(runs, "query_logs", None))
    for row in query_logs:
        if row.get("is_probe"):
            continue

        sql = _str_value(row.get("query"))
        project_id = _str_value(row.get("project_id"))
        target = extract_write_target(sql, default_project_id=project_id)
        if target is None:
            if _looks_like_write_statement(_str_value(row.get("statement_type")), sql):
                builder.add_issue(
                    "unresolved_write_target",
                    {
                        "run_id": _str_value(row.get("run_id")),
                        "project_id": project_id,
                        "job_id": _str_value(row.get("job_id")),
                        "statement_type": _str_value(row.get("statement_type")),
                        "user_email": _str_value(row.get("user_email")),
                        "query_source": _str_value(row.get("query_source")),
                        "query_preview": _compact_sql(sql),
                        "unresolved_write_target": True,
                    },
                )
            continue

        _ensure_asset_nodes(builder, target, discovered_via="write_target")

        source_refs = [
            ref
            for ref in _iter_query_sources(
                row.get("referenced_tables") or [],
                default_project_id=project_id,
            )
            if ref.full_name != target.full_name and not _is_system_ref(ref)
        ]
        if not source_refs:
            source_refs = [
                ref
                for ref in extract_bigquery_tables(sql, default_project_id=project_id)
                if ref.full_name != target.full_name and not _is_system_ref(ref)
            ]

        if not source_refs:
            builder.add_issue(
                "write_target_without_sources",
                {
                    "run_id": _str_value(row.get("run_id")),
                    "project_id": project_id,
                    "job_id": _str_value(row.get("job_id")),
                    "statement_type": _str_value(row.get("statement_type")),
                    "target_asset_id": target.full_name,
                    "query_preview": _compact_sql(sql),
                },
            )
            continue

        for ref_obj in source_refs:
            _ensure_asset_nodes(builder, ref_obj, discovered_via="write_source")
            builder.add_edge(
                ref_obj.full_name,
                target.full_name,
                "writes",
                {
                    "run_ids": {_str_value(row.get("run_id"))},
                    "projects": {project_id, ref_obj.project_id, target.project_id},
                    "observed_projects": {project_id},
                    "locations": {_str_value(row.get("location"))},
                    "query_sources": {_str_value(row.get("query_source"))},
                    "statement_types": {_str_value(row.get("statement_type"))},
                    "user_emails": {_str_value(row.get("user_email")) or "unknown"},
                    "query_count": 1,
                    "first_seen": _timestamp_string(row.get("creation_time")),
                    "last_seen": _timestamp_string(row.get("creation_time")),
                    "total_bytes_processed": _float_value(row.get("total_bytes_processed")) or 0.0,
                    "total_bytes_billed": _float_value(row.get("total_bytes_billed")) or 0.0,
                    "total_slot_ms": _float_value(row.get("total_slot_ms")) or 0.0,
                    "cross_project": ref_obj.project_id != target.project_id,
                    "provenance_sources": {"sql_heuristic"},
                    "sample_queries": {_compact_sql(sql)},
                    "target_asset_id": target.full_name,
                    "statement_type": _str_value(row.get("statement_type")),
                },
            )


def _add_view_lineage(builder: _LineageGraphBuilder, runs: object) -> None:
    tables = _records(getattr(runs, "tables", None))
    for row in tables:
        table_type = _str_value(row.get("table_type")).upper()
        if table_type not in {"VIEW", "MATERIALIZED VIEW"}:
            continue
        project_id = _str_value(row.get("project_id"))
        dataset_id = _str_value(row.get("dataset_id"))
        table_id = _str_value(row.get("table_id"))
        if not project_id or not dataset_id or not table_id:
            continue
        sql = _str_value(row.get("view_query"))
        if not sql:
            continue
        target = SqlTableRef(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        for source in extract_bigquery_tables(
            sql,
            default_project_id=project_id,
            default_dataset_id=dataset_id,
        ):
            if source.full_name == target.full_name or _is_system_ref(source):
                continue
            _ensure_asset_nodes(builder, source, discovered_via="view_query")
            builder.add_edge(
                source.full_name,
                target.full_name,
                "view_depends_on",
                {
                    "run_ids": {_str_value(row.get("run_id"))},
                    "projects": {project_id, source.project_id},
                    "observed_projects": {project_id},
                    "locations": {_str_value(row.get("location"))},
                    "provenance_sources": {"view_query"},
                    "sample_queries": {_compact_sql(sql)},
                    "query_count": 1,
                    "cross_project": source.project_id != project_id,
                },
            )

    ddls = _records(getattr(runs, "ddls", None))
    for row in ddls:
        table_type = _str_value(row.get("table_type")).upper()
        if "VIEW" not in table_type:
            continue
        project_id = _str_value(row.get("project_id"))
        dataset_id = _str_value(row.get("dataset"))
        table_id = _str_value(row.get("table_name"))
        sql = _str_value(row.get("ddl"))
        if not project_id or not dataset_id or not table_id or not sql:
            continue
        target = SqlTableRef(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        _ensure_asset_nodes(builder, target, discovered_via="ddl")
        for source in extract_bigquery_tables(
            sql,
            default_project_id=project_id,
            default_dataset_id=dataset_id,
        ):
            if source.full_name == target.full_name or _is_system_ref(source):
                continue
            _ensure_asset_nodes(builder, source, discovered_via="ddl")
            builder.add_edge(
                source.full_name,
                target.full_name,
                "view_depends_on",
                {
                    "run_ids": {_str_value(row.get("run_id"))},
                    "projects": {project_id, source.project_id},
                    "observed_projects": {project_id},
                    "locations": {_str_value(row.get("location"))},
                    "provenance_sources": {"ddl"},
                    "sample_queries": {_compact_sql(sql)},
                    "query_count": 1,
                    "cross_project": source.project_id != project_id,
                },
            )


def _build_query_patterns(runs: object) -> list[LineageQuery]:
    patterns: dict[tuple[str, str], dict[str, Any]] = {}
    for row in _records(getattr(runs, "frequent_queries", None)):
        project_id = _str_value(row.get("project_id"))
        fingerprint = _str_value(row.get("query_hash")) or atlas_query_fingerprint(_str_value(row.get("sample_query")))
        sample_sql = _str_value(row.get("sample_query"))
        declared_target = _extract_declared_target_from_sql(
            sample_sql,
            default_project_id=project_id,
            include_views=True,
        )
        source_asset_ids = [
            ref.full_name
            for ref in extract_bigquery_tables(sample_sql, default_project_id=project_id)
            if not _is_system_ref(ref) and ref.full_name != (declared_target.full_name if declared_target else "")
        ]
        key = (project_id, fingerprint)
        if key not in patterns:
            patterns[key] = {
                "id": query_pattern_node_id(project_id, fingerprint),
                "fingerprint": fingerprint,
                "project_id": project_id,
                "sample_sql": sample_sql,
                "source_asset_ids": set(source_asset_ids),
                "execution_count": _int_value(row.get("execution_count")) or 0,
                "user_emails": {
                    _str_value(user)
                    for user in (row.get("users") or [])
                    if _str_value(user)
                },
                "avg_bytes": _float_value(row.get("avg_bytes")),
                "avg_slot_ms": _float_value(row.get("avg_slot_ms")),
                "metadata": {
                    "run_ids": {_str_value(row.get("run_id"))},
                    "locations": {_str_value(row.get("location"))},
                    "bq_query_hashes": {_str_value(row.get("query_hash"))},
                    "target_asset_id": declared_target.full_name if declared_target else "",
                },
            }
            continue

        existing = patterns[key]
        existing["source_asset_ids"] |= set(source_asset_ids)
        existing["execution_count"] += _int_value(row.get("execution_count")) or 0
        existing["user_emails"] |= {
            _str_value(user) for user in (row.get("users") or []) if _str_value(user)
        }
        for meta_key, meta_value in {
            "run_ids": {_str_value(row.get("run_id"))},
            "locations": {_str_value(row.get("location"))},
            "bq_query_hashes": {_str_value(row.get("query_hash"))},
        }.items():
            existing["metadata"][meta_key] |= meta_value

    return [
        LineageQuery(
            id=row["id"],
            fingerprint=row["fingerprint"],
            project_id=row["project_id"],
            sample_sql=row["sample_sql"],
            source_asset_ids=sorted(row["source_asset_ids"]),
            execution_count=row["execution_count"],
            user_emails=sorted(row["user_emails"]),
            avg_bytes=row["avg_bytes"],
            avg_slot_ms=row["avg_slot_ms"],
            metadata=_normalize_metadata(row["metadata"]),
        )
        for row in patterns.values()
    ]


def _records(value: object) -> list[dict[str, Any]]:
    if value is None:
        return []
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return list(to_dict(orient="records"))
    if isinstance(value, list):
        return [dict(item) for item in value]
    if isinstance(value, tuple):
        return [dict(item) for item in value]
    return [dict(item) for item in value]  # type: ignore[arg-type]


def _iter_query_sources(
    refs: list[Mapping[str, Any]],
    *,
    default_project_id: str,
) -> list[SqlTableRef]:
    deduped: dict[str, SqlTableRef] = {}
    for ref in refs:
        ref_obj = _sql_ref_from_mapping(ref, default_project_id=default_project_id)
        if ref_obj is None:
            continue
        deduped[ref_obj.full_name] = ref_obj
    return [deduped[key] for key in sorted(deduped)]


def _ensure_asset_nodes(
    builder: _LineageGraphBuilder,
    ref: SqlTableRef,
    *,
    discovered_via: str,
) -> None:
    builder.add_node(
        dataset_node_id(ref.project_id, ref.dataset_id),
        "dataset",
        ref.dataset_id,
        {
            "project_id": ref.project_id,
            "dataset_id": ref.dataset_id,
            "discovered_via": discovered_via,
        },
    )
    builder.add_node(
        ref.full_name,
        "table",
        ref.table_id,
        {
            "project_id": ref.project_id,
            "dataset_id": ref.dataset_id,
            "table_id": ref.table_id,
            "discovered_via": discovered_via,
        },
    )


def _merge_metadata(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if value in (None, "", [], set(), tuple(), {}):
            continue
        if key in _SET_FIELDS:
            existing_values = set(merged.get(key, []))
            if isinstance(value, set):
                merged[key] = existing_values | value
            elif isinstance(value, list):
                merged[key] = existing_values | set(value)
            else:
                merged[key] = existing_values | {value}
        elif key in _SUM_FIELDS:
            merged[key] = _float_value(merged.get(key)) or 0.0
            merged[key] += _float_value(value) or 0.0
        elif key in _MIN_FIELDS:
            candidate = _timestamp_string(value)
            current = _timestamp_string(merged.get(key))
            candidates = [x for x in [current, candidate] if x]
            if candidates:
                merged[key] = min(candidates)
        elif key in _MAX_FIELDS:
            candidate = _timestamp_string(value)
            current = _timestamp_string(merged.get(key))
            candidates = [x for x in [current, candidate] if x]
            if candidates:
                merged[key] = max(candidates)
        elif key in _BOOL_OR_FIELDS:
            merged[key] = bool(merged.get(key)) or bool(value)
        elif key not in merged or merged[key] in (None, ""):
            merged[key] = value
    return merged


def _normalize_metadata(metadata: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if key in _SET_FIELDS:
            normalized[key] = sorted(str(item) for item in value)
        elif isinstance(value, set):
            normalized[key] = sorted(str(item) for item in value)
        else:
            normalized[key] = value
    return normalized


def _node_type_from_table_type(table_type: str) -> str:
    upper = table_type.upper()
    if upper == "VIEW":
        return "view"
    if upper == "MATERIALIZED VIEW":
        return "materialized_view"
    return "table"


def _consumer_kind(user_email: str) -> str:
    lower = user_email.lower()
    if any(token in lower for token in ("gserviceaccount.com", "@appspot", "serviceaccounts.")):
        return "service_account"
    return "user"


def _looks_like_write_statement(statement_type: str, sql: str) -> bool:
    upper = statement_type.upper()
    if any(token in upper for token in ("INSERT", "MERGE", "CREATE_TABLE", "CREATE OR REPLACE")):
        return True
    return _WRITE_PREFIX_RE.match(sql) is not None


def _collect_cte_aliases(parsed: exp.Expression) -> frozenset[str]:
    aliases: set[str] = set()
    for cte in parsed.find_all(exp.CTE):
        alias = cte.alias
        if alias:
            aliases.add(alias.lower())
    return frozenset(aliases)


def _table_ref_from_expression(
    table_expr: exp.Table,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
) -> SqlTableRef | None:
    name = table_expr.name
    if not name:
        return None
    name_lower = name.lower()
    if name_lower in _SKIP_NAMES:
        return None

    catalog = (table_expr.catalog or "").strip()
    db = (table_expr.db or "").strip()
    return _table_ref_from_parts(
        catalog,
        db,
        name,
        default_project_id=default_project_id,
        default_dataset_id=default_dataset_id,
    )


def _table_ref_from_parts(
    catalog: str,
    db: str,
    name: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
) -> SqlTableRef | None:
    table_id = name.strip()
    if not table_id:
        return None
    if table_id.lower() in _SKIP_NAMES:
        return None

    project_id = catalog.strip()
    dataset_id = db.strip()
    if project_id and dataset_id:
        pass
    elif dataset_id:
        project_id = default_project_id or ""
    elif default_dataset_id and default_project_id:
        dataset_id = default_dataset_id
        project_id = default_project_id
    else:
        return None

    if not project_id or not dataset_id:
        return None
    ref = SqlTableRef(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    if _is_system_ref(ref):
        return None
    return ref


def _sql_ref_from_mapping(
    ref: Mapping[str, Any],
    *,
    default_project_id: str,
) -> SqlTableRef | None:
    project_id = _str_value(ref.get("project_id")) or default_project_id
    dataset_id = _str_value(ref.get("dataset_id"))
    table_id = _str_value(ref.get("table_id"))
    if not project_id or not dataset_id or not table_id:
        return None
    result = SqlTableRef(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
    if _is_system_ref(result):
        return None
    return result


def _is_system_ref(ref: SqlTableRef) -> bool:
    return (
        ref.dataset_id.lower() in _SKIP_SCHEMAS
        or ref.table_id.upper().startswith("INFORMATION_SCHEMA")
        or ref.dataset_id.lower().startswith("region-")
        or ref.table_id.upper().startswith("INFORMATION_SCHEMA.")
    )


def _regex_extract_bigquery(
    sql: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
) -> list[SqlTableRef]:
    refs: dict[str, SqlTableRef] = {}
    for match in _BIGQUERY_TABLE_RE.finditer(sql):
        raw_name = match.group("name").strip().strip("`")
        parts = [part.strip() for part in raw_name.split(".") if part.strip()]
        if not parts:
            continue
        if any(part.lower() in _SKIP_NAMES for part in parts):
            continue
        project_id = ""
        dataset_id = ""
        table_id = ""
        if len(parts) >= 3:
            project_id, dataset_id, table_id = parts[-3], parts[-2], parts[-1]
        elif len(parts) == 2:
            project_id = default_project_id or ""
            dataset_id, table_id = parts[-2], parts[-1]
        elif len(parts) == 1:
            project_id = default_project_id or ""
            dataset_id = default_dataset_id or ""
            table_id = parts[-1]
        if not project_id or not dataset_id or not table_id:
            continue
        ref = SqlTableRef(project_id=project_id, dataset_id=dataset_id, table_id=table_id)
        if _is_system_ref(ref):
            continue
        refs[ref.full_name.lower()] = ref
    return [refs[key] for key in sorted(refs)]


def _regex_extract_declared_target(
    sql: str,
    *,
    default_project_id: str | None = None,
    default_dataset_id: str | None = None,
    include_views: bool,
) -> SqlTableRef | None:
    patterns = [_INSERT_TARGET_RE, _CREATE_TARGET_RE, _MERGE_TARGET_RE]
    if include_views:
        patterns.append(_CREATE_VIEW_TARGET_RE)

    for pattern in patterns:
        match = pattern.search(sql)
        if not match:
            continue
        parts = [part.strip() for part in match.group("name").split(".") if part.strip()]
        if len(parts) >= 3:
            return SqlTableRef(project_id=parts[-3], dataset_id=parts[-2], table_id=parts[-1])
        if len(parts) == 2 and default_project_id:
            return SqlTableRef(project_id=default_project_id, dataset_id=parts[-2], table_id=parts[-1])
        if len(parts) == 1 and default_project_id and default_dataset_id:
            return SqlTableRef(project_id=default_project_id, dataset_id=default_dataset_id, table_id=parts[-1])
    return None


def _timestamp_string(value: Any) -> str:
    if value is None or value == "":
        return ""
    if hasattr(value, "isoformat"):
        return str(value.isoformat())
    return str(value)


def _str_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _float_value(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_value(value: Any) -> int | None:
    number = _float_value(value)
    if number is None:
        return None
    return int(number)


def _compact_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip()[:500]


def _resolve_seed_nodes(
    graph: Any,
    *,
    focus_node_ids: list[str],
    project_id: str | None,
    dataset_id: str | None,
    consumer: str | None,
    asset_id: str | None,
) -> list[str]:
    seed_nodes: set[str] = {node_id for node_id in focus_node_ids if node_id in graph}

    if project_id:
        for node_id, data in graph.nodes(data=True):
            metadata = data.get("metadata", {})
            if metadata.get("project_id") == project_id:
                seed_nodes.add(node_id)

    if dataset_id:
        for node_id, data in graph.nodes(data=True):
            metadata = data.get("metadata", {})
            if node_id == dataset_id or metadata.get("dataset_id") == dataset_id.split(".")[-1]:
                if metadata.get("project_id") and "." in dataset_id:
                    project_part = dataset_id.split(".", 1)[0]
                    if metadata.get("project_id") != project_part and node_id != dataset_id:
                        continue
                seed_nodes.add(node_id)

    if consumer:
        consumer_node = consumer if consumer.startswith("consumer:") else consumer_node_id(consumer)
        if consumer_node in graph:
            seed_nodes.add(consumer_node)

    if asset_id and asset_id in graph:
        seed_nodes.add(asset_id)

    return sorted(seed_nodes)


def _subgraph_rank_key(graph: Any, node_id: str, seed_ids: list[str]) -> tuple[int, float, str]:
    total_weight = 0.0
    for _, _, data in graph.in_edges(node_id, data=True):
        total_weight += _edge_weight(data)
    for _, _, data in graph.out_edges(node_id, data=True):
        total_weight += _edge_weight(data)

    return (
        0 if node_id in seed_ids else 1,
        -total_weight,
        node_id,
    )


def _edge_weight(edge_data: Mapping[str, Any]) -> float:
    metadata = edge_data.get("metadata", {})
    if not isinstance(metadata, Mapping):
        return 0.0
    query_count = _float_value(metadata.get("query_count")) or 0.0
    execution_count = _float_value(metadata.get("execution_count")) or 0.0
    return query_count + execution_count


