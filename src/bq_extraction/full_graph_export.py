"""Full-graph export helpers for large lineage graphs."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import networkx as nx

from bq_extraction.lineage import LineageGraph, LineageNode

_NON_ALNUM_RE = re.compile(r"[^a-zA-Z0-9_]+")
_TEMP_INCREMENTAL_DATASET = "temp_incremental_tables"
_TEMP_TABLE_RE = re.compile(r"^(?P<base>.+)_temp_(?P<timestamp>\d+)$")
_SET_FIELDS = {
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
_SUM_FIELDS = {
    "query_count",
    "execution_count",
    "total_bytes_processed",
    "total_bytes_billed",
    "total_slot_ms",
}
_MIN_FIELDS = {"first_seen"}
_MAX_FIELDS = {"last_seen"}
_BOOL_OR_FIELDS = {"cross_project", "unresolved_write_target"}


@dataclass(frozen=True)
class FullGraphExportArtifacts:
    """Paths produced by the full-graph export workflow."""

    graphml_path: Path | None
    gexf_path: Path | None
    asset_graphml_path: Path | None
    asset_gexf_path: Path | None
    logical_asset_graphml_path: Path | None
    logical_asset_gexf_path: Path | None
    nodes_ndjson_path: Path | None
    edges_ndjson_path: Path | None
    nodes_chunk_dir: Path | None
    edges_chunk_dir: Path | None
    manifest_path: Path


def to_external_multidigraph(lineage_graph: LineageGraph) -> nx.MultiDiGraph:
    """Convert a lineage graph into a MultiDiGraph with flattened attributes."""

    graph = nx.MultiDiGraph()
    for node in lineage_graph.nodes:
        graph.add_node(
            node.id,
            node_id=node.id,
            node_type=node.node_type,
            name=node.name,
            **flatten_graph_attributes(node.metadata, prefix="meta"),
        )

    for edge in lineage_graph.edges:
        graph.add_edge(
            edge.source,
            edge.target,
            key=edge.id,
            edge_id=edge.id,
            edge_type=edge.edge_type,
            **flatten_graph_attributes(edge.metadata, prefix="meta"),
        )

    return graph


def to_asset_only_multidigraph(lineage_graph: LineageGraph) -> nx.MultiDiGraph:
    """Convert a lineage graph into an asset-only MultiDiGraph.

    Includes only:
    - nodes with types `table`, `view`, `materialized_view`
    - edges with types `writes`, `view_depends_on`
    """

    asset_node_types = {"table", "view", "materialized_view"}
    asset_edge_types = {"writes", "view_depends_on"}

    graph = nx.MultiDiGraph()
    allowed_node_ids = {
        node.id
        for node in lineage_graph.nodes
        if node.node_type in asset_node_types
    }

    for node in lineage_graph.nodes:
        if node.id not in allowed_node_ids:
            continue
        graph.add_node(
            node.id,
            node_id=node.id,
            node_type=node.node_type,
            name=node.name,
            **flatten_graph_attributes(node.metadata, prefix="meta"),
        )

    for edge in lineage_graph.edges:
        if edge.edge_type not in asset_edge_types:
            continue
        if edge.source not in allowed_node_ids or edge.target not in allowed_node_ids:
            continue
        graph.add_edge(
            edge.source,
            edge.target,
            key=edge.id,
            edge_id=edge.id,
            edge_type=edge.edge_type,
            **flatten_graph_attributes(edge.metadata, prefix="meta"),
        )

    return graph


def to_logical_asset_only_multidigraph(lineage_graph: LineageGraph) -> nx.MultiDiGraph:
    """Convert a lineage graph into a de-noised logical asset-only graph.

    Temp tables in `temp_incremental_tables` with names like `*_temp_<digits>`
    collapse into canonical logical temp nodes, while preserving the temp layer
    rather than bypassing it entirely.
    """

    asset_node_types = {"table", "view", "materialized_view"}
    asset_edge_types = {"writes", "view_depends_on"}
    node_by_id = {node.id: node for node in lineage_graph.nodes}

    logical_nodes: dict[str, dict[str, Any]] = {}
    for node in lineage_graph.nodes:
        if node.node_type not in asset_node_types:
            continue
        logical_id, logical_name, logical_metadata = _logical_asset_identity(node)
        if logical_id not in logical_nodes:
            logical_nodes[logical_id] = {
                "node_type": node.node_type,
                "name": logical_name,
                "metadata": logical_metadata,
            }
            continue
        logical_nodes[logical_id]["metadata"] = _merge_lineage_metadata(
            logical_nodes[logical_id]["metadata"],
            logical_metadata,
        )

    logical_edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    for edge in lineage_graph.edges:
        if edge.edge_type not in asset_edge_types:
            continue
        source_node = node_by_id.get(edge.source)
        target_node = node_by_id.get(edge.target)
        if source_node is None or target_node is None:
            continue
        if source_node.node_type not in asset_node_types or target_node.node_type not in asset_node_types:
            continue
        logical_source, _, _ = _logical_asset_identity(source_node)
        logical_target, _, _ = _logical_asset_identity(target_node)
        edge_key = (logical_source, logical_target, edge.edge_type)
        if edge_key not in logical_edges:
            logical_edges[edge_key] = dict(edge.metadata)
            continue
        logical_edges[edge_key] = _merge_lineage_metadata(logical_edges[edge_key], edge.metadata)

    graph = nx.MultiDiGraph()
    for logical_id, record in sorted(logical_nodes.items()):
        graph.add_node(
            logical_id,
            node_id=logical_id,
            node_type=record["node_type"],
            name=record["name"],
            **flatten_graph_attributes(record["metadata"], prefix="meta"),
        )

    for (source, target, edge_type), metadata in sorted(logical_edges.items()):
        edge_id = f"{source}:{target}:{edge_type}"
        graph.add_edge(
            source,
            target,
            key=edge_id,
            edge_id=edge_id,
            edge_type=edge_type,
            **flatten_graph_attributes(metadata, prefix="meta"),
        )

    return graph


def flatten_graph_attributes(
    payload: Mapping[str, Any] | None,
    *,
    prefix: str = "meta",
) -> dict[str, str | int | float | bool]:
    """Flatten nested metadata for GraphML / GEXF compatibility."""

    payload = dict(payload or {})
    flattened: dict[str, str | int | float | bool] = {}
    flattened[f"{prefix}_json"] = json.dumps(_json_safe(payload), sort_keys=True)
    _flatten_value(prefix, payload, flattened)
    return flattened


def write_full_graph_exports(
    lineage_graph: LineageGraph,
    *,
    output_dir: str | Path,
    write_graphml: bool = True,
    write_gexf: bool = False,
    write_ndjson: bool = True,
    write_chunked_json: bool = True,
    chunk_size: int = 50_000,
) -> FullGraphExportArtifacts:
    """Write full-graph exports for external graph tools and large pipelines."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    graphml_path = output_path / "lineage.graphml" if write_graphml else None
    gexf_path = output_path / "lineage.gexf" if write_gexf else None
    asset_graphml_path = output_path / "lineage_assets_only.graphml" if write_graphml else None
    asset_gexf_path = output_path / "lineage_assets_only.gexf" if write_gexf else None
    logical_asset_graphml_path = (
        output_path / "lineage_assets_logical.graphml" if write_graphml else None
    )
    logical_asset_gexf_path = (
        output_path / "lineage_assets_logical.gexf" if write_gexf else None
    )
    nodes_ndjson_path = output_path / "lineage_nodes.ndjson" if write_ndjson else None
    edges_ndjson_path = output_path / "lineage_edges.ndjson" if write_ndjson else None
    nodes_chunk_dir = output_path / "lineage_nodes_chunks" if write_chunked_json else None
    edges_chunk_dir = output_path / "lineage_edges_chunks" if write_chunked_json else None
    manifest_path = output_path / "manifest.json"

    multi_graph = to_external_multidigraph(lineage_graph)
    asset_graph = to_asset_only_multidigraph(lineage_graph)
    logical_asset_graph = to_logical_asset_only_multidigraph(lineage_graph)

    if graphml_path is not None:
        nx.write_graphml(multi_graph, graphml_path)
    if asset_graphml_path is not None:
        nx.write_graphml(asset_graph, asset_graphml_path)
    if logical_asset_graphml_path is not None:
        nx.write_graphml(logical_asset_graph, logical_asset_graphml_path)

    if gexf_path is not None:
        nx.write_gexf(multi_graph, gexf_path)
    if asset_gexf_path is not None:
        nx.write_gexf(asset_graph, asset_gexf_path)
    if logical_asset_gexf_path is not None:
        nx.write_gexf(logical_asset_graph, logical_asset_gexf_path)

    node_records = [
        {
            "id": node_id,
            **dict(data),
        }
        for node_id, data in sorted(multi_graph.nodes(data=True))
    ]
    edge_records = [
        {
            "source": source,
            "target": target,
            "key": key,
            **dict(data),
        }
        for source, target, key, data in sorted(multi_graph.edges(keys=True, data=True))
    ]

    if nodes_ndjson_path is not None:
        _write_ndjson(nodes_ndjson_path, node_records)
    if edges_ndjson_path is not None:
        _write_ndjson(edges_ndjson_path, edge_records)

    node_chunk_files: list[str] = []
    edge_chunk_files: list[str] = []
    if nodes_chunk_dir is not None:
        node_chunk_files = _write_chunked_json(
            nodes_chunk_dir,
            base_name="lineage_nodes",
            records=node_records,
            chunk_size=chunk_size,
        )
    if edges_chunk_dir is not None:
        edge_chunk_files = _write_chunked_json(
            edges_chunk_dir,
            base_name="lineage_edges",
            records=edge_records,
            chunk_size=chunk_size,
        )

    manifest = {
        "graph_stats": {
            "nodes": len(lineage_graph.nodes),
            "edges": len(lineage_graph.edges),
            "queries": len(lineage_graph.queries),
            "issues": len(lineage_graph.issues),
            "asset_only_nodes": asset_graph.number_of_nodes(),
            "asset_only_edges": asset_graph.number_of_edges(),
            "logical_asset_only_nodes": logical_asset_graph.number_of_nodes(),
            "logical_asset_only_edges": logical_asset_graph.number_of_edges(),
            "collapsed_temp_source_nodes": _count_temp_nodes(asset_graph),
            "collapsed_temp_canonical_nodes": _count_logical_temp_nodes(logical_asset_graph),
            "collapsed_temp_families": _count_logical_temp_nodes(logical_asset_graph),
        },
        "formats": {
            "graphml": str(graphml_path) if graphml_path else "",
            "gexf": str(gexf_path) if gexf_path else "",
            "asset_graphml": str(asset_graphml_path) if asset_graphml_path else "",
            "asset_gexf": str(asset_gexf_path) if asset_gexf_path else "",
            "logical_asset_graphml": str(logical_asset_graphml_path) if logical_asset_graphml_path else "",
            "logical_asset_gexf": str(logical_asset_gexf_path) if logical_asset_gexf_path else "",
            "nodes_ndjson": str(nodes_ndjson_path) if nodes_ndjson_path else "",
            "edges_ndjson": str(edges_ndjson_path) if edges_ndjson_path else "",
            "node_chunks": node_chunk_files,
            "edge_chunks": edge_chunk_files,
        },
        "metadata_strategy": {
            "flatten_prefix": "meta",
            "preserve_json_field": "meta_json",
            "chunk_size": chunk_size,
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    return FullGraphExportArtifacts(
        graphml_path=graphml_path,
        gexf_path=gexf_path,
        asset_graphml_path=asset_graphml_path,
        asset_gexf_path=asset_gexf_path,
        logical_asset_graphml_path=logical_asset_graphml_path,
        logical_asset_gexf_path=logical_asset_gexf_path,
        nodes_ndjson_path=nodes_ndjson_path,
        edges_ndjson_path=edges_ndjson_path,
        nodes_chunk_dir=nodes_chunk_dir,
        edges_chunk_dir=edges_chunk_dir,
        manifest_path=manifest_path,
    )


def _logical_asset_identity(node: LineageNode) -> tuple[str, str, dict[str, Any]]:
    metadata = dict(node.metadata)
    project_id = str(metadata.get("project_id") or "")
    dataset_id = str(metadata.get("dataset_id") or "")
    table_id = str(metadata.get("table_id") or "")

    match = _TEMP_TABLE_RE.match(table_id)
    is_temp = dataset_id == _TEMP_INCREMENTAL_DATASET and match is not None
    if not is_temp:
        return node.id, node.name, metadata

    canonical_name = match.group("base")
    logical_id = f"{project_id}.{dataset_id}.{canonical_name}"
    logical_metadata = {
        **metadata,
        "table_id": canonical_name,
        "logical_node": True,
        "logical_node_kind": "canonical_temp",
        "collapsed_temp_base": canonical_name,
        "collapsed_temp_count": 1,
        "collapsed_physical_ids": [node.id],
        "collapsed_temp_timestamps": [match.group("timestamp")],
    }
    return logical_id, canonical_name, logical_metadata


def _merge_lineage_metadata(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if value in (None, "", [], set(), tuple(), {}):
            continue
        if key in _SET_FIELDS:
            existing_values = set(merged.get(key, []))
            if isinstance(value, set):
                merged[key] = sorted(existing_values | value)
            elif isinstance(value, list):
                merged[key] = sorted(existing_values | set(value))
            else:
                merged[key] = sorted(existing_values | {value})
        elif key in _SUM_FIELDS:
            merged[key] = (_as_float(merged.get(key)) or 0.0) + (_as_float(value) or 0.0)
        elif key in _MIN_FIELDS:
            candidate = _as_timestamp(value)
            current = _as_timestamp(merged.get(key))
            values = [item for item in [candidate, current] if item is not None]
            if values:
                merged[key] = min(values).isoformat()
        elif key in _MAX_FIELDS:
            candidate = _as_timestamp(value)
            current = _as_timestamp(merged.get(key))
            values = [item for item in [candidate, current] if item is not None]
            if values:
                merged[key] = max(values).isoformat()
        elif key in _BOOL_OR_FIELDS:
            merged[key] = bool(merged.get(key)) or bool(value)
        elif key == "collapsed_temp_count":
            merged[key] = int(merged.get(key, 0)) + int(value)
        elif key in {"collapsed_physical_ids", "collapsed_temp_timestamps"}:
            existing_values = list(merged.get(key, []))
            incoming_values = list(value) if isinstance(value, list) else [value]
            merged[key] = sorted(dict.fromkeys(existing_values + incoming_values))
        elif key not in merged or merged[key] in (None, ""):
            merged[key] = value
    return merged


def _flatten_value(
    key: str,
    value: Any,
    flattened: dict[str, str | int | float | bool],
) -> None:
    safe_key = _sanitize_key(key)
    if value is None:
        return
    if isinstance(value, bool):
        flattened[safe_key] = value
        return
    if isinstance(value, (int, float, str)):
        flattened[safe_key] = value
        return
    if isinstance(value, Mapping):
        for child_key, child_value in value.items():
            _flatten_value(f"{safe_key}_{child_key}", child_value, flattened)
        return
    if isinstance(value, (list, tuple, set)):
        values = list(value)
        scalar_values = [item for item in values if isinstance(item, (str, int, float, bool))]
        if len(scalar_values) == len(values):
            flattened[safe_key] = "|".join(str(item) for item in scalar_values)
        else:
            flattened[safe_key] = json.dumps(values, sort_keys=True)
        return
    flattened[safe_key] = str(value)


def _count_temp_nodes(graph: nx.MultiDiGraph) -> int:
    return sum(1 for _, data in graph.nodes(data=True) if _is_temp_node_attrs(data))


def _count_logical_temp_nodes(graph: nx.MultiDiGraph) -> int:
    return sum(1 for _, data in graph.nodes(data=True) if bool(data.get("meta_logical_node")))


def _is_temp_node_attrs(data: Mapping[str, Any]) -> bool:
    dataset_id = str(data.get("meta_dataset_id") or "")
    table_id = str(data.get("meta_table_id") or "")
    return dataset_id == _TEMP_INCREMENTAL_DATASET and _TEMP_TABLE_RE.match(table_id) is not None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _json_safe(child) for key, child in value.items()}
    if isinstance(value, set):
        return [_json_safe(item) for item in sorted(value, key=str)]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _sanitize_key(key: str) -> str:
    sanitized = re.sub(r"[^a-zA-Z0-9_]+", "_", key).strip("_").lower()
    return sanitized or "value"


def _write_ndjson(path: Path, records: Iterable[Mapping[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, sort_keys=True))
            handle.write("\n")


def _write_chunked_json(
    output_dir: Path,
    *,
    base_name: str,
    records: list[Mapping[str, Any]],
    chunk_size: int,
) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    chunk_count = max(1, math.ceil(len(records) / chunk_size))
    written_files: list[str] = []
    for index in range(chunk_count):
        start = index * chunk_size
        stop = min(len(records), start + chunk_size)
        chunk_path = output_dir / f"{base_name}_{index + 1:04d}.json"
        chunk_path.write_text(
            json.dumps(records[start:stop], separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        written_files.append(str(chunk_path))
    return written_files


def _logical_asset_identity(node: LineageNode) -> tuple[str, str, dict[str, Any]]:
    metadata = dict(node.metadata)
    project_id = str(metadata.get("project_id") or "")
    dataset_id = str(metadata.get("dataset_id") or "")
    table_id = str(metadata.get("table_id") or "")

    match = _TEMP_TABLE_RE.match(table_id)
    is_temp = dataset_id == _TEMP_INCREMENTAL_DATASET and match is not None
    if not is_temp:
        return node.id, node.name, metadata

    canonical_name = match.group("base")
    logical_id = f"{project_id}.{dataset_id}.{canonical_name}"
    logical_metadata = {
        **metadata,
        "table_id": canonical_name,
        "logical_node": True,
        "logical_node_kind": "canonical_temp",
        "collapsed_temp_base": canonical_name,
        "collapsed_temp_count": 1,
        "collapsed_physical_ids": [node.id],
        "collapsed_temp_timestamps": [match.group("timestamp")],
    }
    return logical_id, canonical_name, logical_metadata


def _merge_lineage_metadata(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if value in (None, "", [], set(), tuple(), {}):
            continue
        if key in _SET_FIELDS:
            existing_values = set(merged.get(key, []))
            if isinstance(value, set):
                merged[key] = sorted(existing_values | value)
            elif isinstance(value, list):
                merged[key] = sorted(existing_values | set(value))
            else:
                merged[key] = sorted(existing_values | {value})
        elif key in _SUM_FIELDS:
            merged[key] = (_as_float(merged.get(key)) or 0.0) + (_as_float(value) or 0.0)
        elif key in _MIN_FIELDS:
            candidate = _as_timestamp(value)
            current = _as_timestamp(merged.get(key))
            values = [item for item in [candidate, current] if item is not None]
            if values:
                merged[key] = min(values).isoformat()
        elif key in _MAX_FIELDS:
            candidate = _as_timestamp(value)
            current = _as_timestamp(merged.get(key))
            values = [item for item in [candidate, current] if item is not None]
            if values:
                merged[key] = max(values).isoformat()
        elif key in _BOOL_OR_FIELDS:
            merged[key] = bool(merged.get(key)) or bool(value)
        elif key == "collapsed_temp_count":
            merged[key] = int(merged.get(key, 0)) + int(value)
        elif key in {"collapsed_physical_ids", "collapsed_temp_timestamps"}:
            existing_values = list(merged.get(key, []))
            incoming_values = list(value) if isinstance(value, list) else [value]
            merged[key] = sorted(dict.fromkeys(existing_values + incoming_values))
        elif key not in merged or merged[key] in (None, ""):
            merged[key] = value
    return merged


def _count_temp_nodes(graph: nx.MultiDiGraph) -> int:
    return sum(1 for _, data in graph.nodes(data=True) if _is_temp_node_attrs(data))


def _count_logical_temp_nodes(graph: nx.MultiDiGraph) -> int:
    return sum(1 for _, data in graph.nodes(data=True) if bool(data.get("meta_logical_node")))


def _is_temp_node_attrs(data: Mapping[str, Any]) -> bool:
    dataset_id = str(data.get("meta_dataset_id") or "")
    table_id = str(data.get("meta_table_id") or "")
    return dataset_id == _TEMP_INCREMENTAL_DATASET and _TEMP_TABLE_RE.match(table_id) is not None


def _as_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None
