"""BigQuery-specific graph transforms built on the Atlas transform contract."""

from __future__ import annotations

import networkx as nx

from bq_extraction.lineage import LineageGraph


class TempTableCollapseTransform:
    """Collapse timestamped temp tables into canonical logical temp nodes."""

    @property
    def name(self) -> str:
        return "bq.temp_table_collapse"

    def apply(self, graph: LineageGraph) -> nx.MultiDiGraph:
        from bq_extraction.full_graph_export import to_logical_asset_only_multidigraph

        return to_logical_asset_only_multidigraph(graph)
