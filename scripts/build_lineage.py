from __future__ import annotations

import argparse
from pathlib import Path

from bq_extraction.atlas_export import build_atlas_export
from bq_extraction.full_graph_export import write_full_graph_exports
from bq_extraction.lineage import build_lineage_graph
from bq_extraction.loader import load_runs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build offline lineage artifacts from extraction results.")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("output"),
        help="Directory containing extraction run folders.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/lineage"),
        help="Directory where neutral and Atlas-ready artifacts will be written.",
    )
    parser.add_argument(
        "--atlas-source-id",
        default="bq-extraction-offline",
        help="Atlas source identifier prefix for exported asset IDs.",
    )
    parser.add_argument(
        "--graphml",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write a full-graph GraphML export.",
    )
    parser.add_argument(
        "--gexf",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Write a full-graph GEXF export.",
    )
    parser.add_argument(
        "--ndjson",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write full-graph NDJSON node and edge exports.",
    )
    parser.add_argument(
        "--chunked-json",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Write chunked compact JSON node and edge exports.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50_000,
        help="Chunk size for compact JSON full-graph exports.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Loading runs from {args.results_dir.resolve()}...")
    runs = load_runs(args.results_dir)
    print(f"Loaded {len(runs.run_summary)} run(s). Building lineage graph...")
    graph = build_lineage_graph(runs)
    print(
        f"Built graph with {len(graph.nodes)} nodes, {len(graph.edges)} edges, "
        f"{len(graph.queries)} query patterns, and {len(graph.issues)} issues."
    )
    print(f"Building Atlas export with source id '{args.atlas_source_id}'...")
    atlas_export = build_atlas_export(graph, source_id=args.atlas_source_id)

    neutral_dir = args.output_dir / "neutral"
    atlas_dir = args.output_dir / "atlas"
    full_graph_dir = args.output_dir / "full-graph"
    print(f"Writing neutral graph JSON to {neutral_dir.resolve()}...")
    graph.write_json(neutral_dir)
    print(f"Writing Atlas JSON to {atlas_dir.resolve()}...")
    atlas_export.write_json(atlas_dir)
    print(f"Writing Atlas SQLite DB to {(args.output_dir / 'atlas.db').resolve()}...")
    atlas_export.write_sqlite(args.output_dir / "atlas.db")
    print(f"Writing full-graph exports to {full_graph_dir.resolve()}...")
    full_graph_artifacts = write_full_graph_exports(
        graph,
        output_dir=full_graph_dir,
        write_graphml=args.graphml,
        write_gexf=args.gexf,
        write_ndjson=args.ndjson,
        write_chunked_json=args.chunked_json,
        chunk_size=args.chunk_size,
    )

    print(f"Results dir: {args.results_dir.resolve()}")
    print(f"Output dir:  {args.output_dir.resolve()}")
    print(f"Nodes:       {len(graph.nodes)}")
    print(f"Edges:       {len(graph.edges)}")
    print(f"Queries:     {len(graph.queries)}")
    print(f"Issues:      {len(graph.issues)}")
    print(f"Atlas assets:{len(atlas_export.assets)}")
    print(f"Atlas edges: {len(atlas_export.edges)}")
    print(f"GraphML:     {full_graph_artifacts.graphml_path or 'disabled'}")
    print(f"Assets only: {full_graph_artifacts.asset_graphml_path or 'disabled'}")
    print(f"Logical:     {full_graph_artifacts.logical_asset_graphml_path or 'disabled'}")
    print(f"GEXF:        {full_graph_artifacts.gexf_path or 'disabled'}")
    print(f"Manifest:    {full_graph_artifacts.manifest_path}")


if __name__ == "__main__":
    main()
