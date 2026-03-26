# Atlas Graph Core Follow-up

`bq-extraction` is currently restored to a standalone state so it can be
installed, tested, and pushed safely without requiring a sibling Atlas checkout.

The longer-term direction is still to converge on an Atlas-owned reusable graph
core, but that work is intentionally **not** part of the standalone push
recovery.

## Why it is deferred

The cross-repo migration introduced several repo-boundary problems:

- local path dependencies on `../atlas/...`
- a higher Python floor than the repo docs claimed
- runtime imports from Atlas packages in a repo that should work independently

Before reintroducing a shared graph core, `bq-extraction` first needs to remain
healthy as a standalone product.

## What should stay local here for now

- BigQuery artifact loading (`loader.py`)
- BigQuery-specific lineage construction from saved extraction artifacts
- local policy transforms such as temp-table collapse
- notebooks, workflow docs, and CLI wrappers

## What remains a future migration candidate

Potential future convergence into Atlas:

- canonical graph DTOs
- graph transform contracts
- graph-tool export helpers
- Atlas-owned importer/exporter for graph bundles

That follow-up should happen only when one of these is true:

1. Atlas package publishing/versioning is in place
2. `bq-extraction` can depend on published Atlas packages rather than local path deps
3. there is a second real producer/consumer that justifies extracting the shared core

Until then, correctness and standalone operability take priority over
cross-repo abstraction purity.
