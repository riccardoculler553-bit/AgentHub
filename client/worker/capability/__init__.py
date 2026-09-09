"""Worker Capability Runtime (V1.4 Phase 2+3).

Capability definition stays on the server; the Worker only knows packages:

    manifest   - package identity + first validation layer (§11)
    cache      - local install tree with checksum markers (§20/§54)
    puller     - HTTP package download, retries per §67
    manager    - Lazy Pull orchestration with per-version locks (§18/§71)
    local_registry - installed-package scan for worker.capabilities (§17)
"""
