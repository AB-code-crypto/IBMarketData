# Versioned target catalog artifacts

These files are source-controlled inputs for the target `src/ibmd` runtime. They are not runtime state and are never rewritten by a service.

Current scope:

- `instruments.v1.json` contains the frozen MNQ instrument master used by strategy profile v1;
- `contracts.mnq.v1.json` preserves the MNQ futures calendar from runtime commit `5073dbea3f96740b4390e113d06578a514918f00` and declares every interval with no active contract explicitly;
- `strategy.IBMarketData.rolling.v1.json` freezes the current rolling parameters and the accepted v1 risk/protection decisions;
- `sessions.v1.json` captures the current coarse CME weekly session model for parity tests.

The session artifact is intentionally marked `production_qualified=false`: the legacy runtime does not contain a complete holiday and early-close calendar. Target services must reject it when production-qualified session coverage is required. A later administrative calendar update must add verified exceptions and change the artifact version/hash before live cutover.

Every JSON artifact contains a SHA-256 `content_hash`. Editing any field without regenerating the hash is rejected. Applied deployment artifacts should be copied into the deployment-specific catalog directory and treated as immutable.
