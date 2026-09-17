# Impossible Inferences

Impossible Inferences is a ready-made, self-hosted token-completion server for local open models.
Version 0.1 will provide one curated CPU inference path with automatic verified setup, offline
operation after installation, OpenAI-compatible completion and chat endpoints, token streaming,
WebSocket cancellation, gRPC, and bounded MCP tools.

The repository is being implemented in incremental, buildable commits. The initial bootstrap
contains the public contract and crate boundaries; it does not yet contain a token-generation
engine. See [the v0.1 product contract](docs/product-contract.md) for the exact promise and explicit
non-goals.

## Current status

Bootstrap only. Do not treat this revision as a functional inference server.

The repository vendors the reviewed Impossible Server core and testkit as a deterministic source
snapshot. That snapshot supplies bounded lifecycle, health, request, cancellation, and test
primitives. It does not contain HTTP, SSE, WebSocket, gRPC, MCP, automatic setup, or inference
implementations; those remain product work tracked by this contract.

## License

Licensed under either the Apache License 2.0 or the MIT License, at your option.
