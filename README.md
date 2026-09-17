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

The bounded control plane is runnable, but token generation is not implemented yet. Do not treat
this revision as a functional inference server.

```powershell
cargo run -p impossible-inferences-server -- doctor
cargo run -p impossible-inferences-server -- status
cargo run -p impossible-inferences-server -- setup
cargo run -p impossible-inferences-server -- serve
```

The server binds to `127.0.0.1:8080` by default and refuses non-loopback addresses. Effective
configuration precedence is command-line flag, environment variable, optional bounded JSON file,
then safe default. Run `cargo run -p impossible-inferences-server -- serve --help` for the exact
variables and limits. The current control plane exposes `/health/live`, `/health/ready`, `/metrics`,
`/version`, `/v1/capabilities`, `/v1/models`, and `/status`; readiness remains false until a verified
runtime and model are installed and the generation adapter is healthy.

Setup pins and verifies the official llama.cpp `b10964` Windows/Linux x86-64 CPU runtime and the
official Qwen2.5 0.5B Instruct Q4_K_M GGUF profile. It stages downloads under the ignored
`runtime-artifacts/` directory and atomically promotes a complete installation. Run `setup
--offline` to verify the retained runtime archive and model and reconstruct extracted runtime files
without network access. `serve`, `doctor`, and `status` never download artifacts.

The repository vendors the reviewed Impossible Server core and testkit as a deterministic source
snapshot. That snapshot supplies bounded lifecycle, health, request, cancellation, and test
primitives. It does not contain HTTP, SSE, WebSocket, gRPC, MCP, automatic setup, or inference
implementations; those remain product work tracked by this contract.

## License

Licensed under either the Apache License 2.0 or the MIT License, at your option.
