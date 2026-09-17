# Impossible Inferences

Impossible Inferences is a ready-made, self-hosted token-completion server for local open models.
Version 0.1 provides one curated CPU inference path with automatic verified setup, offline
operation after installation, OpenAI-compatible completion and chat endpoints, SSE token
streaming, WebSocket cancellation, unary and streaming gRPC, and bounded MCP tools.

The repository is implemented in incremental, buildable commits. See [the v0.1 product
contract](docs/product-contract.md) for the exact promise and explicit non-goals.

## Quickstart

Install the pinned Rust 1.88 toolchain, clone the repository, then run exactly one setup command and
one serve command. Setup downloads about 510 MB into the ignored `runtime-artifacts/` directory.

Windows PowerShell:

```powershell
./scripts/setup.ps1
./scripts/serve.ps1
```

Linux:

```bash
./scripts/setup.sh
./scripts/serve.sh
```

The second command remains in the foreground. From another terminal, verify generation:

```bash
curl http://127.0.0.1:8080/v1/completions \
  -H "content-type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4-k-m","prompt":"Write one friendly sentence.","max_tokens":64}'
```

The HTTP/MCP server binds to `127.0.0.1:8080` and gRPC binds to `127.0.0.1:50051` by default;
both refuse non-loopback addresses. Effective configuration precedence is command-line flag,
environment variable, optional bounded JSON file, then safe default. Run `cargo run -p
impossible-inferences-server -- serve --help` for the exact variables and limits. The current
control plane exposes `/health/live`, `/health/ready`, `/metrics`,
`/version`, `/v1/capabilities`, `/v1/models`, `/status`, `/v1/completions`,
`/v1/chat/completions`, `/v1/ws`, and `/mcp`; readiness remains false until a verified runtime and
model are installed and the private generation sidecar is healthy. See the [HTTP, SSE, and
WebSocket API guide](docs/http-websocket-api.md) and [gRPC and MCP API guide](docs/grpc-mcp-api.md)
for the bounded wire contracts.

Use `./scripts/setup.ps1 -Offline` or `./scripts/setup.sh --offline` to verify and reconstruct an
already-downloaded installation without network access. `doctor` performs full artifact
verification and a private engine startup probe; `status` performs fast local inspection. See the
[configuration reference](docs/configuration.md) for every flag, environment variable, JSON key,
default, and fixed generation limit.

Setup pins and verifies the official llama.cpp `b10964` Windows/Linux x86-64 CPU runtime and the
official Qwen2.5 0.5B Instruct Q4_K_M GGUF profile. It stages downloads under the ignored
`runtime-artifacts/` directory and atomically promotes a complete installation. Run `setup
--offline` to verify the retained runtime archive and model and reconstruct extracted runtime files
without network access. `serve`, `doctor`, and `status` never download artifacts. `serve` starts
llama.cpp only on an ephemeral loopback port behind an internal per-process bearer and does not
proxy its UI, admin, model-download, router, tool, embedding, or media routes.

The repository vendors the reviewed Impossible Server core and testkit as a deterministic source
snapshot. That snapshot supplies bounded lifecycle, health, request, cancellation, and test
primitives. It does not contain HTTP, SSE, WebSocket, gRPC, MCP, automatic setup, or inference
implementations; the product implements each of those explicitly.

## Packages and Linux container

Tagged and manual CI runs build privacy-checked native archives for Windows x86-64 and Linux
x86-64. Each archive contains the server, setup/serve launchers, licenses, manifest, and API docs;
it never contains a model or runtime. Maintainers can reproduce the current-platform archive with
`./scripts/package.ps1` after a clean release build.

The Linux CPU container preserves the loopback-only security boundary by using host networking:

```bash
docker build -t impossible-inferences:0.1.0 .
docker volume create impossible-inferences-data
docker run --rm --network host -v impossible-inferences-data:/data impossible-inferences:0.1.0 setup --artifact-root /data
docker run --rm --network host -v impossible-inferences-data:/data impossible-inferences:0.1.0
```

The first `docker run` is setup; the second is serve. Ordinary container startup is offline after
setup. Bridge-port publishing is intentionally unsupported because the server refuses non-loopback
binds.

## License

Licensed under either the Apache License 2.0 or the MIT License, at your option.
