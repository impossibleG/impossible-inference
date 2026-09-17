# gRPC and MCP API

Both transports use the same curated model, request validation, context accounting, generation
lane, usage normalization, and cancellation boundary as HTTP and WebSocket. Neither transport can
download a model or reach a paid or remote inference provider.

## gRPC

The local gRPC listener defaults to `127.0.0.1:50051`. Override it with `--grpc-bind`,
`IMPOSSIBLE_INFERENCES_GRPC_BIND`, or `grpc_bind` in the bounded JSON configuration file. Only
loopback addresses are accepted, and the gRPC address must differ from the HTTP address.

The source contract is
[`inference.proto`](../crates/impossible-inferences-protocol/proto/impossible/inferences/v1/inference.proto).
`impossible.inferences.v1.InferenceService` provides:

- `Complete` and `Chat` for unary generation;
- `CompleteStream` and `ChatStream` for ordered delta, usage, and finish events.

Messages are limited to 1 MiB. Client `grpc-timeout` is honored up to the server's 30-second
upper bound. Dropping a response stream cancels or safely abandons generation and releases its
bounded engine lane. The listener also exposes standard gRPC health and v1 reflection. Health is
`SERVING` only while the local engine reports ready and transitions to `NOT_SERVING` when the
runtime becomes unavailable. Shutdown cancels active calls and bounds the complete gRPC drain by
the configured server shutdown timeout. A bundled
`protoc` builds the checked-in contract; users do not need a system Protocol Buffers compiler.

Example with `grpcurl` after starting the server:

```bash
grpcurl -plaintext \
  -d '{"model":"qwen2.5-0.5b-instruct-q4-k-m","prompt":"Say hello","sampling":{"maxTokens":32,"temperature":0,"topP":1}}' \
  127.0.0.1:50051 impossible.inferences.v1.InferenceService/Complete
```

## MCP

MCP uses stateless Streamable HTTP at `POST http://127.0.0.1:8080/mcp` and implements protocol
revision `2026-07-28`. Every request must carry `MCP-Protocol-Version: 2026-07-28` and a matching
`Mcp-Method` header. `tools/call` and `resources/read` also require a matching `Mcp-Name` header.
The JSON-RPC `params._meta` object must include `io.modelcontextprotocol/clientInfo`.

Available tools are `generate_text`, `chat`, `list_models`, and `health`. Available resources are
`impossible://models`, `impossible://health`, and `impossible://capabilities`. Generation tools are
deliberately non-streaming and use the same request deadline and admission limits as ordinary HTTP.
Admission, timeout, and shutdown failures remain JSON-RPC 2.0 error responses and preserve a
parseable request ID.
There are no filesystem, administrative, media, embedding, tool-calling, or model-download tools.

```bash
curl http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  -H 'MCP-Protocol-Version: 2026-07-28' \
  -H 'Mcp-Method: tools/call' \
  -H 'Mcp-Name: generate_text' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"generate_text","arguments":{"model":"qwen2.5-0.5b-instruct-q4-k-m","prompt":"Say hello","max_tokens":32},"_meta":{"io.modelcontextprotocol/clientInfo":{"name":"example","version":"1"}}}}'
```
