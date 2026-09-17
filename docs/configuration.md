# Configuration reference

`serve`, `doctor`, and `status` resolve configuration in this order: command-line flag,
environment variable, optional JSON configuration file, then the safe default. Unknown JSON keys,
non-loopback binds, an empty artifact root, and an HTTP/gRPC bind collision are rejected.

| JSON key | Command-line flag | Environment variable | Default |
| --- | --- | --- | --- |
| `artifact_root` | `--artifact-root` | `IMPOSSIBLE_INFERENCES_ARTIFACT_ROOT` | `runtime-artifacts` |
| `bind` | `--bind` | `IMPOSSIBLE_INFERENCES_BIND` | `127.0.0.1:8080` |
| `grpc_bind` | `--grpc-bind` | `IMPOSSIBLE_INFERENCES_GRPC_BIND` | `127.0.0.1:50051` |
| `max_request_bytes` | `--max-request-bytes` | `IMPOSSIBLE_INFERENCES_MAX_REQUEST_BYTES` | `1048576` |
| `queue_capacity` | `--queue-capacity` | `IMPOSSIBLE_INFERENCES_QUEUE_CAPACITY` | `128` |
| `max_concurrent_requests` | `--max-concurrent-requests` | `IMPOSSIBLE_INFERENCES_MAX_CONCURRENT_REQUESTS` | `8` |
| `request_timeout_ms` | `--request-timeout-ms` | `IMPOSSIBLE_INFERENCES_REQUEST_TIMEOUT_MS` | `30000` |
| `shutdown_timeout_ms` | `--shutdown-timeout-ms` | `IMPOSSIBLE_INFERENCES_SHUTDOWN_TIMEOUT_MS` | `10000` |

The optional file is selected with `--config` or `IMPOSSIBLE_INFERENCES_CONFIG`, is limited to
65,536 bytes, and must be a regular UTF-8 JSON file. Example:

```json
{
  "artifact_root": "runtime-artifacts",
  "bind": "127.0.0.1:8080",
  "grpc_bind": "127.0.0.1:50051",
  "max_request_bytes": 1048576,
  "queue_capacity": 128,
  "max_concurrent_requests": 8,
  "request_timeout_ms": 30000,
  "shutdown_timeout_ms": 10000
}
```

Generation has an additional fixed internal policy: one CPU generation lane, eight waiting engine
requests, 65,536 encoded prompt/message bytes, at most 128 chat messages, at most 1,024 requested
output tokens, a 4,096-token model context, and 32 queued output events. The public server may admit
more HTTP/MCP requests, but the engine boundary independently enforces its tighter queue and
concurrency limits. WebSocket sessions are limited to 16 with one active generation per session;
gRPC messages and WebSocket frames are limited to 1 MiB.

All public and private listeners remain loopback-only. To use the Linux container without widening
that boundary, run it with host networking as shown in the README.
