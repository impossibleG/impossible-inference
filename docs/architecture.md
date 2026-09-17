# Architecture boundaries

The workspace has four deliberately small product boundaries:

- `impossible-inferences-domain`: transport-independent requests, events, limits, and failures.
- `impossible-inferences-protocol`: public HTTP, WebSocket, gRPC, and MCP contracts.
- `impossible-inferences-engine`: the narrow local generation adapter and lifecycle.
- `impossible-inferences-server`: configuration, admission, transports, and process composition.

The Impossible Server foundation is imported as a deterministic source snapshot and remains
outside this workspace. Product crates may depend on its bounded lifecycle primitives through
explicit path dependencies; the foundation must not depend on product code.

The reviewed snapshot contains only `impossible-server-core` and `impossible-server-testkit`.
It provides bounded limits, failures, request identity, cancellation, deadlines, shutdown, health,
and reusable test helpers. It does not provide transports or artifact installation. The product
server implements transports and automatic setup explicitly rather than treating the snapshot as
more complete than it is. The snapshot remains provisional until Impossible Server has a canonical
release identity; its exact reviewed tree hash is pinned in `foundation-sync.json`.

The implemented engine verifies the complete extracted runtime tree against the retained pinned
archive, then owns one llama.cpp child on an ephemeral loopback port. It uses a per-process bearer,
bounded captured diagnostics, explicit restart/backoff, fixed Qwen ChatML, and one bounded CPU
generation lane. Startup, restart, readiness, backoff, and lifecycle-lock waits are fenced by the
request deadline and cancellation signal. The fixed model context is 4096 tokens. Admission uses a
deliberately conservative UTF-8-byte upper bound for prompt text, includes the exact ChatML wrapper
and requested output allowance, and can reject a prompt near the true tokenizer boundary. Runtime
delta count and reported completion usage are independently checked against `max_tokens`.

The public server currently adapts that engine to non-streaming HTTP, incremental SSE, and
versioned WebSocket sessions. gRPC and MCP adapters remain pending.
