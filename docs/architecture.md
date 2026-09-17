# Architecture boundaries

The workspace begins with four deliberately small product boundaries:

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
server must implement HTTP, SSE, WebSocket, gRPC, MCP, and automatic setup explicitly in later
commits. The initial snapshot is provisional until Impossible Server has a canonical release
identity; its exact reviewed tree hash is pinned in `foundation-sync.json`.

This bootstrap intentionally contains no generation engine or transport implementation.
