# Impossible Inference v0.1 product contract

## Product promise

Impossible Inference is a ready-made, self-hosted token-completion server. A supported user runs
one setup command and one serve command. Setup downloads, verifies, and installs one curated GGUF
instruction model and a pinned llama.cpp-compatible CPU runtime. Normal operation is local and
offline after installation and does not require a paid inference API.

Progress and release readiness are measured against this document. An internal test subset must
not be reported as completion of this contract.

## Required v0.1 behavior

- Windows x86-64 and Linux x86-64 CPU operation.
- Checksum-verified, staged, recoverable model and runtime installation.
- Explicit model identity with no silent substitution.
- Offline restart and generation after successful setup.
- `POST /v1/completions` and `POST /v1/chat/completions` with a documented, bounded
  OpenAI-compatible request and response subset.
- Ordered Server-Sent Events for `stream: true`, including deterministic terminal events.
- A versioned WebSocket endpoint for completion and chat generation, ordered deltas, stable request
  identifiers, cancellation, bounded frames, bounded output queues, and deterministic close
  behavior.
- Unary and server-streaming gRPC completion and chat methods with deadlines and cancellation.
- Bounded MCP tools named `generate_text`, `chat`, `list_models`, and `health`.
- One loaded model at a time and one reviewed chat template for the curated model.
- `max_tokens`, `temperature`, `top_p`, `seed`, and stop-sequence controls.
- Explicit limits for context, prompts, generated output, queue depth, concurrency, request time,
  and shutdown time.
- Liveness, readiness, model status, capabilities, metrics, privacy-safe logs, backpressure,
  cancellation propagation, and graceful shutdown.
- Native distribution archives and a Linux CPU container, or an explicit release limitation if a
  supported packaging path cannot be completed honestly.

## Cross-transport semantics

Validation and public errors must be stable across transports. Prompt and context limits are
checked before generation. Output limits are enforced by generated-token count. Stop sequences
behave consistently across HTTP, WebSocket, and gRPC. Cancellation must stop generation or safely
abandon it while releasing queue and concurrency accounting. Backpressure must never create an
unbounded token or event buffer.

Prompts, chat messages, generated text, token identifiers, credentials, local filesystem paths,
and machine details are not logged by default. Administrative interfaces never expose internal
filesystem paths.

## Explicitly deferred

- Tool or function calling and hosted-agent behavior.
- Guaranteed JSON schema output, constrained grammars, log probabilities, and token diagnostics.
- Arbitrary remote models, arbitrary templates, and arbitrary tokenizer execution.
- Multiple simultaneously loaded models, hot swapping, and persistent conversation state.
- RAG, vector search, authentication, user management, usage billing, and hosted control planes.
- Persistent KV caches, speculative decoding, distributed inference, and multi-node scheduling.
- GPU qualification, automatic accelerator selection, and a large dashboard.

Deferred work may be added in later releases. It is not required for v0.1 and must not be implied
by the v0.1 capabilities response.

## Release acceptance

From a clean supported machine, a user must be able to run documented setup, start the server,
observe truthful health and model status, receive a useful completion, receive ordered SSE output,
stream and cancel generation over WebSocket, call representative gRPC and MCP operations, restart
offline, and repeat generation. The repository must not commit models, runtimes, build output,
prompts, credentials, private paths, machine specifications, or local orchestration records.
