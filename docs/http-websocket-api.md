# HTTP, SSE, and WebSocket API

The server binds to loopback by default. All generation routes accept only the curated model ID
`qwen2.5-0.5b-instruct-q4-k-m`. Ordinary serving is offline and never downloads or selects a
different model.

## HTTP completion and chat

`POST /v1/completions` accepts a string `prompt`. `POST /v1/chat/completions` accepts ordered
`system`, `user`, and `assistant` messages whose content is plain text. Both accept `max_tokens`,
`temperature`, `top_p`, `seed`, `stop` as a string or string array, and `stream`.

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H "content-type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4-k-m","messages":[{"role":"user","content":"Say hello."}],"max_tokens":32}'
```

The non-streaming response uses the OpenAI completion/chat shape with one choice, a stable
process-local request ID, a normalized `stop` or `length` finish reason, and prompt, completion,
and total token usage. Unknown fields and unsupported content types are rejected rather than
silently ignored.

The curated model has a fixed 4096-token context. Before execution, the server conservatively
accounts for prompt UTF-8 bytes, the fixed ChatML wrapper for chat requests, and `max_tokens`.
This privacy-preserving local check does not run a second tokenizer and may reject a prompt near
the actual tokenizer boundary. During generation, both non-empty runtime delta count and reported
completion usage are independently limited by `max_tokens`.

Set `stream` to `true` for `text/event-stream`. Ordered chunks contain text deltas, followed by a
usage chunk, a finish chunk, and `data: [DONE]`. Slow consumers are bounded by the engine event
queue. Client disconnect, server shutdown, and request deadline cancel or fence the generation.

```bash
curl -N http://127.0.0.1:8080/v1/completions \
  -H "content-type: application/json" \
  -d '{"model":"qwen2.5-0.5b-instruct-q4-k-m","prompt":"Count to five:","stream":true}'
```

## WebSocket

Connect to `ws://127.0.0.1:8080/v1/ws`. The server first sends:

```json
{"type":"ready","version":1,"model":"qwen2.5-0.5b-instruct-q4-k-m"}
```

Start completion with one JSON text message:

```json
{"type":"start","id":"example-1","request":{"mode":"completion","model":"qwen2.5-0.5b-instruct-q4-k-m","prompt":"Hello","max_tokens":32}}
```

Use `mode: "chat"` with the chat fields for chat generation. The server emits ordered `delta`
messages and one `final` message containing usage and finish reason. One request may be active per
session. Cancel it with `{"type":"cancel","id":"example-1"}`. Typed `ping` and `close` messages
are also supported.

Frames are limited to 1 MiB, caller IDs to 128 encoded bytes, sessions to a fixed process-local
semaphore, and each session to one active request with a five-minute upper deadline. Binary,
audio, media, tool calls, constrained schemas, embeddings, and llama.cpp administrative routes are
not supported.

## Errors and privacy

HTTP errors use `{"error":{"code":"...","message":"..."}}`. WebSocket errors use a typed
`error` message and echo the caller ID when available. Stable codes include `invalid_request`,
`overloaded`, `cancelled`, `deadline_exceeded`, `not_ready`, `upstream_protocol`, and
`shutting_down`.

Prompts, messages, generated text, internal bearer values, and local paths are not logged. The
public server never exposes the private llama.cpp listener or forwards unsupported runtime routes.
