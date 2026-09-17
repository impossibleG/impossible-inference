//! Stateless, bounded MCP convenience transport over local HTTP.

use axum::{
    Extension, Json,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use impossible_inferences_domain::{FinishReason, GenerationEvent, TokenUsage};
use impossible_inferences_engine::EngineError;
use impossible_inferences_protocol::{ChatCompletionRequest, CompletionRequest, model_id};
use impossible_server_core::RequestContext;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{EngineEventStream, TransportState};

const MCP_VERSION: &str = "2026-07-28";
const JSON_RPC_VERSION: &str = "2.0";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcRequest {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

pub(crate) async fn handle(
    State(state): State<TransportState>,
    Extension(context): Extension<RequestContext>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if headers
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|value| !value.trim().eq_ignore_ascii_case("application/json"))
    {
        return rpc_error(
            &Value::Null,
            -32_600,
            "Content-Type must be application/json",
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        );
    }
    let request: RpcRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return rpc_error(
                &Value::Null,
                -32_700,
                "Parse error",
                StatusCode::BAD_REQUEST,
            );
        }
    };
    if request.jsonrpc != JSON_RPC_VERSION || !valid_id(&request.id) {
        return rpc_error(
            &request.id,
            -32_600,
            "Invalid Request",
            StatusCode::BAD_REQUEST,
        );
    }
    if let Err(message) = validate_headers(&headers, &request) {
        return rpc_error(&request.id, -32_600, message, StatusCode::BAD_REQUEST);
    }
    if !valid_client_meta(&request.params) {
        return rpc_error(
            &request.id,
            -32_602,
            "request _meta must include clientInfo",
            StatusCode::BAD_REQUEST,
        );
    }
    let result = match request.method.as_str() {
        "server/discover" => Ok(discover()),
        "tools/list" => Ok(tool_list()),
        "tools/call" => call_tool(&state, context, &request.params).await,
        "resources/list" => Ok(resource_list()),
        "resources/read" => read_resource(&state, &request.params).await,
        "ping" => Ok(json!({})),
        _ => Err(RpcFailure::new(-32_601, "Method not found")),
    };
    match result {
        Ok(result) => Json(json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": request.id,
            "result": result
        }))
        .into_response(),
        Err(error) => rpc_error(&request.id, error.code, error.message, StatusCode::OK),
    }
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.is_number()
}

pub(crate) fn request_id(body: &Bytes) -> Option<Value> {
    let value: Value = serde_json::from_slice(body).ok()?;
    value.get("id").filter(|id| valid_id(id)).cloned()
}

pub(crate) fn outer_error(
    id: Option<&Value>,
    message: &'static str,
    status: StatusCode,
) -> Response {
    rpc_error(id.unwrap_or(&Value::Null), -32_003, message, status)
}

fn valid_client_meta(params: &Value) -> bool {
    params
        .get("_meta")
        .and_then(|value| value.get("io.modelcontextprotocol/clientInfo"))
        .is_some_and(Value::is_object)
}

fn validate_headers(headers: &HeaderMap, request: &RpcRequest) -> Result<(), &'static str> {
    if headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        != Some(MCP_VERSION)
    {
        return Err("unsupported MCP protocol version");
    }
    if headers
        .get("mcp-method")
        .and_then(|value| value.to_str().ok())
        != Some(request.method.as_str())
    {
        return Err("Mcp-Method does not match the request");
    }
    let expected_name = match request.method.as_str() {
        "tools/call" => request.params.get("name").and_then(Value::as_str),
        "resources/read" => request.params.get("uri").and_then(Value::as_str),
        _ => None,
    };
    let actual_name = headers
        .get("mcp-name")
        .and_then(|value| value.to_str().ok());
    if actual_name != expected_name {
        return Err("Mcp-Name does not match the request");
    }
    Ok(())
}

fn discover() -> Value {
    json!({
        "protocolVersion": MCP_VERSION,
        "serverInfo": {"name": "impossible-inferences", "version": env!("CARGO_PKG_VERSION")},
        "capabilities": {"tools": {}, "resources": {}},
        "instructions": "Local bounded completion and chat over the curated model only."
    })
}

fn tool_list() -> Value {
    json!({
        "tools": [
            {
                "name": "generate_text",
                "description": "Generate a bounded local text completion.",
                "inputSchema": generation_schema(false)
            },
            {
                "name": "chat",
                "description": "Generate a bounded local chat response.",
                "inputSchema": generation_schema(true)
            },
            {
                "name": "list_models",
                "description": "Report the curated local model status.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
            },
            {
                "name": "health",
                "description": "Report privacy-safe local engine health.",
                "inputSchema": {"type": "object", "properties": {}, "additionalProperties": false}
            }
        ],
        "ttlMs": 60_000,
        "cacheScope": "public"
    })
}

fn generation_schema(chat: bool) -> Value {
    let input = if chat {
        json!({
            "messages": {
                "type": "array",
                "minItems": 1,
                "maxItems": 128,
                "items": {
                    "type": "object",
                    "required": ["role", "content"],
                    "properties": {
                        "role": {"enum": ["system", "user", "assistant"]},
                        "content": {"type": "string", "minLength": 1}
                    },
                    "additionalProperties": false
                }
            }
        })
    } else {
        json!({"prompt": {"type": "string", "minLength": 1}})
    };
    let mut properties = json!({
        "model": {"const": model_id()},
        "max_tokens": {"type": "integer", "minimum": 1, "maximum": 1024, "default": 256},
        "temperature": {"type": "number", "minimum": 0, "maximum": 2, "default": 0.8},
        "top_p": {"type": "number", "exclusiveMinimum": 0, "maximum": 1, "default": 0.95},
        "seed": {"type": "integer", "minimum": 0, "maximum": 4_294_967_295_u64},
        "stop": {"oneOf": [
            {"type": "string", "minLength": 1, "maxLength": 128},
            {"type": "array", "maxItems": 8, "items": {"type": "string", "minLength": 1, "maxLength": 128}}
        ]}
    });
    if let (Some(target), Some(source)) = (properties.as_object_mut(), input.as_object()) {
        target.extend(source.clone());
    }
    json!({
        "type": "object",
        "required": if chat { json!(["model", "messages"]) } else { json!(["model", "prompt"]) },
        "properties": properties,
        "additionalProperties": false
    })
}

async fn call_tool(
    state: &TransportState,
    context: RequestContext,
    params: &Value,
) -> Result<Value, RpcFailure> {
    let name = params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcFailure::new(-32_602, "tool name is required"))?;
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    match name {
        "generate_text" => {
            let request: CompletionRequest = serde_json::from_value(arguments)
                .map_err(|_| RpcFailure::new(-32_602, "invalid generate_text arguments"))?;
            if request.stream {
                return Err(RpcFailure::new(-32_602, "MCP tool calls are non-streaming"));
            }
            generation_tool(state, context, request.into_generation()).await
        }
        "chat" => {
            let request: ChatCompletionRequest = serde_json::from_value(arguments)
                .map_err(|_| RpcFailure::new(-32_602, "invalid chat arguments"))?;
            if request.stream {
                return Err(RpcFailure::new(-32_602, "MCP tool calls are non-streaming"));
            }
            generation_tool(state, context, request.into_generation()).await
        }
        "list_models" => {
            empty_arguments(&arguments)?;
            let status = state.engine.status().await;
            Ok(tool_result(&json!({
                "models": if status.ready { json!([{"id": status.profile, "status": "loaded"}]) } else { json!([]) },
                "ready": status.ready
            })))
        }
        "health" => {
            empty_arguments(&arguments)?;
            let status = state.engine.status().await;
            Ok(tool_result(&json!({
                "ready": status.ready,
                "runtime": status.runtime,
                "model": status.model
            })))
        }
        _ => Err(RpcFailure::new(-32_602, "unknown tool")),
    }
}

fn empty_arguments(arguments: &Value) -> Result<(), RpcFailure> {
    if arguments.as_object().is_some_and(serde_json::Map::is_empty) {
        Ok(())
    } else {
        Err(RpcFailure::new(-32_602, "tool takes no arguments"))
    }
}

async fn generation_tool(
    state: &TransportState,
    context: RequestContext,
    request: impossible_inferences_domain::GenerationRequest,
) -> Result<Value, RpcFailure> {
    let events = state
        .engine
        .generate(request, context)
        .map_err(engine_failure)?;
    let completed = collect(events).await.map_err(engine_failure)?;
    Ok(tool_result(&json!({
        "text": completed.text,
        "model": model_id(),
        "finish_reason": finish_reason(completed.finish),
        "usage": {
            "prompt_tokens": completed.usage.prompt_tokens,
            "completion_tokens": completed.usage.completion_tokens,
            "total_tokens": completed.usage.total_tokens
        }
    })))
}

fn tool_result(structured: &Value) -> Value {
    json!({
        "content": [{"type": "text", "text": structured.to_string()}],
        "structuredContent": structured,
        "isError": false
    })
}

struct Completed {
    text: String,
    usage: TokenUsage,
    finish: FinishReason,
}

async fn collect(mut events: Box<dyn EngineEventStream>) -> Result<Completed, EngineError> {
    let mut text = String::new();
    let mut usage = None;
    let mut finish = None;
    while let Some(event) = events.recv().await {
        match event? {
            GenerationEvent::Delta(delta) => text.push_str(&delta),
            GenerationEvent::Usage(value) => usage = Some(value),
            GenerationEvent::Finished(value) => finish = Some(value),
        }
    }
    Ok(Completed {
        text,
        usage: usage.ok_or(EngineError::RuntimeProtocol)?,
        finish: finish.ok_or(EngineError::RuntimeProtocol)?,
    })
}

fn resource_list() -> Value {
    json!({
        "resources": [
            {"uri": "impossible://models", "name": "Local models", "mimeType": "application/json"},
            {"uri": "impossible://health", "name": "Engine health", "mimeType": "application/json"},
            {"uri": "impossible://capabilities", "name": "Server capabilities", "mimeType": "application/json"}
        ],
        "ttlMs": 1_000,
        "cacheScope": "private"
    })
}

async fn read_resource(state: &TransportState, params: &Value) -> Result<Value, RpcFailure> {
    let uri = params
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcFailure::new(-32_602, "resource URI is required"))?;
    let status = state.engine.status().await;
    let value = match uri {
        "impossible://models" => json!({
            "models": if status.ready { json!([{"id": status.profile, "status": "loaded"}]) } else { json!([]) }
        }),
        "impossible://health" => json!({
            "ready": status.ready,
            "runtime": status.runtime,
            "model": status.model
        }),
        "impossible://capabilities" => json!({
            "model": model_id(),
            "generation": ["completion", "chat"],
            "transports": ["http", "sse", "websocket", "grpc", "mcp"],
            "deferred": ["tool_calls", "schema_output", "embeddings", "media", "admin"]
        }),
        _ => return Err(RpcFailure::new(-32_602, "resource not found")),
    };
    Ok(json!({
        "contents": [{"uri": uri, "mimeType": "application/json", "text": value.to_string()}]
    }))
}

const fn finish_reason(reason: FinishReason) -> &'static str {
    match reason {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::Cancelled => "cancelled",
    }
}

fn engine_failure(error: EngineError) -> RpcFailure {
    let message = match error {
        EngineError::InvalidRequest(_) => "invalid generation request",
        EngineError::Overloaded => "generation capacity exhausted",
        EngineError::Cancelled => "request cancelled",
        EngineError::DeadlineExceeded => "request deadline exceeded",
        EngineError::ArtifactsUnavailable | EngineError::RuntimeUnavailable => {
            "local runtime unavailable"
        }
        EngineError::ShuttingDown => "server is shutting down",
        EngineError::Configuration | EngineError::RuntimeProtocol => "local runtime failed",
    };
    RpcFailure::new(-32_003, message)
}

struct RpcFailure {
    code: i32,
    message: &'static str,
}

impl RpcFailure {
    const fn new(code: i32, message: &'static str) -> Self {
        Self { code, message }
    }
}

fn rpc_error(id: &Value, code: i32, message: &'static str, status: StatusCode) -> Response {
    (
        status,
        Json(json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": id,
            "error": {"code": code, "message": message}
        })),
    )
        .into_response()
}
