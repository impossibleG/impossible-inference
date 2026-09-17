//! Bounded HTTP control plane for Impossible Inferences.

mod grpc;
mod mcp;

use std::{
    convert::Infallible,
    future::{Future, IntoFuture, poll_fn},
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension, Json, Router,
    body::{self, Body, Bytes},
    extract::{
        Request, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use futures_util::{StreamExt, stream};
use impossible_inferences_domain::{FinishReason, GenerationEvent, GenerationRequest, TokenUsage};
use impossible_inferences_engine::{EngineError, EngineStatus, GenerationEngine, GenerationStream};
use impossible_inferences_protocol::{
    ChatCompletionRequest, CompletionRequest, WebSocketClientMessage, WebSocketGeneration,
    WebSocketServerMessage, model_id,
};
use impossible_server_core::{
    CancellationToken, DrainOutcome, HealthRegistry, ProcessState, ReadinessReason, RequestContext,
    RequestIdSource, ServerLimits, ShutdownGate,
};
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, timeout_at},
};

/// Boxed shutdown future returned by a workload implementation.
pub type ShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// Boxed engine future used by transport adapters and test doubles.
pub type EngineFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Transport-owned view of one bounded engine event stream.
pub trait EngineEventStream: Send {
    /// Receives the next ordered event.
    fn recv(&mut self) -> EngineFuture<'_, Option<Result<GenerationEvent, EngineError>>>;
}

impl EngineEventStream for GenerationStream {
    fn recv(&mut self) -> EngineFuture<'_, Option<Result<GenerationEvent, EngineError>>> {
        Box::pin(GenerationStream::recv(self))
    }
}

/// Minimal generation boundary consumed by public transports.
pub trait InferenceEngine: Send + Sync + 'static {
    /// Returns current privacy-safe state.
    fn status(&self) -> EngineFuture<'_, EngineStatus>;

    /// Admits one normalized generation request.
    ///
    /// # Errors
    /// Returns a stable engine error for invalid input, bounded overload, or unavailable runtime.
    fn generate(
        &self,
        request: GenerationRequest,
        context: RequestContext,
    ) -> Result<Box<dyn EngineEventStream>, EngineError>;

    /// Terminates the owned runtime.
    fn shutdown(&self) -> EngineFuture<'_, Result<(), EngineError>>;
}

impl InferenceEngine for GenerationEngine {
    fn status(&self) -> EngineFuture<'_, EngineStatus> {
        Box::pin(GenerationEngine::status(self))
    }

    fn generate(
        &self,
        request: GenerationRequest,
        context: RequestContext,
    ) -> Result<Box<dyn EngineEventStream>, EngineError> {
        GenerationEngine::generate(self, request, context)
            .map(|stream| Box::new(stream) as Box<dyn EngineEventStream>)
    }

    fn shutdown(&self) -> EngineFuture<'_, Result<(), EngineError>> {
        Box::pin(GenerationEngine::shutdown(self))
    }
}

/// Server-owned context exposed to a workload extension.
#[derive(Debug, Clone)]
pub struct WorkloadContext {
    health: HealthRegistry,
    component: Arc<str>,
    limits: ServerLimits,
    shutdown: CancellationToken,
}

impl WorkloadContext {
    /// Publishes aggregate workload readiness.
    pub fn set_ready(&self, ready: bool) {
        let _ = self.health.set_component_ready(&self.component, ready);
    }

    /// Returns the validated resource and lifecycle bounds applied by the host.
    #[must_use]
    pub const fn limits(&self) -> ServerLimits {
        self.limits
    }

    /// Returns a terminal token cancelled after admitted work drains, when drain is forced, or
    /// when the host is dropped. Use request admission responses—not this token—to observe the
    /// instant admission closes.
    #[must_use]
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

/// Compile-time seam implemented by the service-specific workload.
pub trait Workload: Send + Sync + 'static {
    /// Stable internal component name. It is not exposed by public health endpoints.
    fn component_name(&self) -> &'static str;

    /// Returns workload-specific routes with any private state captured by the implementation.
    fn routes(&self, context: WorkloadContext) -> Router;

    /// Whether the private, transport-neutral generation boundary is available.
    fn engine_available(&self) -> bool {
        false
    }

    /// Releases workload resources after HTTP admission has stopped.
    ///
    /// The host polls this future at least once but drops it at the configured shutdown deadline.
    /// `force` is already cancelled when HTTP drain was forced; it is cancelled if cleanup itself
    /// reaches the deadline. Implementations should hand the token to retained native or remote
    /// work before their first suspension.
    fn shutdown(&self, force: CancellationToken) -> ShutdownFuture<'_> {
        let _ = force;
        Box::pin(async {})
    }
}

/// Honest pre-installation workload used until the generation adapter is configured.
#[derive(Debug, Default)]
pub struct PendingInference;

impl Workload for PendingInference {
    fn component_name(&self) -> &'static str {
        "inference-runtime"
    }

    fn routes(&self, context: WorkloadContext) -> Router {
        context.set_ready(false);
        Router::new()
            .route("/status", get(pending_status))
            .route("/v1/models", get(pending_models))
    }
}

/// Ready local generation workload with bounded public HTTP and WebSocket transports.
#[derive(Clone)]
pub struct LocalInference {
    state: TransportState,
}

impl LocalInference {
    /// Wraps a successfully started private generation engine.
    #[must_use]
    pub fn new(engine: impl InferenceEngine) -> Self {
        Self {
            state: TransportState {
                engine: Arc::new(engine),
                websocket_sessions: Arc::new(Semaphore::new(16)),
                websocket_request_ids: RequestIdSource::default(),
            },
        }
    }

    /// Serves the versioned local gRPC API on a pre-bound listener until cancellation.
    ///
    /// # Errors
    /// Returns an I/O error if the HTTP/2 server cannot run or shut down cleanly.
    pub async fn serve_grpc(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> io::Result<()> {
        grpc::serve(self.state.engine.clone(), listener, shutdown).await
    }
}

impl std::fmt::Debug for LocalInference {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LocalInference")
            .field("engine", &"redacted")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct TransportState {
    engine: Arc<dyn InferenceEngine>,
    websocket_sessions: Arc<Semaphore>,
    websocket_request_ids: RequestIdSource,
}

impl Workload for LocalInference {
    fn component_name(&self) -> &'static str {
        "inference-runtime"
    }

    fn routes(&self, context: WorkloadContext) -> Router {
        let monitor_engine = self.state.engine.clone();
        let monitor_context = context.clone();
        let terminal = context.shutdown_token();
        tokio::spawn(async move {
            loop {
                monitor_context.set_ready(monitor_engine.status().await.ready);
                tokio::select! {
                    () = terminal.cancelled() => {
                        monitor_context.set_ready(false);
                        return;
                    }
                    () = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                }
            }
        });
        Router::new()
            .route("/status", get(local_status))
            .route("/v1/models", get(local_models))
            .route("/v1/completions", post(completions))
            .route("/v1/chat/completions", post(chat_completions))
            .route("/v1/ws", get(websocket_upgrade))
            .route("/mcp", post(mcp::handle))
            .with_state(self.state.clone())
    }

    fn engine_available(&self) -> bool {
        true
    }

    fn shutdown(&self, _force: CancellationToken) -> ShutdownFuture<'_> {
        Box::pin(async move {
            let _ = self.state.engine.shutdown().await;
        })
    }
}

#[derive(Debug, Default)]
struct Metrics {
    control_requests: AtomicU64,
}

impl Metrics {
    fn increment(&self) {
        self.control_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self) -> String {
        format!(
            "# HELP impossible_inferences_control_requests_total Control-plane HTTP requests.\n\
             # TYPE impossible_inferences_control_requests_total counter\n\
             impossible_inferences_control_requests_total {}\n",
            self.control_requests.load(Ordering::Relaxed)
        )
    }
}

#[derive(Debug)]
struct AppState {
    health: HealthRegistry,
    metrics: Metrics,
    engine_available: bool,
}

#[derive(Debug, Clone)]
struct WorkloadMiddlewareState {
    limits: ServerLimits,
    request_ids: RequestIdSource,
    shutdown_gate: ShutdownGate,
    execution: Arc<Semaphore>,
    admitted: Arc<Semaphore>,
}

#[derive(Debug)]
struct RequestCancellationGuard(CancellationToken);

impl Drop for RequestCancellationGuard {
    fn drop(&mut self) {
        let _ = self.0.cancel();
    }
}

#[derive(Debug, Serialize)]
struct HealthBody {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct PublicErrorBody {
    code: &'static str,
    message: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorEnvelope {
    error: PublicErrorBody,
}

#[derive(Debug, Serialize)]
struct PendingStatusBody {
    status: &'static str,
    runtime: &'static str,
    model: &'static str,
    ready: bool,
}

/// Assembled HTTP server around one workload implementation.
pub struct InferenceServer<W: Workload> {
    state: Arc<AppState>,
    workload: Arc<W>,
    router: Router,
    limits: ServerLimits,
    shutdown_gate: ShutdownGate,
}

impl<W: Workload> InferenceServer<W> {
    /// Creates a server and lets the workload install its routes.
    #[must_use]
    pub fn new(workload: W) -> Self {
        Self::with_limits(workload, ServerLimits::default())
    }

    /// Creates a server with an explicit, validated resource and lifecycle policy.
    #[must_use]
    pub fn with_limits(workload: W, limits: ServerLimits) -> Self {
        let health = HealthRegistry::new();
        health.register_component(workload.component_name());
        let shutdown_gate = ShutdownGate::new();
        let context = WorkloadContext {
            health: health.clone(),
            component: Arc::from(workload.component_name()),
            limits,
            shutdown: shutdown_gate.stop_token(),
        };
        let workload = Arc::new(workload);
        let state = Arc::new(AppState {
            health,
            metrics: Metrics::default(),
            engine_available: workload.engine_available(),
        });
        let middleware_state = WorkloadMiddlewareState {
            limits,
            request_ids: RequestIdSource::default(),
            shutdown_gate: shutdown_gate.clone(),
            execution: Arc::new(Semaphore::new(limits.max_concurrent_requests())),
            admitted: Arc::new(Semaphore::new(
                limits
                    .max_concurrent_requests()
                    .saturating_add(limits.queue_capacity()),
            )),
        };
        let workload_routes = workload
            .routes(context)
            .route_layer(middleware::from_fn_with_state(
                middleware_state,
                enforce_workload_policy,
            ));
        let router = Router::new()
            .route("/", get(root))
            .route("/version", get(version))
            .route("/v1/capabilities", get(capabilities))
            .route("/health/live", get(live))
            .route("/health/ready", get(ready))
            .route("/metrics", get(metrics))
            .with_state(state.clone())
            .merge(workload_routes)
            .fallback(not_found)
            .layer(middleware::from_fn(normalize_public_failures));
        state.health.set_process(ProcessState::Running);
        Self {
            state,
            workload,
            router,
            limits,
            shutdown_gate,
        }
    }

    /// Returns a clone of the assembled router for in-process tests or embedding in another host.
    pub fn router(&self) -> Router {
        self.router.clone()
    }

    /// Serves a pre-bound listener until cancellation, then shuts down the workload cleanly.
    ///
    /// # Errors
    /// Returns an I/O error from the HTTP server.
    pub async fn serve(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> std::io::Result<()> {
        let http_shutdown = CancellationToken::new();
        let server_shutdown = http_shutdown.clone();
        let server = axum::serve(listener, self.router)
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .into_future();
        tokio::pin!(server);
        tokio::select! {
            result = &mut server => {
                self.state.health.set_process(ProcessState::Draining);
                self.shutdown_gate.stop_now();
                let deadline = Instant::now() + self.limits.shutdown_timeout();
                let force = CancellationToken::new();
                if !poll_workload_shutdown(self.workload.as_ref(), force, deadline).await {
                    self.state.health.set_process(ProcessState::Stopped);
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "workload shutdown exceeded its configured bound"));
                }
                self.state.health.set_process(ProcessState::Stopped);
                result
            }
            () = shutdown.cancelled() => {
                self.state.health.set_process(ProcessState::Draining);
                let deadline = Instant::now() + self.limits.shutdown_timeout();
                let _ = http_shutdown.cancel();
                let drain_outcome = self.shutdown_gate.drain(self.limits.shutdown_timeout()).await;
                let http_timed_out = timeout_at(deadline, &mut server).await.is_err();
                let http_forced = drain_outcome == DrainOutcome::Forced || http_timed_out;
                if http_forced {
                    self.shutdown_gate.stop_now();
                }
                let force = CancellationToken::new();
                if http_forced {
                    let _ = force.cancel();
                }
                let cleanup_completed = poll_workload_shutdown(self.workload.as_ref(), force, deadline).await;
                self.state.health.set_process(ProcessState::Stopped);
                if http_forced {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "HTTP drain exceeded its configured bound"));
                }
                if !cleanup_completed {
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "workload shutdown exceeded its configured bound"));
                }
                Ok(())
            }
        }
    }
}

async fn poll_workload_shutdown<W: Workload>(
    workload: &W,
    force: CancellationToken,
    deadline: Instant,
) -> bool {
    let mut cleanup = workload.shutdown(force.clone());
    let completed =
        poll_fn(|context| Poll::Ready(matches!(cleanup.as_mut().poll(context), Poll::Ready(()))))
            .await;
    if completed {
        return true;
    }
    if Instant::now() >= deadline {
        let _ = force.cancel();
        return false;
    }
    if timeout_at(deadline, cleanup).await.is_ok() {
        true
    } else {
        let _ = force.cancel();
        false
    }
}

async fn enforce_workload_policy(
    State(state): State<WorkloadMiddlewareState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(_work_guard) = state.shutdown_gate.try_enter() else {
        return public_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "cancelled",
            "the request was cancelled",
        );
    };
    let Ok(_admission_permit) = state.admitted.clone().try_acquire_owned() else {
        return public_error(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "the service is temporarily overloaded",
        );
    };

    let cancellation = CancellationToken::new();
    let cancel_on_drop = RequestCancellationGuard(cancellation.clone());
    let Ok(request_id) = state.request_ids.next() else {
        return public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "an internal server error occurred",
        );
    };
    let Ok(context) = RequestContext::new(
        request_id,
        cancellation.clone(),
        Some(state.limits.request_timeout()),
    ) else {
        return public_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "an internal server error occurred",
        );
    };
    let deadline = Instant::now() + state.limits.request_timeout();
    let stop = state.shutdown_gate.stop_token();
    let (mut parts, body) = request.into_parts();
    let body = tokio::select! {
        biased;
        () = stop.cancelled() => {
            let _ = cancellation.cancel();
            return public_error(StatusCode::SERVICE_UNAVAILABLE, "cancelled", "the request was cancelled");
        }
        result = timeout_at(deadline, body::to_bytes(body, state.limits.max_request_bytes())) => {
            match result {
                Ok(Ok(bytes)) => bytes,
                Ok(Err(_)) => return public_error(StatusCode::PAYLOAD_TOO_LARGE, "invalid_request", "the request body exceeds the configured limit"),
                Err(_) => {
                    let _ = cancellation.cancel();
                    return public_error(StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded", "the request deadline was exceeded");
                }
            }
        }
    };
    parts.extensions.insert(context);
    let request = Request::from_parts(parts, body::Body::from(body));

    let execution = tokio::select! {
        biased;
        () = stop.cancelled() => {
            let _ = cancellation.cancel();
            return public_error(StatusCode::SERVICE_UNAVAILABLE, "cancelled", "the request was cancelled");
        }
        result = timeout_at(deadline, state.execution.clone().acquire_owned()) => {
            match result {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => return public_error(StatusCode::SERVICE_UNAVAILABLE, "cancelled", "the request was cancelled"),
                Err(_) => {
                    let _ = cancellation.cancel();
                    return public_error(StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded", "the request deadline was exceeded");
                }
            }
        }
    };

    let mut response = tokio::select! {
        biased;
        () = stop.cancelled() => {
            let _ = cancellation.cancel();
            public_error(StatusCode::SERVICE_UNAVAILABLE, "cancelled", "the request was cancelled")
        }
        () = tokio::time::sleep_until(deadline) => {
            let _ = cancellation.cancel();
            public_error(StatusCode::GATEWAY_TIMEOUT, "deadline_exceeded", "the request deadline was exceeded")
        }
        response = next.run(request) => response,
    };
    if let Ok(value) = request_id.get().to_string().parse() {
        response.headers_mut().insert("x-request-id", value);
    }
    drop(execution);
    let (parts, body) = response.into_parts();
    let data = body.into_data_stream();
    let guarded = stream::unfold(
        (data, cancel_on_drop),
        |(mut data, cancel_on_drop)| async move {
            data.next().await.map(|item| (item, (data, cancel_on_drop)))
        },
    );
    Response::from_parts(parts, Body::from_stream(guarded))
}

async fn normalize_public_failures(request: Request, next: Next) -> Response {
    let response = next.run(request).await;
    match response.status() {
        StatusCode::METHOD_NOT_ALLOWED => public_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "invalid_request",
            "the request method is not supported",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => public_error(
            StatusCode::PAYLOAD_TOO_LARGE,
            "invalid_request",
            "the request body exceeds the configured limit",
        ),
        _ => response,
    }
}

async fn not_found() -> Response {
    public_error(
        StatusCode::NOT_FOUND,
        "invalid_request",
        "the requested path does not exist",
    )
}

fn public_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        Json(ErrorEnvelope {
            error: PublicErrorBody { code, message },
        }),
    )
        .into_response()
}

async fn root(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    state.metrics.increment();
    Json(serde_json::json!({
        "name": "impossible-inferences-server",
        "status": "ok"
    }))
}

async fn version(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    state.metrics.increment();
    Json(serde_json::json!({
        "name": "impossible-inferences",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn capabilities(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    state.metrics.increment();
    let available: &[&str] = if state.engine_available {
        &[
            "generation_engine",
            "completions",
            "chat_completions",
            "sse",
            "websocket",
            "grpc",
            "mcp",
        ]
    } else {
        &[]
    };
    let pending: &[&str] = if state.engine_available {
        &[]
    } else {
        &[
            "generation_engine",
            "completions",
            "chat_completions",
            "sse",
            "websocket",
            "grpc",
            "mcp",
        ]
    };
    Json(serde_json::json!({
        "schema_version": 1,
        "product": "impossible-inferences",
        "version": env!("CARGO_PKG_VERSION"),
        "available": available,
        "pending": pending
    }))
}

async fn local_status(State(state): State<TransportState>) -> Json<serde_json::Value> {
    let status = state.engine.status().await;
    Json(serde_json::json!({
        "status": if status.ready { "ready" } else { "not_ready" },
        "runtime": status.runtime,
        "model": status.model,
        "profile": status.profile,
        "ready": status.ready,
        "public_generation_transports": ["http", "sse", "websocket", "grpc", "mcp"]
    }))
}

async fn local_models(State(state): State<TransportState>) -> Json<serde_json::Value> {
    let status = state.engine.status().await;
    let data = if status.ready {
        vec![serde_json::json!({
            "id": status.profile,
            "object": "model",
            "owned_by": "local"
        })]
    } else {
        Vec::new()
    };
    Json(serde_json::json!({
        "object": "list",
        "data": data,
        "status": if status.ready { "loaded" } else { "not_ready" }
    }))
}

#[derive(Debug, Clone, Copy)]
enum ResponseMode {
    Completion,
    Chat,
}

async fn completions(
    State(state): State<TransportState>,
    Extension(context): Extension<RequestContext>,
    body: Bytes,
) -> Response {
    let request: CompletionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return public_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the request body is not valid for this endpoint",
            );
        }
    };
    let streaming = request.stream;
    generation_response(
        state,
        context,
        request.into_generation(),
        ResponseMode::Completion,
        streaming,
    )
    .await
}

async fn chat_completions(
    State(state): State<TransportState>,
    Extension(context): Extension<RequestContext>,
    body: Bytes,
) -> Response {
    let request: ChatCompletionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(_) => {
            return public_error(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "the request body is not valid for this endpoint",
            );
        }
    };
    let streaming = request.stream;
    generation_response(
        state,
        context,
        request.into_generation(),
        ResponseMode::Chat,
        streaming,
    )
    .await
}

async fn generation_response(
    state: TransportState,
    context: RequestContext,
    request: GenerationRequest,
    mode: ResponseMode,
    streaming: bool,
) -> Response {
    let request_id = format_request_id(mode, context.id().get());
    let events = match state.engine.generate(request, context) {
        Ok(events) => events,
        Err(error) => return engine_error_response(error),
    };
    if streaming {
        return sse_response(events, request_id, mode);
    }
    collect_response(events, request_id, mode).await
}

async fn collect_response(
    mut events: Box<dyn EngineEventStream>,
    request_id: String,
    mode: ResponseMode,
) -> Response {
    let mut text = String::new();
    let mut usage = None;
    let mut finish = None;
    while let Some(event) = events.recv().await {
        match event {
            Ok(GenerationEvent::Delta(delta)) => text.push_str(&delta),
            Ok(GenerationEvent::Usage(value)) => usage = Some(value),
            Ok(GenerationEvent::Finished(reason)) => finish = Some(reason),
            Err(error) => return engine_error_response(error),
        }
    }
    let (Some(usage), Some(finish)) = (usage, finish) else {
        return engine_error_response(EngineError::RuntimeProtocol);
    };
    let choice = match mode {
        ResponseMode::Completion => serde_json::json!({
            "text": text,
            "index": 0,
            "logprobs": null,
            "finish_reason": finish_reason(finish)
        }),
        ResponseMode::Chat => serde_json::json!({
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": finish_reason(finish)
        }),
    };
    Json(serde_json::json!({
        "id": request_id,
        "object": match mode {
            ResponseMode::Completion => "text_completion",
            ResponseMode::Chat => "chat.completion",
        },
        "created": unix_timestamp(),
        "model": model_id(),
        "choices": [choice],
        "usage": usage_json(usage)
    }))
    .into_response()
}

struct SseState {
    events: Box<dyn EngineEventStream>,
    request_id: String,
    mode: ResponseMode,
    send_done: bool,
    finished: bool,
}

fn sse_response(
    events: Box<dyn EngineEventStream>,
    request_id: String,
    mode: ResponseMode,
) -> Response {
    let stream = stream::unfold(
        SseState {
            events,
            request_id,
            mode,
            send_done: false,
            finished: false,
        },
        |mut state| async move {
            if state.finished {
                return None;
            }
            if state.send_done {
                state.finished = true;
                return Some((Ok::<_, Infallible>(Event::default().data("[DONE]")), state));
            }
            let event = state.events.recv().await;
            let output = match event {
                Some(Ok(GenerationEvent::Delta(delta))) => Event::default().data(stream_chunk(
                    &state.request_id,
                    state.mode,
                    Some(&delta),
                    None,
                    None,
                )),
                Some(Ok(GenerationEvent::Usage(usage))) => Event::default().data(stream_chunk(
                    &state.request_id,
                    state.mode,
                    None,
                    None,
                    Some(usage),
                )),
                Some(Ok(GenerationEvent::Finished(reason))) => {
                    state.send_done = true;
                    Event::default().data(stream_chunk(
                        &state.request_id,
                        state.mode,
                        None,
                        Some(reason),
                        None,
                    ))
                }
                Some(Err(error)) => {
                    state.finished = true;
                    let (_, code, message) = engine_error(error);
                    Event::default().event("error").data(
                        serde_json::json!({"error": {"code": code, "message": message}})
                            .to_string(),
                    )
                }
                None => {
                    state.finished = true;
                    Event::default().event("error").data(
                        serde_json::json!({"error": {"code": "upstream_protocol", "message": "the local runtime ended unexpectedly"}})
                            .to_string(),
                    )
                }
            };
            Some((Ok(output), state))
        },
    );
    Sse::new(stream)
        .keep_alive(
            KeepAlive::new()
                .interval(Duration::from_millis(100))
                .text("keepalive"),
        )
        .into_response()
}

fn stream_chunk(
    request_id: &str,
    mode: ResponseMode,
    delta: Option<&str>,
    finish: Option<FinishReason>,
    usage: Option<TokenUsage>,
) -> String {
    let choices = if usage.is_some() {
        Vec::new()
    } else {
        vec![match mode {
            ResponseMode::Completion => serde_json::json!({
                "text": delta.unwrap_or_default(),
                "index": 0,
                "logprobs": null,
                "finish_reason": finish.map(finish_reason)
            }),
            ResponseMode::Chat => serde_json::json!({
                "index": 0,
                "delta": if let Some(delta) = delta {
                    serde_json::json!({"content": delta})
                } else {
                    serde_json::json!({})
                },
                "finish_reason": finish.map(finish_reason)
            }),
        }]
    };
    let mut chunk = serde_json::json!({
        "id": request_id,
        "object": match mode {
            ResponseMode::Completion => "text_completion",
            ResponseMode::Chat => "chat.completion.chunk",
        },
        "created": unix_timestamp(),
        "model": model_id(),
        "choices": choices
    });
    if let Some(usage) = usage {
        chunk["usage"] = usage_json(usage);
    }
    chunk.to_string()
}

fn engine_error_response(error: EngineError) -> Response {
    let (status, code, message) = engine_error(error);
    public_error(status, code, message)
}

fn engine_error(error: EngineError) -> (StatusCode, &'static str, &'static str) {
    match error {
        EngineError::InvalidRequest(_) => (
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "the generation request is invalid",
        ),
        EngineError::Overloaded => (
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "the service is temporarily overloaded",
        ),
        EngineError::Cancelled => (
            StatusCode::from_u16(499).unwrap_or(StatusCode::BAD_REQUEST),
            "cancelled",
            "the request was cancelled",
        ),
        EngineError::DeadlineExceeded => (
            StatusCode::GATEWAY_TIMEOUT,
            "deadline_exceeded",
            "the request deadline was exceeded",
        ),
        EngineError::ArtifactsUnavailable | EngineError::RuntimeUnavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "not_ready",
            "the local inference runtime is unavailable",
        ),
        EngineError::RuntimeProtocol => (
            StatusCode::BAD_GATEWAY,
            "upstream_protocol",
            "the local runtime returned an invalid response",
        ),
        EngineError::ShuttingDown => (
            StatusCode::SERVICE_UNAVAILABLE,
            "shutting_down",
            "the service is shutting down",
        ),
        EngineError::Configuration => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "an internal server error occurred",
        ),
    }
}

const fn finish_reason(reason: FinishReason) -> &'static str {
    match reason {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::Cancelled => "cancelled",
    }
}

fn usage_json(usage: TokenUsage) -> serde_json::Value {
    serde_json::json!({
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens
    })
}

fn format_request_id(mode: ResponseMode, id: u64) -> String {
    format!(
        "{}-{id}",
        match mode {
            ResponseMode::Completion => "cmpl",
            ResponseMode::Chat => "chatcmpl",
        }
    )
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

async fn websocket_upgrade(
    State(state): State<TransportState>,
    upgrade: WebSocketUpgrade,
) -> Response {
    let Ok(permit) = state.websocket_sessions.clone().try_acquire_owned() else {
        return public_error(
            StatusCode::TOO_MANY_REQUESTS,
            "overloaded",
            "the WebSocket session limit is reached",
        );
    };
    upgrade
        .max_message_size(1_048_576)
        .max_frame_size(1_048_576)
        .on_upgrade(move |socket| websocket_session(socket, state, permit))
        .into_response()
}

struct ActiveWebSocketGeneration {
    id: String,
    cancellation: CancellationToken,
    events: Box<dyn EngineEventStream>,
    usage: Option<TokenUsage>,
}

enum WebSocketInput {
    Socket(Option<Result<Message, axum::Error>>),
    Engine(Option<Result<GenerationEvent, EngineError>>),
}

async fn websocket_session(
    mut socket: WebSocket,
    state: TransportState,
    _permit: OwnedSemaphorePermit,
) {
    if send_websocket(
        &mut socket,
        &WebSocketServerMessage::Ready {
            version: 1,
            model: model_id(),
        },
    )
    .await
    .is_err()
    {
        return;
    }
    let mut active: Option<ActiveWebSocketGeneration> = None;
    loop {
        if active.is_none() {
            let Some(message) = socket.next().await else {
                return;
            };
            let Ok(message) = message else {
                return;
            };
            if !handle_websocket_message(&mut socket, &state, &mut active, message).await {
                return;
            }
            continue;
        }
        let input = tokio::select! {
            message = socket.next() => WebSocketInput::Socket(message),
            event = async {
                match active.as_mut() {
                    Some(active) => active.events.recv().await,
                    None => None,
                }
            } => WebSocketInput::Engine(event),
        };
        match input {
            WebSocketInput::Socket(Some(Ok(message))) => {
                if !handle_websocket_message(&mut socket, &state, &mut active, message).await {
                    return;
                }
            }
            WebSocketInput::Socket(_) => {
                cancel_active(&mut active);
                return;
            }
            WebSocketInput::Engine(event) => {
                if !handle_websocket_event(&mut socket, &mut active, event).await {
                    return;
                }
            }
        }
    }
}

async fn handle_websocket_message(
    socket: &mut WebSocket,
    state: &TransportState,
    active: &mut Option<ActiveWebSocketGeneration>,
    message: Message,
) -> bool {
    match message {
        Message::Text(text) => {
            let command: WebSocketClientMessage = match serde_json::from_str(text.as_str()) {
                Ok(command) => command,
                Err(_) => {
                    return send_websocket_error(
                        socket,
                        None,
                        "invalid_request",
                        "the WebSocket message is invalid",
                    )
                    .await;
                }
            };
            match command {
                WebSocketClientMessage::Start { id, request } => {
                    start_websocket_generation(socket, state, active, id, request).await
                }
                WebSocketClientMessage::Cancel { id } => {
                    if active.as_ref().is_some_and(|request| request.id == id) {
                        cancel_active(active);
                        send_websocket_error(
                            socket,
                            Some(&id),
                            "cancelled",
                            "the request was cancelled",
                        )
                        .await
                    } else {
                        send_websocket_error(
                            socket,
                            Some(&id),
                            "invalid_request",
                            "no matching request is active",
                        )
                        .await
                    }
                }
                WebSocketClientMessage::Ping => {
                    send_websocket(socket, &WebSocketServerMessage::Pong)
                        .await
                        .is_ok()
                }
                WebSocketClientMessage::Close => {
                    cancel_active(active);
                    let _ = socket.send(Message::Close(None)).await;
                    false
                }
            }
        }
        Message::Binary(_) => {
            send_websocket_error(
                socket,
                None,
                "invalid_request",
                "binary and media messages are not supported",
            )
            .await
        }
        Message::Ping(value) => socket.send(Message::Pong(value)).await.is_ok(),
        Message::Pong(_) => true,
        Message::Close(_) => {
            cancel_active(active);
            false
        }
    }
}

async fn start_websocket_generation(
    socket: &mut WebSocket,
    state: &TransportState,
    active: &mut Option<ActiveWebSocketGeneration>,
    id: String,
    request: WebSocketGeneration,
) -> bool {
    if id.is_empty() || id.len() > 128 {
        return send_websocket_error(
            socket,
            None,
            "invalid_request",
            "the request id is outside the supported bound",
        )
        .await;
    }
    if active.is_some() {
        return send_websocket_error(
            socket,
            Some(&id),
            "overloaded",
            "only one request may be active in a session",
        )
        .await;
    }
    let Ok(internal_id) = state.websocket_request_ids.next() else {
        return send_websocket_error(
            socket,
            Some(&id),
            "internal",
            "an internal server error occurred",
        )
        .await;
    };
    let cancellation = CancellationToken::new();
    let Ok(context) = RequestContext::new(
        internal_id,
        cancellation.clone(),
        Some(Duration::from_secs(300)),
    ) else {
        return send_websocket_error(
            socket,
            Some(&id),
            "internal",
            "an internal server error occurred",
        )
        .await;
    };
    match state.engine.generate(request.into_generation(), context) {
        Ok(events) => {
            *active = Some(ActiveWebSocketGeneration {
                id,
                cancellation,
                events,
                usage: None,
            });
            true
        }
        Err(error) => {
            let (_, code, message) = engine_error(error);
            send_websocket_error(socket, Some(&id), code, message).await
        }
    }
}

async fn handle_websocket_event(
    socket: &mut WebSocket,
    active: &mut Option<ActiveWebSocketGeneration>,
    event: Option<Result<GenerationEvent, EngineError>>,
) -> bool {
    match event {
        Some(Ok(GenerationEvent::Delta(delta))) => {
            let Some(request) = active.as_ref() else {
                return false;
            };
            send_websocket(
                socket,
                &WebSocketServerMessage::Delta {
                    id: &request.id,
                    delta: &delta,
                },
            )
            .await
            .is_ok()
        }
        Some(Ok(GenerationEvent::Usage(usage))) => {
            let Some(request) = active.as_mut() else {
                return false;
            };
            request.usage = Some(usage);
            true
        }
        Some(Ok(GenerationEvent::Finished(reason))) => {
            let Some(request) = active.take() else {
                return false;
            };
            let Some(usage) = request.usage else {
                return send_websocket_error(
                    socket,
                    Some(&request.id),
                    "upstream_protocol",
                    "the local runtime ended unexpectedly",
                )
                .await;
            };
            send_websocket(
                socket,
                &WebSocketServerMessage::Final {
                    id: &request.id,
                    finish_reason: finish_reason(reason),
                    prompt_tokens: usage.prompt_tokens,
                    completion_tokens: usage.completion_tokens,
                    total_tokens: usage.total_tokens,
                },
            )
            .await
            .is_ok()
        }
        Some(Err(error)) => {
            let Some(request) = active.take() else {
                return false;
            };
            let (_, code, message) = engine_error(error);
            send_websocket_error(socket, Some(&request.id), code, message).await
        }
        None => {
            let id = active.take().map(|request| request.id);
            send_websocket_error(
                socket,
                id.as_deref(),
                "upstream_protocol",
                "the local runtime ended unexpectedly",
            )
            .await
        }
    }
}

fn cancel_active(active: &mut Option<ActiveWebSocketGeneration>) {
    if let Some(request) = active.take() {
        let _ = request.cancellation.cancel();
    }
}

async fn send_websocket(
    socket: &mut WebSocket,
    message: &WebSocketServerMessage<'_>,
) -> Result<(), ()> {
    let text = serde_json::to_string(message).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

async fn send_websocket_error(
    socket: &mut WebSocket,
    id: Option<&str>,
    code: &'static str,
    message: &'static str,
) -> bool {
    send_websocket(socket, &WebSocketServerMessage::Error { id, code, message })
        .await
        .is_ok()
}

async fn pending_status() -> Json<PendingStatusBody> {
    Json(PendingStatusBody {
        status: "not_ready",
        runtime: "not_installed",
        model: "not_installed",
        ready: false,
    })
}

async fn pending_models() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "object": "list",
        "data": [],
        "status": "not_installed"
    }))
}

async fn live(State(state): State<Arc<AppState>>) -> Response {
    state.metrics.increment();
    let snapshot = state.health.snapshot();
    let status = if snapshot.live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(HealthBody {
            status: if snapshot.live { "live" } else { "stopped" },
            reason: None,
        }),
    )
        .into_response()
}

async fn ready(State(state): State<Arc<AppState>>) -> Response {
    state.metrics.increment();
    let snapshot = state.health.snapshot();
    let status = if snapshot.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(HealthBody {
            status: if snapshot.ready { "ready" } else { "not_ready" },
            reason: snapshot.reason.map(ReadinessReason::as_str),
        }),
    )
        .into_response()
}

async fn metrics(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    state.metrics.increment();
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future,
        net::SocketAddr,
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Extension, Router,
        body::Body,
        http::{Method, Request, StatusCode},
        response::Response,
        routing::get,
    };
    use futures_util::{SinkExt, StreamExt};
    use http_body_util::BodyExt;
    use impossible_inferences_domain::{
        CURATED_MODEL_ID, FinishReason, GenerationEvent, GenerationInput, GenerationRequest,
        TokenUsage,
    };
    use impossible_inferences_engine::{EngineError, EngineStatus};
    use impossible_inferences_protocol::grpc::{
        ChatMessage as GrpcChatMessage, ChatRequest as GrpcChatRequest,
        CompletionRequest as GrpcCompletionRequest, Sampling,
        generation_event::Payload as GrpcPayload, inference_service_client::InferenceServiceClient,
    };
    use impossible_server_core::{CancellationToken, RequestContext, RequestStop, ServerLimits};
    use impossible_server_testkit::reserve_loopback_listener;
    use tokio::{net::TcpStream, sync::Notify, task::JoinHandle};
    use tokio_tungstenite::{connect_async, tungstenite::Message as ClientWebSocketMessage};
    use tower::ServiceExt;

    use super::{
        EngineEventStream, EngineFuture, InferenceEngine, InferenceServer, LocalInference,
        PendingInference, ShutdownFuture, Workload, WorkloadContext,
    };

    fn test_limits(
        max_request_bytes: usize,
        queue_capacity: usize,
        max_concurrent_requests: usize,
        request_timeout: Duration,
        shutdown_timeout: Duration,
    ) -> Result<ServerLimits, Box<dyn std::error::Error>> {
        Ok(ServerLimits::new(
            max_request_bytes,
            queue_capacity,
            max_concurrent_requests,
            request_timeout,
            shutdown_timeout,
        )?)
    }

    #[tokio::test]
    async fn control_plane_and_pending_status_are_runnable()
    -> Result<(), Box<dyn std::error::Error>> {
        let server = InferenceServer::new(PendingInference);
        for (path, expected) in [
            ("/health/live", 200),
            ("/health/ready", 503),
            ("/metrics", 200),
            ("/version", 200),
            ("/v1/capabilities", 200),
            ("/v1/models", 200),
            ("/status", 200),
        ] {
            let response = server
                .router()
                .oneshot(Request::builder().uri(path).body(Body::empty())?)
                .await?;
            assert_eq!(response.status().as_u16(), expected);
        }
        let metrics = server
            .router()
            .oneshot(Request::builder().uri("/metrics").body(Body::empty())?)
            .await?;
        let body = metrics.into_body().collect().await?.to_bytes();
        assert!(
            String::from_utf8(body.to_vec())?
                .contains("impossible_inferences_control_requests_total")
        );
        Ok(())
    }

    #[tokio::test]
    async fn body_missing_path_and_method_fail_with_stable_json()
    -> Result<(), Box<dyn std::error::Error>> {
        let limits = test_limits(4, 1, 1, Duration::from_secs(1), Duration::from_secs(1))?;
        let router = InferenceServer::with_limits(PendingInference, limits).router();
        for (request, status, code) in [
            (
                Request::builder()
                    .uri("/status")
                    .body(Body::from("12345"))?,
                StatusCode::PAYLOAD_TOO_LARGE,
                "invalid_request",
            ),
            (
                Request::builder().uri("/missing").body(Body::empty())?,
                StatusCode::NOT_FOUND,
                "invalid_request",
            ),
            (
                Request::builder()
                    .method(Method::POST)
                    .uri("/status")
                    .body(Body::empty())?,
                StatusCode::METHOD_NOT_ALLOWED,
                "invalid_request",
            ),
        ] {
            let response = router.clone().oneshot(request).await?;
            assert_eq!(response.status(), status);
            assert_eq!(
                response
                    .headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok()),
                Some("application/json")
            );
            let body = response.into_body().collect().await?.to_bytes();
            assert!(String::from_utf8(body.to_vec())?.contains(code));
        }
        Ok(())
    }

    #[derive(Debug)]
    struct SlowWorkload {
        observed_cancellation: Arc<Notify>,
    }

    impl Workload for SlowWorkload {
        fn component_name(&self) -> &'static str {
            "slow"
        }

        fn routes(&self, context: WorkloadContext) -> Router {
            context.set_ready(true);
            let observed = self.observed_cancellation.clone();
            Router::new().route(
                "/slow",
                get(move |Extension(request): Extension<RequestContext>| {
                    let observed = observed.clone();
                    async move {
                        tokio::spawn(async move {
                            let _ = request.stopped().await;
                            observed.notify_one();
                        });
                        future::pending::<Response>().await
                    }
                }),
            )
        }
    }

    #[tokio::test]
    async fn admission_deadline_and_disconnect_are_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let observed = Arc::new(Notify::new());
        let limits = test_limits(
            1024,
            1,
            1,
            Duration::from_millis(100),
            Duration::from_secs(1),
        )?;
        let router = InferenceServer::with_limits(
            SlowWorkload {
                observed_cancellation: observed.clone(),
            },
            limits,
        )
        .router();
        let first = tokio::spawn(
            router
                .clone()
                .oneshot(Request::builder().uri("/slow").body(Body::empty())?),
        );
        tokio::task::yield_now().await;
        let second = tokio::spawn(
            router
                .clone()
                .oneshot(Request::builder().uri("/slow").body(Body::empty())?),
        );
        tokio::task::yield_now().await;
        let overloaded = router
            .clone()
            .oneshot(Request::builder().uri("/slow").body(Body::empty())?)
            .await?;
        assert_eq!(overloaded.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(first.await??.status(), StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(second.await??.status(), StatusCode::GATEWAY_TIMEOUT);

        let disconnected =
            tokio::spawn(router.oneshot(Request::builder().uri("/slow").body(Body::empty())?));
        tokio::task::yield_now().await;
        disconnected.abort();
        tokio::time::timeout(Duration::from_secs(1), observed.notified()).await?;
        Ok(())
    }

    #[derive(Debug)]
    struct PendingShutdown;

    impl Workload for PendingShutdown {
        fn component_name(&self) -> &'static str {
            "pending-shutdown"
        }

        fn routes(&self, context: WorkloadContext) -> Router {
            context.set_ready(true);
            Router::new().route("/status", get(|| async { StatusCode::NO_CONTENT }))
        }

        fn shutdown(&self, force: CancellationToken) -> ShutdownFuture<'_> {
            Box::pin(async move {
                force.cancelled().await;
                future::pending::<()>().await;
            })
        }
    }

    #[tokio::test]
    async fn pending_workload_shutdown_cannot_exceed_the_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = reserve_loopback_listener().await?;
        let limits = test_limits(
            1024,
            1,
            1,
            Duration::from_secs(1),
            Duration::from_millis(20),
        )?;
        let token = CancellationToken::new();
        let stop = token.clone();
        let server = InferenceServer::with_limits(PendingShutdown, limits);
        let join = tokio::spawn(server.serve(listener, token));
        let _ = stop.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), join).await??;
        let Err(error) = result else {
            return Err("pending cleanup did not report its forced timeout".into());
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        Ok(())
    }

    #[derive(Debug)]
    struct ActiveShutdownWorkload {
        started: Arc<Notify>,
        cleanup_called: Arc<AtomicBool>,
        cleanup_forced: Arc<AtomicBool>,
    }

    impl Workload for ActiveShutdownWorkload {
        fn component_name(&self) -> &'static str {
            "active-shutdown"
        }

        fn routes(&self, context: WorkloadContext) -> Router {
            context.set_ready(true);
            let started = self.started.clone();
            Router::new().route(
                "/hold",
                get(move || {
                    let started = started.clone();
                    async move {
                        started.notify_one();
                        future::pending::<Response>().await
                    }
                }),
            )
        }

        fn shutdown(&self, force: CancellationToken) -> ShutdownFuture<'_> {
            let called = self.cleanup_called.clone();
            let forced = self.cleanup_forced.clone();
            Box::pin(async move {
                called.store(true, Ordering::Release);
                forced.store(force.is_cancelled(), Ordering::Release);
            })
        }
    }

    #[tokio::test]
    async fn forced_active_http_drain_still_polls_cleanup_with_cancelled_token()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = reserve_loopback_listener().await?;
        let address = listener.local_addr()?;
        let started = Arc::new(Notify::new());
        let cleanup_called = Arc::new(AtomicBool::new(false));
        let cleanup_forced = Arc::new(AtomicBool::new(false));
        let limits = test_limits(
            1024,
            1,
            1,
            Duration::from_secs(5),
            Duration::from_millis(20),
        )?;
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let server = InferenceServer::with_limits(
            ActiveShutdownWorkload {
                started: started.clone(),
                cleanup_called: cleanup_called.clone(),
                cleanup_forced: cleanup_forced.clone(),
            },
            limits,
        );
        let join = tokio::spawn(server.serve(listener, shutdown));
        let client = TcpStream::connect(address).await?;
        client.writable().await?;
        let request = b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n";
        if client.try_write(request)? != request.len() {
            return Err("active shutdown request was only partially written".into());
        }
        tokio::time::timeout(Duration::from_secs(1), started.notified()).await?;
        let _ = stop.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), join).await??;
        let Err(error) = result else {
            return Err("forced HTTP drain did not report a timeout".into());
        };
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert!(cleanup_called.load(Ordering::Acquire));
        assert!(cleanup_forced.load(Ordering::Acquire));
        drop(client);
        Ok(())
    }

    #[derive(Debug)]
    struct TerminalTokenWorkload {
        terminal: Arc<Mutex<Option<CancellationToken>>>,
        cleanup_saw_terminal: Arc<AtomicBool>,
        cleanup_saw_force: Arc<AtomicBool>,
    }

    impl Workload for TerminalTokenWorkload {
        fn component_name(&self) -> &'static str {
            "terminal-token"
        }

        fn routes(&self, context: WorkloadContext) -> Router {
            if let Ok(mut token) = self.terminal.lock() {
                *token = Some(context.shutdown_token());
            }
            context.set_ready(true);
            Router::new().route("/status", get(|| async { StatusCode::NO_CONTENT }))
        }

        fn shutdown(&self, force: CancellationToken) -> ShutdownFuture<'_> {
            let terminal = self.terminal.clone();
            let saw_terminal = self.cleanup_saw_terminal.clone();
            let saw_force = self.cleanup_saw_force.clone();
            Box::pin(async move {
                let terminal_cancelled = terminal
                    .lock()
                    .ok()
                    .and_then(|token| token.clone())
                    .is_some_and(|token| token.is_cancelled());
                saw_terminal.store(terminal_cancelled, Ordering::Release);
                saw_force.store(force.is_cancelled(), Ordering::Release);
            })
        }
    }

    #[tokio::test]
    async fn context_shutdown_token_is_terminal_not_force_only()
    -> Result<(), Box<dyn std::error::Error>> {
        let terminal = Arc::new(Mutex::new(None));
        let saw_terminal = Arc::new(AtomicBool::new(false));
        let saw_force = Arc::new(AtomicBool::new(false));
        let server = InferenceServer::new(TerminalTokenWorkload {
            terminal: terminal.clone(),
            cleanup_saw_terminal: saw_terminal.clone(),
            cleanup_saw_force: saw_force.clone(),
        });
        let token_before = terminal
            .lock()
            .map_err(|_| "terminal token lock was poisoned")?
            .clone()
            .ok_or("terminal token was not installed")?;
        assert!(!token_before.is_cancelled());

        let listener = reserve_loopback_listener().await?;
        let shutdown = CancellationToken::new();
        let stop = shutdown.clone();
        let join = tokio::spawn(server.serve(listener, shutdown));
        let _ = stop.cancel();
        tokio::time::timeout(Duration::from_secs(1), join).await???;
        assert!(token_before.is_cancelled());
        assert!(saw_terminal.load(Ordering::Acquire));
        assert!(!saw_force.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn real_listener_starts_and_stops_within_a_bound()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = reserve_loopback_listener().await?;
        let token = CancellationToken::new();
        let stop = token.clone();
        let server = InferenceServer::new(PendingInference);
        let join = tokio::spawn(server.serve(listener, token));
        let _ = stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), join).await???;
        Ok(())
    }

    #[derive(Clone, Default)]
    struct FakeInferenceEngine {
        cancelled: Arc<Notify>,
        observed_cancellation: Arc<AtomicBool>,
    }

    impl InferenceEngine for FakeInferenceEngine {
        fn status(&self) -> EngineFuture<'_, EngineStatus> {
            Box::pin(async {
                EngineStatus {
                    ready: true,
                    runtime: "running",
                    model: "loaded",
                    profile: CURATED_MODEL_ID,
                }
            })
        }

        fn generate(
            &self,
            request: GenerationRequest,
            context: RequestContext,
        ) -> Result<Box<dyn EngineEventStream>, EngineError> {
            if request.model != CURATED_MODEL_ID {
                return Err(EngineError::InvalidRequest("model unavailable"));
            }
            let slow = matches!(
                &request.input,
                GenerationInput::Completion { prompt } if prompt == "slow"
            );
            let events = if slow {
                VecDeque::new()
            } else {
                VecDeque::from([
                    Ok(GenerationEvent::Delta("hel".to_owned())),
                    Ok(GenerationEvent::Delta("lo".to_owned())),
                    Ok(GenerationEvent::Usage(TokenUsage {
                        prompt_tokens: 2,
                        completion_tokens: 2,
                        total_tokens: 4,
                    })),
                    Ok(GenerationEvent::Finished(FinishReason::Stop)),
                ])
            };
            Ok(Box::new(FakeEventStream {
                events,
                context,
                cancelled: self.cancelled.clone(),
                observed_cancellation: self.observed_cancellation.clone(),
                slow,
                terminal: false,
            }))
        }

        fn shutdown(&self) -> EngineFuture<'_, Result<(), EngineError>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct FakeEventStream {
        events: VecDeque<Result<GenerationEvent, EngineError>>,
        context: RequestContext,
        cancelled: Arc<Notify>,
        observed_cancellation: Arc<AtomicBool>,
        slow: bool,
        terminal: bool,
    }

    impl Drop for FakeEventStream {
        fn drop(&mut self) {
            if self.context.cancellation().is_cancelled() {
                self.observed_cancellation.store(true, Ordering::Release);
                self.cancelled.notify_one();
            }
        }
    }

    impl EngineEventStream for FakeEventStream {
        fn recv(
            &mut self,
        ) -> Pin<Box<dyn Future<Output = Option<Result<GenerationEvent, EngineError>>> + Send + '_>>
        {
            Box::pin(async move {
                if self.terminal {
                    return None;
                }
                if self.slow {
                    let stop = self.context.stopped().await;
                    self.observed_cancellation.store(true, Ordering::Release);
                    self.cancelled.notify_one();
                    self.terminal = true;
                    return Some(Err(match stop {
                        RequestStop::Cancelled => EngineError::Cancelled,
                        RequestStop::DeadlineExceeded => EngineError::DeadlineExceeded,
                    }));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                self.events.pop_front()
            })
        }
    }

    async fn spawn_transport_server(
        limits: ServerLimits,
    ) -> Result<
        (
            SocketAddr,
            CancellationToken,
            JoinHandle<std::io::Result<()>>,
            FakeInferenceEngine,
        ),
        Box<dyn std::error::Error>,
    > {
        let listener = reserve_loopback_listener().await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let engine = FakeInferenceEngine::default();
        let server = InferenceServer::with_limits(LocalInference::new(engine.clone()), limits);
        let join = tokio::spawn(server.serve(listener, shutdown.clone()));
        Ok((address, shutdown, join, engine))
    }

    fn transport_limits() -> Result<ServerLimits, Box<dyn std::error::Error>> {
        test_limits(
            1_024,
            4,
            2,
            Duration::from_millis(200),
            Duration::from_secs(1),
        )
    }

    async fn wait_for_fake_cancellation(
        engine: &FakeInferenceEngine,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !engine.observed_cancellation.load(Ordering::Acquire) {
            tokio::time::timeout(Duration::from_secs(1), engine.cancelled.notified())
                .await
                .map_err(|_| "generation cancellation was not observed")?;
        }
        Ok(())
    }

    async fn assert_http_failures(
        client: &reqwest::Client,
        endpoint: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let malformed = client
            .post(endpoint)
            .header("content-type", "application/json")
            .body("{")
            .send()
            .await?;
        assert_eq!(malformed.status(), reqwest::StatusCode::BAD_REQUEST);
        assert!(malformed.text().await?.contains("invalid_request"));

        let unsupported = client
            .post(endpoint)
            .json(&serde_json::json!({
                "model": CURATED_MODEL_ID,
                "prompt": "hello",
                "tools": []
            }))
            .send()
            .await?;
        assert_eq!(unsupported.status(), reqwest::StatusCode::BAD_REQUEST);

        let oversized = client
            .post(endpoint)
            .header("content-type", "application/json")
            .body("x".repeat(1_025))
            .send()
            .await?;
        assert_eq!(oversized.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

        let slow = client
            .post(endpoint)
            .json(&serde_json::json!({
                "model": CURATED_MODEL_ID,
                "prompt": "slow"
            }))
            .send()
            .await?;
        assert_eq!(slow.status(), reqwest::StatusCode::GATEWAY_TIMEOUT);
        Ok(())
    }

    #[tokio::test]
    async fn real_network_http_json_sse_validation_and_disconnect_are_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let (address, shutdown, join, _engine) =
            spawn_transport_server(transport_limits()?).await?;
        let client = reqwest::Client::builder().no_proxy().build()?;
        let endpoint = format!("http://{address}/v1/completions");
        let response = client
            .post(&endpoint)
            .json(&serde_json::json!({
                "model": CURATED_MODEL_ID,
                "prompt": "hello",
                "max_tokens": 8
            }))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert!(response.headers().contains_key("x-request-id"));
        let json: serde_json::Value = response.json().await?;
        assert_eq!(json["choices"][0]["text"], "hello");
        assert_eq!(json["usage"]["total_tokens"], 4);

        let chat: serde_json::Value = client
            .post(format!("http://{address}/v1/chat/completions"))
            .json(&serde_json::json!({
                "model": CURATED_MODEL_ID,
                "messages": [{"role": "user", "content": "hello"}],
                "max_tokens": 8
            }))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        assert_eq!(chat["choices"][0]["message"]["content"], "hello");
        assert_eq!(chat["choices"][0]["message"]["role"], "assistant");

        let response = client
            .post(&endpoint)
            .json(&serde_json::json!({
                "model": CURATED_MODEL_ID,
                "prompt": "hello",
                "stream": true
            }))
            .send()
            .await?;
        assert_eq!(response.status(), reqwest::StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream")
        );
        let mut bytes = response.bytes_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), bytes.next())
            .await
            .map_err(|_| "SSE first chunk timed out")?
            .ok_or("SSE response ended before its first chunk")??;
        let first = String::from_utf8(first.to_vec())?;
        assert!(first.contains("hel"));
        assert!(!first.contains("[DONE]"));
        let mut remainder = String::new();
        while let Some(chunk) = bytes.next().await {
            remainder.push_str(&String::from_utf8(chunk?.to_vec())?);
        }
        assert!(remainder.contains("lo"));
        assert!(remainder.contains("[DONE]"));

        assert_http_failures(&client, &endpoint).await?;

        let _ = shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), join)
            .await
            .map_err(|_| "transport server shutdown timed out")???;
        Ok(())
    }

    #[tokio::test]
    async fn real_network_websocket_streams_rejects_binary_and_cancels()
    -> Result<(), Box<dyn std::error::Error>> {
        let (address, shutdown, join, engine) = spawn_transport_server(transport_limits()?).await?;
        let (mut socket, response) = connect_async(format!("ws://{address}/v1/ws")).await?;
        assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
        let ready = socket.next().await.ok_or("missing ready message")??;
        assert!(ready.to_text()?.contains("\"type\":\"ready\""));

        socket
            .send(ClientWebSocketMessage::Text(
                serde_json::json!({
                    "type": "start",
                    "id": "one",
                    "request": {
                        "mode": "completion",
                        "model": CURATED_MODEL_ID,
                        "prompt": "hello"
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
        let mut messages = Vec::new();
        while messages.len() < 3 {
            messages.push(
                socket
                    .next()
                    .await
                    .ok_or("WebSocket generation ended early")??
                    .into_text()?
                    .to_string(),
            );
        }
        assert!(messages[0].contains("\"type\":\"delta\""));
        assert!(messages[1].contains("\"type\":\"delta\""));
        assert!(messages[2].contains("\"type\":\"final\""));
        assert!(messages[2].contains("\"total_tokens\":4"));

        socket
            .send(ClientWebSocketMessage::Binary(vec![1, 2, 3].into()))
            .await?;
        let binary_error = socket.next().await.ok_or("missing binary error")??;
        assert!(binary_error.to_text()?.contains("invalid_request"));

        socket
            .send(ClientWebSocketMessage::Text(
                serde_json::json!({
                    "type": "start",
                    "id": "slow-one",
                    "request": {
                        "mode": "completion",
                        "model": CURATED_MODEL_ID,
                        "prompt": "slow"
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
        socket
            .send(ClientWebSocketMessage::Text(
                serde_json::json!({"type": "cancel", "id": "slow-one"})
                    .to_string()
                    .into(),
            ))
            .await?;
        let cancelled = socket.next().await.ok_or("missing cancellation")??;
        assert!(cancelled.to_text()?.contains("cancelled"));
        wait_for_fake_cancellation(&engine).await?;
        engine.observed_cancellation.store(false, Ordering::Release);
        socket
            .send(ClientWebSocketMessage::Text(
                serde_json::json!({
                    "type": "start",
                    "id": "disconnect-one",
                    "request": {
                        "mode": "completion",
                        "model": CURATED_MODEL_ID,
                        "prompt": "slow"
                    }
                })
                .to_string()
                .into(),
            ))
            .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(socket);
        wait_for_fake_cancellation(&engine).await?;

        let _ = shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), join).await???;
        Ok(())
    }

    fn mcp_meta() -> serde_json::Value {
        serde_json::json!({
            "io.modelcontextprotocol/clientInfo": {"name": "contract-test", "version": "1"}
        })
    }

    async fn mcp_request(
        client: &reqwest::Client,
        address: SocketAddr,
        method: &str,
        name: Option<&str>,
        params: serde_json::Value,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut request = client
            .post(format!("http://{address}/mcp"))
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", method)
            .json(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params
            }));
        if let Some(name) = name {
            request = request.header("mcp-name", name);
        }
        request.send().await
    }

    #[tokio::test]
    async fn real_network_mcp_lists_calls_and_validates_modern_routing()
    -> Result<(), Box<dyn std::error::Error>> {
        let (address, shutdown, join, _engine) =
            spawn_transport_server(transport_limits()?).await?;
        let client = reqwest::Client::builder().no_proxy().build()?;

        let missing_headers = client
            .post(format!("http://{address}/mcp"))
            .json(&serde_json::json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/list",
                "params": {"_meta": mcp_meta()}
            }))
            .send()
            .await?;
        assert_eq!(missing_headers.status(), reqwest::StatusCode::BAD_REQUEST);

        let oversized = client
            .post(format!("http://{address}/mcp"))
            .header("content-type", "application/json")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "tools/list")
            .body("x".repeat(1_025))
            .send()
            .await?;
        assert_eq!(oversized.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);

        let listed: serde_json::Value = mcp_request(
            &client,
            address,
            "tools/list",
            None,
            serde_json::json!({"_meta": mcp_meta()}),
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
        assert_eq!(listed["result"]["tools"].as_array().map(Vec::len), Some(4));
        assert_eq!(listed["result"]["ttlMs"], 60_000);

        let generated: serde_json::Value = mcp_request(
            &client,
            address,
            "tools/call",
            Some("generate_text"),
            serde_json::json!({
                "name": "generate_text",
                "arguments": {"model": CURATED_MODEL_ID, "prompt": "hello", "max_tokens": 8},
                "_meta": mcp_meta()
            }),
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
        assert_eq!(generated["result"]["structuredContent"]["text"], "hello");
        assert_eq!(
            generated["result"]["structuredContent"]["usage"]["total_tokens"],
            4
        );

        let resource: serde_json::Value = mcp_request(
            &client,
            address,
            "resources/read",
            Some("impossible://capabilities"),
            serde_json::json!({
                "uri": "impossible://capabilities",
                "_meta": mcp_meta()
            }),
        )
        .await?
        .error_for_status()?
        .json()
        .await?;
        assert!(
            resource["result"]["contents"][0]["text"]
                .as_str()
                .is_some_and(|text| text.contains("grpc"))
        );

        let mismatch = mcp_request(
            &client,
            address,
            "tools/call",
            Some("chat"),
            serde_json::json!({
                "name": "generate_text",
                "arguments": {"model": CURATED_MODEL_ID, "prompt": "hello"},
                "_meta": mcp_meta()
            }),
        )
        .await?;
        assert_eq!(mismatch.status(), reqwest::StatusCode::BAD_REQUEST);

        let _ = shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), join).await???;
        Ok(())
    }

    #[tokio::test]
    async fn real_network_grpc_unary_streaming_validation_and_disconnect_are_bounded()
    -> Result<(), Box<dyn std::error::Error>> {
        let listener = reserve_loopback_listener().await?;
        let address = listener.local_addr()?;
        let shutdown = CancellationToken::new();
        let engine = FakeInferenceEngine::default();
        let local = LocalInference::new(engine.clone());
        let join = tokio::spawn(local.serve_grpc(listener, shutdown.clone()));
        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut client = InferenceServiceClient::connect(format!("http://{address}")).await?;

        let response = client
            .complete(GrpcCompletionRequest {
                model: CURATED_MODEL_ID.to_owned(),
                prompt: "hello".to_owned(),
                sampling: Some(Sampling {
                    max_tokens: 8,
                    temperature: 0.0,
                    top_p: 1.0,
                    seed: Some(7),
                    stop: Vec::new(),
                }),
            })
            .await?
            .into_inner();
        assert_eq!(response.text, "hello");
        assert_eq!(response.usage.map(|usage| usage.total_tokens), Some(4));

        let mut stream = client
            .chat_stream(GrpcChatRequest {
                model: CURATED_MODEL_ID.to_owned(),
                messages: vec![GrpcChatMessage {
                    role: impossible_inferences_protocol::grpc::ChatRole::User.into(),
                    content: "hello".to_owned(),
                }],
                sampling: None,
            })
            .await?
            .into_inner();
        let mut payloads = Vec::new();
        while let Some(event) = stream.message().await? {
            payloads.push(event.payload.ok_or("missing gRPC event payload")?);
        }
        assert!(matches!(payloads.first(), Some(GrpcPayload::Delta(value)) if value == "hel"));
        assert!(matches!(payloads.get(1), Some(GrpcPayload::Delta(value)) if value == "lo"));
        assert!(
            matches!(payloads.get(2), Some(GrpcPayload::Usage(value)) if value.total_tokens == 4)
        );
        assert!(
            matches!(payloads.get(3), Some(GrpcPayload::FinishReason(value)) if value == "stop")
        );

        let invalid = client
            .chat(GrpcChatRequest {
                model: CURATED_MODEL_ID.to_owned(),
                messages: vec![GrpcChatMessage {
                    role: 0,
                    content: "hello".to_owned(),
                }],
                sampling: None,
            })
            .await
            .err()
            .ok_or("invalid gRPC chat role was accepted")?;
        assert_eq!(invalid.code(), tonic::Code::InvalidArgument);

        let mut deadline_request = tonic::Request::new(GrpcCompletionRequest {
            model: CURATED_MODEL_ID.to_owned(),
            prompt: "slow".to_owned(),
            sampling: None,
        });
        deadline_request.set_timeout(Duration::from_millis(100));
        let deadline = client
            .complete(deadline_request)
            .await
            .err()
            .ok_or("expired gRPC request was accepted")?;
        assert_eq!(deadline.code(), tonic::Code::DeadlineExceeded);
        wait_for_fake_cancellation(&engine).await?;
        engine.observed_cancellation.store(false, Ordering::Release);

        let slow = client
            .complete_stream(GrpcCompletionRequest {
                model: CURATED_MODEL_ID.to_owned(),
                prompt: "slow".to_owned(),
                sampling: None,
            })
            .await?
            .into_inner();
        drop(slow);
        wait_for_fake_cancellation(&engine).await?;

        let _ = shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), join).await???;
        Ok(())
    }
}
