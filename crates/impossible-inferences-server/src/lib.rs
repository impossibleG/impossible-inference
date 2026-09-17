//! Bounded HTTP control plane for Impossible Inferences.

use std::{
    future::{Future, IntoFuture, poll_fn},
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::Poll,
};

use axum::{
    Json, Router, body,
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use impossible_server_core::{
    CancellationToken, DrainOutcome, HealthRegistry, ProcessState, ReadinessReason, RequestContext,
    RequestIdSource, ServerLimits, ShutdownGate,
};
use serde::Serialize;
use tokio::{
    net::TcpListener,
    sync::Semaphore,
    time::{Instant, timeout_at},
};

/// Boxed shutdown future returned by a workload implementation.
pub type ShutdownFuture<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

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
    let _cancel_on_drop = RequestCancellationGuard(cancellation.clone());
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
    response
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
    Json(serde_json::json!({
        "schema_version": 1,
        "product": "impossible-inferences",
        "version": env!("CARGO_PKG_VERSION"),
        "available": [],
        "pending": ["completions", "chat_completions", "sse", "websocket", "grpc", "mcp"]
    }))
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
        future,
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
    use http_body_util::BodyExt;
    use impossible_server_core::{CancellationToken, RequestContext, ServerLimits};
    use impossible_server_testkit::reserve_loopback_listener;
    use tokio::{net::TcpStream, sync::Notify};
    use tower::ServiceExt;

    use super::{InferenceServer, PendingInference, ShutdownFuture, Workload, WorkloadContext};

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
}
