//! Private, transport-neutral token generation through a pinned `llama-server` sidecar.

use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    future::Future,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use futures_util::StreamExt;
use impossible_inferences_artifacts::{InstalledArtifacts, Verification, resolve_installed};
use impossible_inferences_domain::{
    CURATED_MODEL_ID, ChatRole, FinishReason, GenerationEvent, GenerationInput, GenerationRequest,
    TokenUsage,
};
use impossible_server_core::{CancellationToken, RequestContext, RequestStop};
use reqwest::{Client, StatusCode, redirect::Policy};
use serde::Serialize;
use serde_json::Value;
use tokio::{
    io::AsyncReadExt,
    process::{Child, Command},
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};

const MODEL_ALIAS: &str = CURATED_MODEL_ID;
const MAX_SSE_LINE_BYTES: usize = 1_048_576;
const MAX_CAPTURED_LOG_CHUNKS: usize = 64;
const CAPTURED_LOG_CHUNK_BYTES: usize = 1_024;
const LAUNCH_ATTEMPTS: usize = 3;

/// Resource and lifecycle policy enforced inside the engine boundary.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Maximum encoded prompt/message content bytes.
    pub max_prompt_bytes: usize,
    /// Maximum messages accepted by one chat request.
    pub max_messages: usize,
    /// Maximum generated tokens accepted from a caller.
    pub max_output_tokens: u32,
    /// Maximum requests waiting for the single curated CPU generation lane.
    pub queue_capacity: usize,
    /// Maximum simultaneous generation requests. v0.1 defaults to one.
    pub max_concurrent: usize,
    /// Bounded number of events waiting for a slow transport consumer.
    pub event_capacity: usize,
    /// Time allowed for the child runtime to load and become healthy.
    pub startup_timeout: Duration,
    /// Time allowed to terminate the child runtime.
    pub shutdown_timeout: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_prompt_bytes: 65_536,
            max_messages: 128,
            max_output_tokens: 1_024,
            queue_capacity: 8,
            max_concurrent: 1,
            event_capacity: 32,
            startup_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

impl EngineConfig {
    fn validate(&self) -> Result<(), EngineError> {
        if self.max_prompt_bytes == 0
            || self.max_messages == 0
            || self.max_output_tokens == 0
            || self.max_concurrent != 1
            || self.event_capacity == 0
            || self.startup_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
        {
            return Err(EngineError::Configuration);
        }
        Ok(())
    }
}

/// Sanitized engine failure suitable for mapping into every public transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    /// Engine policy is invalid.
    Configuration,
    /// Request validation failed.
    InvalidRequest(&'static str),
    /// The bounded admission queue is full.
    Overloaded,
    /// Curated artifacts are not installed and fully verified.
    ArtifactsUnavailable,
    /// The private child could not start or exited unexpectedly.
    RuntimeUnavailable,
    /// The private runtime returned malformed or incomplete data.
    RuntimeProtocol,
    /// Caller cancellation won.
    Cancelled,
    /// Request deadline elapsed.
    DeadlineExceeded,
    /// Engine shutdown has begun.
    ShuttingDown,
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Configuration => "generation engine configuration is invalid",
            Self::InvalidRequest(message) => message,
            Self::Overloaded => "generation capacity is temporarily exhausted",
            Self::ArtifactsUnavailable => "verified inference artifacts are unavailable",
            Self::RuntimeUnavailable => "local inference runtime is unavailable",
            Self::RuntimeProtocol => "local inference runtime returned an invalid response",
            Self::Cancelled => "generation was cancelled",
            Self::DeadlineExceeded => "generation deadline was exceeded",
            Self::ShuttingDown => "generation engine is shutting down",
        })
    }
}

impl Error for EngineError {}

/// Privacy-safe engine state used by status and doctor surfaces.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EngineStatus {
    /// Whether the private sidecar is currently healthy.
    pub ready: bool,
    /// Curated runtime state.
    pub runtime: &'static str,
    /// Curated model state.
    pub model: &'static str,
    /// Stable model identity.
    pub profile: &'static str,
}

/// A bounded stream of ordered generation events.
pub struct GenerationStream {
    receiver: mpsc::Receiver<Result<GenerationEvent, EngineError>>,
    disconnected: CancellationToken,
}

impl GenerationStream {
    /// Receives the next event. `None` is returned only after the producer is fenced.
    pub async fn recv(&mut self) -> Option<Result<GenerationEvent, EngineError>> {
        self.receiver.recv().await
    }
}

impl Drop for GenerationStream {
    fn drop(&mut self) {
        let _ = self.disconnected.cancel();
    }
}

#[derive(Clone)]
struct LaunchAssets {
    runtime_directory: PathBuf,
    runtime_executable: PathBuf,
    model_file: PathBuf,
}

impl From<InstalledArtifacts> for LaunchAssets {
    fn from(value: InstalledArtifacts) -> Self {
        Self {
            runtime_directory: value.runtime_directory().to_path_buf(),
            runtime_executable: value.runtime_executable().to_path_buf(),
            model_file: value.model_file().to_path_buf(),
        }
    }
}

struct LaunchSpec {
    assets: LaunchAssets,
    port: u16,
    bearer: String,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

trait ManagedChild: Send {
    fn try_exited(&mut self) -> Result<bool, EngineError>;
    fn terminate(&mut self, deadline: Duration) -> BoxFuture<'_, Result<(), EngineError>>;
}

trait SidecarLauncher: Send + Sync {
    fn launch<'a>(
        &'a self,
        spec: &'a LaunchSpec,
    ) -> BoxFuture<'a, Result<Box<dyn ManagedChild>, EngineError>>;
}

#[derive(Debug, Default)]
struct RealLauncher;

impl SidecarLauncher for RealLauncher {
    fn launch<'a>(
        &'a self,
        spec: &'a LaunchSpec,
    ) -> BoxFuture<'a, Result<Box<dyn ManagedChild>, EngineError>> {
        Box::pin(async move {
            let mut command = build_command(spec);
            let mut child = command
                .spawn()
                .map_err(|_| EngineError::RuntimeUnavailable)?;
            let sensitive = Arc::new(vec![
                spec.bearer.clone(),
                spec.assets.runtime_directory.to_string_lossy().into_owned(),
                spec.assets
                    .runtime_executable
                    .to_string_lossy()
                    .into_owned(),
                spec.assets.model_file.to_string_lossy().into_owned(),
            ]);
            let captured = Arc::new(Mutex::new(VecDeque::new()));
            let stdout_task = child.stdout.take().map(|stream| {
                tokio::spawn(drain_private_output(
                    stream,
                    captured.clone(),
                    sensitive.clone(),
                ))
            });
            let stderr_task = child.stderr.take().map(|stream| {
                tokio::spawn(drain_private_output(stream, captured.clone(), sensitive))
            });
            Ok(Box::new(RealChild {
                child,
                _captured: captured,
                output_tasks: [stdout_task, stderr_task],
            }) as Box<dyn ManagedChild>)
        })
    }
}

struct RealChild {
    child: Child,
    _captured: Arc<Mutex<VecDeque<String>>>,
    output_tasks: [Option<JoinHandle<()>>; 2],
}

impl ManagedChild for RealChild {
    fn try_exited(&mut self) -> Result<bool, EngineError> {
        self.child
            .try_wait()
            .map(|status| status.is_some())
            .map_err(|_| EngineError::RuntimeUnavailable)
    }

    fn terminate(&mut self, deadline: Duration) -> BoxFuture<'_, Result<(), EngineError>> {
        Box::pin(async move {
            if !self.try_exited()? {
                self.child
                    .start_kill()
                    .map_err(|_| EngineError::RuntimeUnavailable)?;
                timeout(deadline, self.child.wait())
                    .await
                    .map_err(|_| EngineError::RuntimeUnavailable)?
                    .map_err(|_| EngineError::RuntimeUnavailable)?;
            }
            for task in self.output_tasks.iter().flatten() {
                task.abort();
            }
            Ok(())
        })
    }
}

fn build_command(spec: &LaunchSpec) -> Command {
    let mut command = Command::new(&spec.assets.runtime_executable);
    command
        .current_dir(&spec.assets.runtime_directory)
        .arg("--model")
        .arg(&spec.assets.model_file)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            &spec.port.to_string(),
            "--alias",
            MODEL_ALIAS,
            "--ctx-size",
            "4096",
            "--parallel",
            "1",
            "--no-cont-batching",
            "--no-webui",
            "--no-slots",
            "--no-mmproj",
            "--offline",
            "--timeout",
            "60",
            "--sse-ping-interval",
            "-1",
            "--no-cache-prompt",
            "--reasoning",
            "off",
            "--jinja",
            "--chat-template",
            "chatml",
            "--log-verbosity",
            "1",
            "--log-colors",
            "off",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("LLAMA_API_KEY", &spec.bearer)
        .kill_on_drop(true);
    #[cfg(target_os = "windows")]
    command.creation_flags(0x0800_0000);
    command
}

async fn drain_private_output<R>(
    mut stream: R,
    captured: Arc<Mutex<VecDeque<String>>>,
    sensitive: Arc<Vec<String>>,
) where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = [0_u8; CAPTURED_LOG_CHUNK_BYTES];
    loop {
        let Ok(count) = stream.read(&mut bytes).await else {
            return;
        };
        if count == 0 {
            return;
        }
        let mut value = String::from_utf8_lossy(&bytes[..count]).into_owned();
        for protected in sensitive.iter().filter(|value| !value.is_empty()) {
            value = value.replace(protected, "[redacted]");
        }
        value
            .retain(|character| !character.is_control() || matches!(character, '\n' | '\r' | '\t'));
        let mut logs = captured.lock().await;
        if logs.len() == MAX_CAPTURED_LOG_CHUNKS {
            let _ = logs.pop_front();
        }
        logs.push_back(value);
    }
}

struct RunningSidecar {
    child: Box<dyn ManagedChild>,
    endpoint: String,
    bearer: String,
}

#[derive(Default)]
struct Lifecycle {
    running: Option<RunningSidecar>,
}

struct EngineInner {
    assets: LaunchAssets,
    config: EngineConfig,
    client: Client,
    launcher: Arc<dyn SidecarLauncher>,
    lifecycle: Mutex<Lifecycle>,
    admitted: Arc<Semaphore>,
    execution: Arc<Semaphore>,
    shutdown: CancellationToken,
}

/// Cloneable local generation engine. All public transports consume this single boundary.
#[derive(Clone)]
pub struct GenerationEngine(Arc<EngineInner>);

impl fmt::Debug for GenerationEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GenerationEngine")
            .field("profile", &CURATED_MODEL_ID)
            .field("paths", &"redacted")
            .finish_non_exhaustive()
    }
}

impl GenerationEngine {
    /// Fully verifies the offline artifact store and starts the private sidecar.
    ///
    /// Ordinary serving never downloads, repairs, or selects an unpinned artifact.
    ///
    /// # Errors
    /// Returns a sanitized error when configuration, artifacts, or child startup fail.
    pub async fn start_from_store(root: &Path, config: EngineConfig) -> Result<Self, EngineError> {
        config.validate()?;
        let installed = resolve_installed(root, Verification::Full)
            .map_err(|_| EngineError::ArtifactsUnavailable)?;
        Self::start(
            LaunchAssets::from(installed),
            config,
            Arc::new(RealLauncher),
        )
        .await
    }

    async fn start(
        assets: LaunchAssets,
        config: EngineConfig,
        launcher: Arc<dyn SidecarLauncher>,
    ) -> Result<Self, EngineError> {
        config.validate()?;
        let client = Client::builder()
            .no_proxy()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .build()
            .map_err(|_| EngineError::Configuration)?;
        let admitted_capacity = config
            .max_concurrent
            .checked_add(config.queue_capacity)
            .ok_or(EngineError::Configuration)?;
        let engine = Self(Arc::new(EngineInner {
            assets,
            admitted: Arc::new(Semaphore::new(admitted_capacity)),
            execution: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            client,
            launcher,
            lifecycle: Mutex::new(Lifecycle::default()),
            shutdown: CancellationToken::new(),
        }));
        engine.ensure_running().await?;
        Ok(engine)
    }

    /// Returns privacy-safe live engine state and observes an unexpected child exit.
    pub async fn status(&self) -> EngineStatus {
        let mut lifecycle = self.0.lifecycle.lock().await;
        let ready = if let Some(running) = lifecycle.running.as_mut() {
            matches!(running.child.try_exited(), Ok(false))
        } else {
            false
        };
        if !ready {
            lifecycle.running = None;
        }
        EngineStatus {
            ready,
            runtime: if ready { "running" } else { "not_ready" },
            model: if ready { "loaded" } else { "not_ready" },
            profile: CURATED_MODEL_ID,
        }
    }

    /// Validates and admits one request, returning a bounded ordered event stream.
    ///
    /// # Errors
    /// Returns immediately for invalid input, a full queue, or shutdown.
    pub fn generate(
        &self,
        request: GenerationRequest,
        context: RequestContext,
    ) -> Result<GenerationStream, EngineError> {
        if self.0.shutdown.is_cancelled() {
            return Err(EngineError::ShuttingDown);
        }
        let request = normalize_request(request, &self.0.config)?;
        let admitted = self
            .0
            .admitted
            .clone()
            .try_acquire_owned()
            .map_err(|_| EngineError::Overloaded)?;
        let disconnected = CancellationToken::new();
        let task_disconnected = disconnected.clone();
        let (sender, receiver) = mpsc::channel(self.0.config.event_capacity);
        let engine = self.clone();
        tokio::spawn(async move {
            let result = engine
                .run_generation(
                    request,
                    context,
                    task_disconnected,
                    sender.clone(),
                    admitted,
                )
                .await;
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        });
        Ok(GenerationStream {
            receiver,
            disconnected,
        })
    }

    /// Cancels work and terminates exactly the owned child process within the configured bound.
    ///
    /// # Errors
    /// Returns a sanitized runtime failure if the owned child cannot be reaped within the bound.
    pub async fn shutdown(&self) -> Result<(), EngineError> {
        let _ = self.0.shutdown.cancel();
        let mut lifecycle = self.0.lifecycle.lock().await;
        if let Some(mut running) = lifecycle.running.take() {
            running
                .child
                .terminate(self.0.config.shutdown_timeout)
                .await?;
        }
        Ok(())
    }

    async fn run_generation(
        &self,
        request: NormalizedRequest,
        context: RequestContext,
        disconnected: CancellationToken,
        sender: mpsc::Sender<Result<GenerationEvent, EngineError>>,
        _admitted: OwnedSemaphorePermit,
    ) -> Result<(), EngineError> {
        let execution = tokio::select! {
            biased;
            stop = context.stopped() => return Err(map_stop(stop)),
            () = disconnected.cancelled() => return Err(EngineError::Cancelled),
            () = self.0.shutdown.cancelled() => return Err(EngineError::ShuttingDown),
            permit = self.0.execution.clone().acquire_owned() => {
                permit.map_err(|_| EngineError::ShuttingDown)?
            }
        };
        self.ensure_running().await?;
        let (endpoint, bearer) = self.connection().await?;
        let body = request.body();
        let remaining = context.remaining().ok_or(EngineError::Configuration)?;
        if remaining.is_zero() {
            return Err(EngineError::DeadlineExceeded);
        }
        let request_future = self
            .0
            .client
            .post(format!("{endpoint}{}", request.path()))
            .bearer_auth(bearer)
            .timeout(remaining)
            .json(&body)
            .send();
        let response = tokio::select! {
            biased;
            stop = context.stopped() => return Err(map_stop(stop)),
            () = disconnected.cancelled() => return Err(EngineError::Cancelled),
            () = self.0.shutdown.cancelled() => return Err(EngineError::ShuttingDown),
            result = request_future => result.map_err(|_| EngineError::RuntimeUnavailable)?,
        };
        if response.status() != StatusCode::OK {
            return Err(EngineError::RuntimeUnavailable);
        }
        let result = self
            .consume_sse(response, &request, &context, &disconnected, &sender)
            .await;
        drop(execution);
        result
    }

    async fn consume_sse(
        &self,
        response: reqwest::Response,
        request: &NormalizedRequest,
        context: &RequestContext,
        disconnected: &CancellationToken,
        sender: &mpsc::Sender<Result<GenerationEvent, EngineError>>,
    ) -> Result<(), EngineError> {
        let mut stream = response.bytes_stream();
        let mut buffer = Vec::new();
        let mut finish = None;
        let mut usage = None;
        let mut done = false;
        loop {
            let next = tokio::select! {
                biased;
                stop = context.stopped() => return Err(map_stop(stop)),
                () = disconnected.cancelled() => return Err(EngineError::Cancelled),
                () = self.0.shutdown.cancelled() => return Err(EngineError::ShuttingDown),
                value = stream.next() => value,
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk.map_err(|_| EngineError::RuntimeUnavailable)?;
            if buffer.len().saturating_add(chunk.len()) > MAX_SSE_LINE_BYTES {
                return Err(EngineError::RuntimeProtocol);
            }
            buffer.extend_from_slice(&chunk);
            while let Some(position) = buffer.iter().position(|byte| *byte == b'\n') {
                let mut line: Vec<u8> = buffer.drain(..=position).collect();
                if line.last() == Some(&b'\n') {
                    let _ = line.pop();
                }
                if line.last() == Some(&b'\r') {
                    let _ = line.pop();
                }
                let Some(data) = line.strip_prefix(b"data:") else {
                    continue;
                };
                let data = if data.first() == Some(&b' ') {
                    &data[1..]
                } else {
                    data
                };
                if data == b"[DONE]" {
                    done = true;
                    continue;
                }
                let value: Value =
                    serde_json::from_slice(data).map_err(|_| EngineError::RuntimeProtocol)?;
                if let Some(parsed) = parse_usage(&value)? {
                    usage = Some(parsed);
                }
                if let Some(reason) = parse_finish_reason(&value)? {
                    finish = Some(reason);
                }
                if let Some(delta) = request.delta(&value)?
                    && !delta.is_empty()
                {
                    send_event(
                        sender,
                        GenerationEvent::Delta(delta.to_owned()),
                        context,
                        disconnected,
                        &self.0.shutdown,
                    )
                    .await?;
                }
            }
            if done {
                break;
            }
        }
        if !done || !buffer.is_empty() || finish.is_none() || usage.is_none() {
            return Err(EngineError::RuntimeProtocol);
        }
        send_event(
            sender,
            GenerationEvent::Usage(usage.ok_or(EngineError::RuntimeProtocol)?),
            context,
            disconnected,
            &self.0.shutdown,
        )
        .await?;
        send_event(
            sender,
            GenerationEvent::Finished(finish.ok_or(EngineError::RuntimeProtocol)?),
            context,
            disconnected,
            &self.0.shutdown,
        )
        .await
    }

    async fn connection(&self) -> Result<(String, String), EngineError> {
        let mut lifecycle = self.0.lifecycle.lock().await;
        let running = lifecycle
            .running
            .as_mut()
            .ok_or(EngineError::RuntimeUnavailable)?;
        if running.child.try_exited()? {
            lifecycle.running = None;
            return Err(EngineError::RuntimeUnavailable);
        }
        Ok((running.endpoint.clone(), running.bearer.clone()))
    }

    async fn ensure_running(&self) -> Result<(), EngineError> {
        if self.0.shutdown.is_cancelled() {
            return Err(EngineError::ShuttingDown);
        }
        let mut lifecycle = self.0.lifecycle.lock().await;
        if let Some(running) = lifecycle.running.as_mut() {
            if !running.child.try_exited()? {
                return Ok(());
            }
            lifecycle.running = None;
        }
        for attempt in 0..LAUNCH_ATTEMPTS {
            if attempt > 0 {
                let shift = u32::try_from(attempt - 1).map_err(|_| EngineError::Configuration)?;
                let millis = 250_u64.checked_shl(shift).unwrap_or(1_000).min(1_000);
                sleep(Duration::from_millis(millis)).await;
            }
            let port = reserve_ephemeral_port()?;
            let bearer = ephemeral_bearer()?;
            let spec = LaunchSpec {
                assets: self.0.assets.clone(),
                port,
                bearer: bearer.clone(),
            };
            let Ok(mut child) = self.0.launcher.launch(&spec).await else {
                continue;
            };
            let endpoint = format!("http://127.0.0.1:{port}");
            if self
                .wait_until_ready(&mut *child, &endpoint, &bearer)
                .await
                .is_ok()
            {
                lifecycle.running = Some(RunningSidecar {
                    child,
                    endpoint,
                    bearer,
                });
                return Ok(());
            }
            let _ = child.terminate(self.0.config.shutdown_timeout).await;
        }
        Err(EngineError::RuntimeUnavailable)
    }

    async fn wait_until_ready(
        &self,
        child: &mut dyn ManagedChild,
        endpoint: &str,
        bearer: &str,
    ) -> Result<(), EngineError> {
        let deadline = Instant::now() + self.0.config.startup_timeout;
        loop {
            if child.try_exited()? {
                return Err(EngineError::RuntimeUnavailable);
            }
            if let Ok(response) = self
                .0
                .client
                .get(format!("{endpoint}/health"))
                .bearer_auth(bearer)
                .timeout(Duration::from_millis(500))
                .send()
                .await
                && response.status() == StatusCode::OK
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(EngineError::RuntimeUnavailable);
            }
            tokio::select! {
                () = self.0.shutdown.cancelled() => return Err(EngineError::ShuttingDown),
                () = sleep(Duration::from_millis(50)) => {}
            }
        }
    }
}

#[derive(Clone)]
struct NormalizedRequest {
    input: GenerationInput,
    max_tokens: u32,
    temperature: f32,
    top_p: f32,
    seed: Option<u32>,
    stop: Vec<String>,
}

impl NormalizedRequest {
    const fn path(&self) -> &'static str {
        match self.input {
            GenerationInput::Completion { .. } => "/v1/completions",
            GenerationInput::Chat { .. } => "/v1/chat/completions",
        }
    }

    fn body(&self) -> Value {
        let mut value = serde_json::json!({
            "model": MODEL_ALIAS,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "stop": self.stop,
            "stream": true,
            "stream_options": { "include_usage": true }
        });
        if let Some(seed) = self.seed {
            value["seed"] = Value::from(seed);
        }
        match &self.input {
            GenerationInput::Completion { prompt } => value["prompt"] = Value::from(prompt.clone()),
            GenerationInput::Chat { messages } => {
                value["messages"] = Value::Array(
                    messages
                        .iter()
                        .map(|message| {
                            serde_json::json!({
                                "role": match message.role {
                                    ChatRole::System => "system",
                                    ChatRole::User => "user",
                                    ChatRole::Assistant => "assistant",
                                },
                                "content": message.content
                            })
                        })
                        .collect(),
                );
            }
        }
        value
    }

    fn delta<'a>(&self, value: &'a Value) -> Result<Option<&'a str>, EngineError> {
        let choice = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|v| v.first());
        let Some(choice) = choice else {
            return Ok(None);
        };
        match self.input {
            GenerationInput::Completion { .. } => choice
                .get("text")
                .map(|value| value.as_str().ok_or(EngineError::RuntimeProtocol))
                .transpose(),
            GenerationInput::Chat { .. } => choice
                .get("delta")
                .and_then(|value| value.get("content"))
                .map(|value| value.as_str().ok_or(EngineError::RuntimeProtocol))
                .transpose(),
        }
    }
}

fn normalize_request(
    request: GenerationRequest,
    config: &EngineConfig,
) -> Result<NormalizedRequest, EngineError> {
    if request.model != CURATED_MODEL_ID {
        return Err(EngineError::InvalidRequest(
            "the requested model is not available",
        ));
    }
    if request.max_tokens == 0 || request.max_tokens > config.max_output_tokens {
        return Err(EngineError::InvalidRequest(
            "max_tokens is outside the supported bound",
        ));
    }
    if !request.temperature.is_finite() || !(0.0..=2.0).contains(&request.temperature) {
        return Err(EngineError::InvalidRequest(
            "temperature must be between 0 and 2",
        ));
    }
    if !(request.top_p.is_finite() && 0.0 < request.top_p && request.top_p <= 1.0) {
        return Err(EngineError::InvalidRequest(
            "top_p must be greater than 0 and at most 1",
        ));
    }
    if request.stop.len() > 8
        || request
            .stop
            .iter()
            .any(|value| value.is_empty() || value.len() > 128)
    {
        return Err(EngineError::InvalidRequest(
            "stop sequences exceed the supported bound",
        ));
    }
    let input_bytes = match &request.input {
        GenerationInput::Completion { prompt } => {
            if prompt.is_empty() {
                return Err(EngineError::InvalidRequest("prompt must not be empty"));
            }
            prompt.len()
        }
        GenerationInput::Chat { messages } => {
            if messages.is_empty()
                || messages.len() > config.max_messages
                || messages.iter().any(|message| message.content.is_empty())
            {
                return Err(EngineError::InvalidRequest(
                    "chat messages are outside the supported bound",
                ));
            }
            messages.iter().try_fold(0_usize, |total, message| {
                total
                    .checked_add(message.content.len())
                    .ok_or(EngineError::InvalidRequest(
                        "chat messages are outside the supported bound",
                    ))
            })?
        }
    };
    if input_bytes > config.max_prompt_bytes {
        return Err(EngineError::InvalidRequest(
            "prompt exceeds the supported bound",
        ));
    }
    Ok(NormalizedRequest {
        input: request.input,
        max_tokens: request.max_tokens,
        temperature: request.temperature,
        top_p: if request.temperature == 0.0 {
            1.0
        } else {
            request.top_p
        },
        seed: request.seed,
        stop: request.stop,
    })
}

fn parse_usage(value: &Value) -> Result<Option<TokenUsage>, EngineError> {
    let Some(usage) = value.get("usage") else {
        return Ok(None);
    };
    if usage.is_null() {
        return Ok(None);
    }
    let read = |name| {
        usage
            .get(name)
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(EngineError::RuntimeProtocol)
    };
    let prompt_tokens = read("prompt_tokens")?;
    let completion_tokens = read("completion_tokens")?;
    let total_tokens = read("total_tokens")?;
    if prompt_tokens.saturating_add(completion_tokens) != total_tokens {
        return Err(EngineError::RuntimeProtocol);
    }
    Ok(Some(TokenUsage {
        prompt_tokens,
        completion_tokens,
        total_tokens,
    }))
}

fn parse_finish_reason(value: &Value) -> Result<Option<FinishReason>, EngineError> {
    let Some(reason) = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("finish_reason"))
    else {
        return Ok(None);
    };
    if reason.is_null() {
        return Ok(None);
    }
    match reason.as_str() {
        Some("stop") => Ok(Some(FinishReason::Stop)),
        Some("length") => Ok(Some(FinishReason::Length)),
        _ => Err(EngineError::RuntimeProtocol),
    }
}

async fn send_event(
    sender: &mpsc::Sender<Result<GenerationEvent, EngineError>>,
    event: GenerationEvent,
    context: &RequestContext,
    disconnected: &CancellationToken,
    shutdown: &CancellationToken,
) -> Result<(), EngineError> {
    tokio::select! {
        biased;
        stop = context.stopped() => Err(map_stop(stop)),
        () = disconnected.cancelled() => Err(EngineError::Cancelled),
        () = shutdown.cancelled() => Err(EngineError::ShuttingDown),
        result = sender.send(Ok(event)) => result.map_err(|_| EngineError::Cancelled),
    }
}

const fn map_stop(stop: RequestStop) -> EngineError {
    match stop {
        RequestStop::Cancelled => EngineError::Cancelled,
        RequestStop::DeadlineExceeded => EngineError::DeadlineExceeded,
    }
}

fn reserve_ephemeral_port() -> Result<u16, EngineError> {
    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .map_err(|_| EngineError::RuntimeUnavailable)?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|_| EngineError::RuntimeUnavailable)
}

fn ephemeral_bearer() -> Result<String, EngineError> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| EngineError::RuntimeUnavailable)?;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        write!(output, "{byte:02x}").map_err(|_| EngineError::RuntimeUnavailable)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use axum::{
        Json, Router,
        body::Body,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use impossible_inferences_domain::{
        CURATED_MODEL_ID, ChatMessage, ChatRole, FinishReason, GenerationEvent, GenerationInput,
        GenerationRequest, TokenUsage,
    };
    use impossible_server_core::{CancellationToken, RequestContext, RequestIdSource};
    use serde_json::Value;
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle, time::timeout};

    use super::{
        BoxFuture, EngineConfig, EngineError, GenerationEngine, LaunchAssets, LaunchSpec,
        ManagedChild, SidecarLauncher, build_command, ephemeral_bearer, normalize_request,
    };

    #[derive(Default)]
    struct FakeControl {
        launches: AtomicUsize,
        terminations: AtomicUsize,
        stall: AtomicBool,
        servers: Mutex<Vec<CancellationToken>>,
        requests: Mutex<Vec<Value>>,
    }

    impl FakeControl {
        async fn crash_latest(&self) -> Result<(), Box<dyn std::error::Error>> {
            let token = self
                .servers
                .lock()
                .await
                .last()
                .cloned()
                .ok_or("fake sidecar was not launched")?;
            let _ = token.cancel();
            Ok(())
        }
    }

    struct FakeLauncher {
        control: Arc<FakeControl>,
    }

    #[derive(Clone)]
    struct FakeState {
        bearer: String,
        requests: Arc<FakeControl>,
    }

    impl SidecarLauncher for FakeLauncher {
        fn launch<'a>(
            &'a self,
            spec: &'a LaunchSpec,
        ) -> BoxFuture<'a, Result<Box<dyn ManagedChild>, EngineError>> {
            Box::pin(async move {
                let listener = TcpListener::bind(("127.0.0.1", spec.port))
                    .await
                    .map_err(|_| EngineError::RuntimeUnavailable)?;
                let stop = CancellationToken::new();
                self.control.servers.lock().await.push(stop.clone());
                self.control.launches.fetch_add(1, Ordering::AcqRel);
                let state = FakeState {
                    bearer: spec.bearer.clone(),
                    requests: self.control.clone(),
                };
                let router = Router::new()
                    .route("/health", get(|| async { StatusCode::OK }))
                    .route("/v1/completions", post(fake_generation))
                    .route("/v1/chat/completions", post(fake_generation))
                    .with_state(state);
                let server_stop = stop.clone();
                let join = tokio::spawn(async move {
                    let _ = axum::serve(listener, router)
                        .with_graceful_shutdown(async move { server_stop.cancelled().await })
                        .await;
                });
                Ok(Box::new(FakeChild {
                    stop,
                    join,
                    control: self.control.clone(),
                }) as Box<dyn ManagedChild>)
            })
        }
    }

    async fn fake_generation(
        State(state): State<FakeState>,
        headers: HeaderMap,
        Json(request): Json<Value>,
    ) -> Response {
        let expected = format!("Bearer {}", state.bearer);
        if headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(expected.as_str())
        {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        state.requests.requests.lock().await.push(request.clone());
        if state.requests.stall.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        let delta_field = if request.get("messages").is_some() {
            "\"delta\":{\"content\":\"hello\"}"
        } else {
            "\"text\":\"hello\""
        };
        let body = format!(
            "data: {{\"choices\":[{{{delta_field},\"finish_reason\":null}}]}}\n\n\
             data: {{\"choices\":[{{\"finish_reason\":\"stop\"}}]}}\n\n\
             data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}}}\n\n\
             data: [DONE]\n\n"
        );
        Response::builder()
            .header("content-type", "text/event-stream")
            .body(Body::from(body))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
    }

    struct FakeChild {
        stop: CancellationToken,
        join: JoinHandle<()>,
        control: Arc<FakeControl>,
    }

    impl Drop for FakeChild {
        fn drop(&mut self) {
            let _ = self.stop.cancel();
        }
    }

    impl ManagedChild for FakeChild {
        fn try_exited(&mut self) -> Result<bool, EngineError> {
            Ok(self.stop.is_cancelled() || self.join.is_finished())
        }

        fn terminate(&mut self, deadline: Duration) -> BoxFuture<'_, Result<(), EngineError>> {
            Box::pin(async move {
                self.control.terminations.fetch_add(1, Ordering::AcqRel);
                let _ = self.stop.cancel();
                timeout(deadline, &mut self.join)
                    .await
                    .map_err(|_| EngineError::RuntimeUnavailable)?
                    .map_err(|_| EngineError::RuntimeUnavailable)?;
                Ok(())
            })
        }
    }

    fn assets() -> LaunchAssets {
        LaunchAssets {
            runtime_directory: PathBuf::from("private-runtime"),
            runtime_executable: PathBuf::from("private-runtime/llama-server"),
            model_file: PathBuf::from("private-model/model.gguf"),
        }
    }

    async fn engine(
        config: EngineConfig,
    ) -> Result<(GenerationEngine, Arc<FakeControl>), EngineError> {
        let control = Arc::new(FakeControl::default());
        let engine = GenerationEngine::start(
            assets(),
            config,
            Arc::new(FakeLauncher {
                control: control.clone(),
            }),
        )
        .await?;
        Ok((engine, control))
    }

    fn context(timeout: Duration) -> Result<RequestContext, Box<dyn std::error::Error>> {
        Ok(RequestContext::new(
            RequestIdSource::default().next()?,
            CancellationToken::new(),
            Some(timeout),
        )?)
    }

    fn completion() -> GenerationRequest {
        GenerationRequest {
            model: CURATED_MODEL_ID.to_owned(),
            input: GenerationInput::Completion {
                prompt: "Say hello".to_owned(),
            },
            max_tokens: 16,
            temperature: 0.0,
            top_p: 0.25,
            seed: Some(7),
            stop: vec!["END".to_owned()],
        }
    }

    #[test]
    fn request_normalization_is_bounded_and_deterministic() -> Result<(), EngineError> {
        let normalized = normalize_request(completion(), &EngineConfig::default())?;
        let body = normalized.body();
        assert_eq!(body["temperature"], 0.0);
        assert_eq!(body["top_p"], 1.0);
        assert_eq!(body["seed"], 7);
        assert_eq!(body["stream"], true);
        assert!(body.get("tools").is_none());

        let mut wrong = completion();
        wrong.model = "another-model".to_owned();
        assert!(matches!(
            normalize_request(wrong, &EngineConfig::default()),
            Err(EngineError::InvalidRequest(_))
        ));
        let mut oversized = completion();
        oversized.input = GenerationInput::Completion {
            prompt: "x".repeat(65_537),
        };
        assert!(normalize_request(oversized, &EngineConfig::default()).is_err());
        Ok(())
    }

    #[test]
    fn real_launch_is_loopback_private_and_disables_auxiliary_surfaces()
    -> Result<(), Box<dyn std::error::Error>> {
        let spec = LaunchSpec {
            assets: assets(),
            port: 31_337,
            bearer: ["test", "secret"].join("-"),
        };
        let command = build_command(&spec);
        let arguments: Vec<String> = command
            .as_std()
            .get_args()
            .map(|value| value.to_string_lossy().into_owned())
            .collect();
        let joined = arguments.join(" ");
        assert!(joined.contains("--host 127.0.0.1"));
        assert!(joined.contains("--port 31337"));
        assert!(!joined.contains("--api-key"));
        assert!(command.as_std().get_envs().any(|(name, value)| {
            name == "LLAMA_API_KEY" && value.is_some_and(|value| value == "test-secret")
        }));
        assert!(joined.contains("--no-webui"));
        assert!(joined.contains("--no-slots"));
        assert!(joined.contains("--no-mmproj"));
        assert!(joined.contains("--offline"));
        assert!(joined.contains("--no-cache-prompt"));
        assert!(joined.contains("--chat-template chatml"));
        for forbidden in [
            "--model-url",
            "--models-dir",
            "--metrics",
            "--props",
            "--tools",
            "--media",
            "--router",
        ] {
            assert!(!arguments.iter().any(|value| value == forbidden));
        }
        let first = ephemeral_bearer()?;
        let second = ephemeral_bearer()?;
        assert_eq!(first.len(), 64);
        assert_ne!(first, second);
        Ok(())
    }

    #[tokio::test]
    async fn fake_sidecar_streams_completion_and_chat_with_normalized_usage()
    -> Result<(), Box<dyn std::error::Error>> {
        let (engine, control) = engine(EngineConfig {
            startup_timeout: Duration::from_secs(1),
            ..EngineConfig::default()
        })
        .await?;
        let mut stream = engine.generate(completion(), context(Duration::from_secs(2))?)?;
        let mut events = Vec::new();
        while let Some(event) = stream.recv().await {
            events.push(event?);
        }
        assert_eq!(
            events,
            vec![
                GenerationEvent::Delta("hello".to_owned()),
                GenerationEvent::Usage(TokenUsage {
                    prompt_tokens: 3,
                    completion_tokens: 1,
                    total_tokens: 4,
                }),
                GenerationEvent::Finished(FinishReason::Stop),
            ]
        );

        let chat = GenerationRequest {
            input: GenerationInput::Chat {
                messages: vec![
                    ChatMessage {
                        role: ChatRole::System,
                        content: "Be concise".to_owned(),
                    },
                    ChatMessage {
                        role: ChatRole::User,
                        content: "Hello".to_owned(),
                    },
                ],
            },
            ..completion()
        };
        let mut stream = engine.generate(chat, context(Duration::from_secs(2))?)?;
        assert_eq!(
            stream.recv().await.transpose()?,
            Some(GenerationEvent::Delta("hello".to_owned()))
        );
        drop(stream);
        let requests = control.requests.lock().await;
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[1]["messages"][0]["role"], "system");
        assert!(requests[1].get("prompt").is_none());
        drop(requests);
        engine.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn queue_disconnect_restart_and_shutdown_are_fenced()
    -> Result<(), Box<dyn std::error::Error>> {
        let (engine, control) = engine(EngineConfig {
            queue_capacity: 0,
            event_capacity: 1,
            startup_timeout: Duration::from_secs(1),
            ..EngineConfig::default()
        })
        .await?;
        let first = engine.generate(completion(), context(Duration::from_secs(5))?)?;
        assert!(matches!(
            engine.generate(completion(), context(Duration::from_secs(1))?),
            Err(EngineError::Overloaded)
        ));
        drop(first);
        tokio::time::sleep(Duration::from_millis(50)).await;

        control.crash_latest().await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!engine.status().await.ready);
        let mut restarted = engine.generate(completion(), context(Duration::from_secs(2))?)?;
        assert_eq!(
            restarted.recv().await.transpose()?,
            Some(GenerationEvent::Delta("hello".to_owned()))
        );
        drop(restarted);
        assert_eq!(control.launches.load(Ordering::Acquire), 2);
        engine.shutdown().await?;
        assert!(control.terminations.load(Ordering::Acquire) >= 1);
        assert!(matches!(
            engine.generate(completion(), context(Duration::from_secs(1))?),
            Err(EngineError::ShuttingDown)
        ));
        Ok(())
    }

    #[tokio::test]
    async fn deadlines_and_caller_cancellation_fence_late_output()
    -> Result<(), Box<dyn std::error::Error>> {
        let (engine, control) = engine(EngineConfig {
            startup_timeout: Duration::from_secs(1),
            ..EngineConfig::default()
        })
        .await?;
        control.stall.store(true, Ordering::Release);
        let mut deadline = engine.generate(completion(), context(Duration::from_millis(20))?)?;
        let deadline_event = timeout(Duration::from_secs(1), deadline.recv())
            .await?
            .ok_or("deadline stream closed without an event")?;
        assert_eq!(deadline_event, Err(EngineError::DeadlineExceeded));
        assert!(deadline.recv().await.is_none());

        tokio::time::sleep(Duration::from_millis(20)).await;
        let cancellation = CancellationToken::new();
        let request_context = RequestContext::new(
            RequestIdSource::default().next()?,
            cancellation.clone(),
            Some(Duration::from_secs(2)),
        )?;
        let mut cancelled = engine.generate(completion(), request_context)?;
        let _ = cancellation.cancel();
        let cancelled_event = timeout(Duration::from_secs(1), cancelled.recv())
            .await?
            .ok_or("cancelled stream closed without an event")?;
        assert_eq!(cancelled_event, Err(EngineError::Cancelled));
        assert!(cancelled.recv().await.is_none());
        control.stall.store(false, Ordering::Release);
        engine.shutdown().await?;
        Ok(())
    }
}
