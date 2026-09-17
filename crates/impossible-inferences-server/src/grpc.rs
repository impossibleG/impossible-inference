//! Bounded local gRPC completion and chat transport.

#![allow(clippy::result_large_err)]

use std::{io, pin::Pin, sync::Arc, time::Duration};

use impossible_inferences_domain::{
    ChatMessage, ChatRole, FinishReason, GenerationEvent as DomainEvent, GenerationInput,
    GenerationRequest, TokenUsage,
};
use impossible_inferences_engine::EngineError;
use impossible_inferences_protocol::grpc::{
    self as pb, ChatRequest, CompletionRequest, GenerationEvent, GenerationResponse, Sampling,
    Usage, generation_event,
    inference_service_server::{InferenceService, InferenceServiceServer},
};
use impossible_server_core::{CancellationToken, RequestContext, RequestIdSource};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Server};

use crate::{EngineEventStream, InferenceEngine};

const DEFAULT_GRPC_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_GRPC_DEADLINE_MARGIN: Duration = Duration::from_millis(50);
const MAX_GRPC_MESSAGE_BYTES: usize = 1_048_576;
const DEFAULT_MAX_TOKENS: u32 = 256;
const DEFAULT_TEMPERATURE: f32 = 0.8;
const DEFAULT_TOP_P: f32 = 0.95;
const STREAM_BUFFER: usize = 16;
const HEALTH_REFRESH: Duration = Duration::from_millis(100);

#[derive(Clone)]
struct GrpcInference {
    engine: Arc<dyn InferenceEngine>,
    request_ids: RequestIdSource,
    shutdown: CancellationToken,
}

struct CancellationGuard(CancellationToken);

impl Drop for CancellationGuard {
    fn drop(&mut self) {
        let _ = self.0.cancel();
    }
}

impl GrpcInference {
    fn new(engine: Arc<dyn InferenceEngine>, shutdown: CancellationToken) -> Self {
        Self {
            engine,
            request_ids: RequestIdSource::default(),
            shutdown,
        }
    }

    fn context<T>(
        &self,
        request: &Request<T>,
    ) -> Result<(RequestContext, CancellationToken), Status> {
        let timeout = grpc_timeout(request.metadata())?.unwrap_or(DEFAULT_GRPC_TIMEOUT);
        if timeout.is_zero() {
            return Err(Status::deadline_exceeded("request deadline exceeded"));
        }
        let cancellation = CancellationToken::new();
        let context = RequestContext::new(
            self.request_ids
                .next()
                .map_err(|_| Status::internal("request identity unavailable"))?,
            cancellation.clone(),
            Some(timeout),
        )
        .map_err(|_| Status::internal("request context unavailable"))?;
        Ok((context, cancellation))
    }

    async fn unary<T>(
        &self,
        request: Request<T>,
        convert: impl FnOnce(T) -> Result<GenerationRequest, Status>,
    ) -> Result<Response<GenerationResponse>, Status> {
        let (context, cancellation) = self.context(&request)?;
        let _guard = CancellationGuard(cancellation.clone());
        let request_id = format!("grpc-{}", context.id().get());
        let generation = convert(request.into_inner())?;
        let events = self
            .engine
            .generate(generation, context)
            .map_err(engine_status)?;
        let completed = tokio::select! {
            biased;
            () = self.shutdown.cancelled() => {
                let _ = cancellation.cancel();
                return Err(Status::unavailable("server is shutting down"));
            }
            completed = collect(events) => completed?,
        };
        Ok(Response::new(GenerationResponse {
            request_id,
            model: impossible_inferences_protocol::model_id().to_owned(),
            text: completed.text,
            usage: Some(usage(completed.usage)),
            finish_reason: finish_reason(completed.finish).to_owned(),
        }))
    }

    fn streaming<T>(
        &self,
        request: Request<T>,
        convert: impl FnOnce(T) -> Result<GenerationRequest, Status>,
    ) -> Result<Response<GrpcEventStream>, Status> {
        let (context, cancellation) = self.context(&request)?;
        let request_id = format!("grpc-{}", context.id().get());
        let generation = convert(request.into_inner())?;
        let mut events = self
            .engine
            .generate(generation, context)
            .map_err(engine_status)?;
        let (sender, receiver) = mpsc::channel(STREAM_BUFFER);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        let _ = cancellation.cancel();
                        return;
                    }
                    () = sender.closed() => {
                        let _ = cancellation.cancel();
                        return;
                    }
                    event = events.recv() => {
                        let Some(event) = event else {
                            return;
                        };
                        let output = event
                            .map(|event| grpc_event(&request_id, event))
                            .map_err(engine_status);
                        let terminal = matches!(output, Ok(GenerationEvent {
                            payload: Some(generation_event::Payload::FinishReason(_)),
                            ..
                        }) | Err(_));
                        if sender.send(output).await.is_err() {
                            let _ = cancellation.cancel();
                            return;
                        }
                        if terminal {
                            return;
                        }
                    }
                }
            }
        });
        Ok(Response::new(
            Box::pin(ReceiverStream::new(receiver)) as GrpcEventStream
        ))
    }
}

type GrpcEventStream = Pin<Box<dyn Stream<Item = Result<GenerationEvent, Status>> + Send>>;

#[tonic::async_trait]
impl InferenceService for GrpcInference {
    type CompleteStreamStream = GrpcEventStream;
    type ChatStreamStream = GrpcEventStream;

    async fn complete(
        &self,
        request: Request<CompletionRequest>,
    ) -> Result<Response<GenerationResponse>, Status> {
        self.unary(request, completion_request).await
    }

    async fn complete_stream(
        &self,
        request: Request<CompletionRequest>,
    ) -> Result<Response<Self::CompleteStreamStream>, Status> {
        self.streaming(request, completion_request)
    }

    async fn chat(
        &self,
        request: Request<ChatRequest>,
    ) -> Result<Response<GenerationResponse>, Status> {
        self.unary(request, chat_request).await
    }

    async fn chat_stream(
        &self,
        request: Request<ChatRequest>,
    ) -> Result<Response<Self::ChatStreamStream>, Status> {
        self.streaming(request, chat_request)
    }
}

struct Completed {
    text: String,
    usage: TokenUsage,
    finish: FinishReason,
}

async fn collect(mut events: Box<dyn EngineEventStream>) -> Result<Completed, Status> {
    let mut text = String::new();
    let mut token_usage = None;
    let mut finish = None;
    while let Some(event) = events.recv().await {
        match event.map_err(engine_status)? {
            DomainEvent::Delta(delta) => text.push_str(&delta),
            DomainEvent::Usage(value) => token_usage = Some(value),
            DomainEvent::Finished(value) => finish = Some(value),
        }
    }
    Ok(Completed {
        text,
        usage: token_usage.ok_or_else(|| Status::internal("local runtime ended unexpectedly"))?,
        finish: finish.ok_or_else(|| Status::internal("local runtime ended unexpectedly"))?,
    })
}

#[allow(clippy::unnecessary_wraps)]
fn completion_request(request: CompletionRequest) -> Result<GenerationRequest, Status> {
    Ok(GenerationRequest {
        model: request.model,
        input: GenerationInput::Completion {
            prompt: request.prompt,
        },
        ..sampling(request.sampling)
    })
}

fn chat_request(request: ChatRequest) -> Result<GenerationRequest, Status> {
    let messages = request
        .messages
        .into_iter()
        .map(|message| {
            let role = match pb::ChatRole::try_from(message.role) {
                Ok(pb::ChatRole::System) => ChatRole::System,
                Ok(pb::ChatRole::User) => ChatRole::User,
                Ok(pb::ChatRole::Assistant) => ChatRole::Assistant,
                _ => return Err(Status::invalid_argument("unsupported chat role")),
            };
            Ok(ChatMessage {
                role,
                content: message.content,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GenerationRequest {
        model: request.model,
        input: GenerationInput::Chat { messages },
        ..sampling(request.sampling)
    })
}

fn sampling(sampling: Option<Sampling>) -> GenerationRequest {
    let sampling = sampling.unwrap_or(Sampling {
        max_tokens: DEFAULT_MAX_TOKENS,
        temperature: DEFAULT_TEMPERATURE,
        top_p: DEFAULT_TOP_P,
        seed: None,
        stop: Vec::new(),
    });
    GenerationRequest {
        model: String::new(),
        input: GenerationInput::Completion {
            prompt: String::new(),
        },
        max_tokens: sampling.max_tokens,
        temperature: sampling.temperature,
        top_p: sampling.top_p,
        seed: sampling.seed,
        stop: sampling.stop,
    }
}

fn grpc_event(request_id: &str, event: DomainEvent) -> GenerationEvent {
    let payload = match event {
        DomainEvent::Delta(delta) => generation_event::Payload::Delta(delta),
        DomainEvent::Usage(value) => generation_event::Payload::Usage(usage(value)),
        DomainEvent::Finished(reason) => {
            generation_event::Payload::FinishReason(finish_reason(reason).to_owned())
        }
    };
    GenerationEvent {
        request_id: request_id.to_owned(),
        payload: Some(payload),
    }
}

const fn usage(value: TokenUsage) -> Usage {
    Usage {
        prompt_tokens: value.prompt_tokens,
        completion_tokens: value.completion_tokens,
        total_tokens: value.total_tokens,
    }
}

const fn finish_reason(reason: FinishReason) -> &'static str {
    match reason {
        FinishReason::Stop => "stop",
        FinishReason::Length => "length",
        FinishReason::Cancelled => "cancelled",
    }
}

fn grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Result<Option<Duration>, Status> {
    let Some(value) = metadata.get("grpc-timeout") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    if value.len() < 2 || value.len() > 9 {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    let amount = digits
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    let duration = match unit {
        "H" => Duration::from_secs(amount.saturating_mul(60 * 60)),
        "M" => Duration::from_secs(amount.saturating_mul(60)),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return Err(Status::invalid_argument("invalid grpc-timeout")),
    };
    let duration = duration.min(DEFAULT_GRPC_TIMEOUT);
    let margin = (duration / 2).min(MAX_GRPC_DEADLINE_MARGIN);
    Ok(Some(duration.checked_sub(margin).unwrap_or(Duration::ZERO)))
}

fn engine_status(error: EngineError) -> Status {
    match error {
        EngineError::InvalidRequest(message) => Status::invalid_argument(message),
        EngineError::Overloaded => Status::resource_exhausted("generation capacity exhausted"),
        EngineError::Cancelled => Status::cancelled("request cancelled"),
        EngineError::DeadlineExceeded => Status::deadline_exceeded("request deadline exceeded"),
        EngineError::ArtifactsUnavailable | EngineError::RuntimeUnavailable => {
            Status::unavailable("local runtime unavailable")
        }
        EngineError::ShuttingDown => Status::unavailable("server is shutting down"),
        EngineError::Configuration | EngineError::RuntimeProtocol => {
            Status::internal("local runtime failed")
        }
    }
}

pub(crate) async fn serve(
    engine: Arc<dyn InferenceEngine>,
    listener: TcpListener,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> io::Result<()> {
    let request_shutdown = CancellationToken::new();
    let service =
        InferenceServiceServer::new(GrpcInference::new(engine.clone(), request_shutdown.clone()))
            .max_decoding_message_size(MAX_GRPC_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_BYTES);
    let (mut reporter, health) = tonic_health::server::health_reporter();
    publish_health(&mut reporter, engine.status().await.ready).await;
    let health_shutdown = request_shutdown.clone();
    let health_engine = engine.clone();
    let health_monitor = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                () = health_shutdown.cancelled() => {
                    reporter
                        .set_not_serving::<InferenceServiceServer<GrpcInference>>()
                        .await;
                    return;
                }
                () = tokio::time::sleep(HEALTH_REFRESH) => {
                    publish_health(&mut reporter, health_engine.status().await.ready).await;
                }
            }
        }
    });
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .map_err(io::Error::other)?;
    let shutdown_signal = shutdown.clone();
    let request_stop = request_shutdown.clone();
    let server = Server::builder()
        .add_service(health)
        .add_service(reflection)
        .add_service(service)
        .serve_with_incoming_shutdown(
            tokio_stream::wrappers::TcpListenerStream::new(listener),
            async move {
                shutdown_signal.cancelled().await;
                let _ = request_stop.cancel();
            },
        );
    tokio::pin!(server);
    let result = tokio::select! {
        result = &mut server => result.map_err(io::Error::other),
        () = shutdown.cancelled() => {
            let _ = request_shutdown.cancel();
            match tokio::time::timeout(shutdown_timeout, &mut server).await {
                Ok(result) => result.map_err(io::Error::other),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "gRPC shutdown exceeded its configured bound",
                )),
            }
        }
    };
    let _ = request_shutdown.cancel();
    let _ = health_monitor.await;
    result
}

async fn publish_health(reporter: &mut tonic_health::server::HealthReporter, ready: bool) {
    if ready {
        reporter
            .set_serving::<InferenceServiceServer<GrpcInference>>()
            .await;
    } else {
        reporter
            .set_not_serving::<InferenceServiceServer<GrpcInference>>()
            .await;
    }
}
