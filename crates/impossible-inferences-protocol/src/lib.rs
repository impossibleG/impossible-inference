//! Public JSON contracts shared by HTTP and WebSocket transports.

/// Generated, versioned gRPC contract.
#[allow(clippy::all, clippy::pedantic, missing_docs)]
pub mod grpc {
    tonic::include_proto!("impossible.inferences.v1");

    /// Encoded service descriptors used by local reflection.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("impossible.inferences.v1");
}

use impossible_inferences_domain::{
    CURATED_MODEL_ID, ChatMessage, ChatRole, GenerationInput, GenerationRequest,
};
use serde::{Deserialize, Serialize};

const DEFAULT_MAX_TOKENS: u32 = 256;
const DEFAULT_TEMPERATURE: f32 = 0.8;
const DEFAULT_TOP_P: f32 = 0.95;

/// A string or an ordered list of strings, matching the `OpenAI` request shape.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StopInput {
    /// One stop string.
    One(String),
    /// Multiple stop strings.
    Many(Vec<String>),
}

impl StopInput {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(value) => vec![value],
            Self::Many(values) => values,
        }
    }
}

/// Bounded `OpenAI`-compatible completion request subset.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompletionRequest {
    /// Must name the curated model exactly.
    pub model: String,
    /// Raw completion prompt.
    pub prompt: String,
    /// Maximum generated tokens.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Sampling temperature.
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    /// Nucleus probability.
    #[serde(default = "default_top_p")]
    pub top_p: f32,
    /// Optional deterministic seed.
    #[serde(default)]
    pub seed: Option<u32>,
    /// Stop string or strings.
    #[serde(default)]
    pub stop: Option<StopInput>,
    /// Whether to return incremental SSE events.
    #[serde(default)]
    pub stream: bool,
}

impl CompletionRequest {
    /// Converts the wire request to the shared engine contract.
    #[must_use]
    pub fn into_generation(self) -> GenerationRequest {
        GenerationRequest {
            model: self.model,
            input: GenerationInput::Completion {
                prompt: self.prompt,
            },
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            top_p: self.top_p,
            seed: self.seed,
            stop: self.stop.map_or_else(Vec::new, StopInput::into_vec),
        }
    }
}

/// One `OpenAI`-compatible chat message.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatMessageInput {
    /// Supported role.
    pub role: ChatRoleInput,
    /// Plain text only in v0.1.
    pub content: String,
}

/// Public chat roles.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRoleInput {
    /// System instruction.
    System,
    /// User input.
    User,
    /// Prior assistant output.
    Assistant,
}

/// Bounded `OpenAI`-compatible chat-completion request subset.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChatCompletionRequest {
    /// Must name the curated model exactly.
    pub model: String,
    /// Ordered messages.
    pub messages: Vec<ChatMessageInput>,
    /// Maximum generated tokens.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Sampling temperature.
    #[serde(default = "default_temperature")]
    pub temperature: f32,
    /// Nucleus probability.
    #[serde(default = "default_top_p")]
    pub top_p: f32,
    /// Optional deterministic seed.
    #[serde(default)]
    pub seed: Option<u32>,
    /// Stop string or strings.
    #[serde(default)]
    pub stop: Option<StopInput>,
    /// Whether to return incremental SSE events.
    #[serde(default)]
    pub stream: bool,
}

impl ChatCompletionRequest {
    /// Converts the wire request to the shared engine contract.
    #[must_use]
    pub fn into_generation(self) -> GenerationRequest {
        GenerationRequest {
            model: self.model,
            input: GenerationInput::Chat {
                messages: self
                    .messages
                    .into_iter()
                    .map(|message| ChatMessage {
                        role: match message.role {
                            ChatRoleInput::System => ChatRole::System,
                            ChatRoleInput::User => ChatRole::User,
                            ChatRoleInput::Assistant => ChatRole::Assistant,
                        },
                        content: message.content,
                    })
                    .collect(),
            },
            max_tokens: self.max_tokens,
            temperature: self.temperature,
            top_p: self.top_p,
            seed: self.seed,
            stop: self.stop.map_or_else(Vec::new, StopInput::into_vec),
        }
    }
}

/// One versioned WebSocket client message.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum WebSocketClientMessage {
    /// Start one completion or chat request.
    Start {
        /// Caller-selected identifier, echoed unchanged.
        id: String,
        /// Generation payload.
        request: WebSocketGeneration,
    },
    /// Cancel the active request with this identifier.
    Cancel {
        /// Active caller-selected identifier.
        id: String,
    },
    /// Application-level heartbeat.
    Ping,
    /// Gracefully close the session.
    Close,
}

/// Completion or chat generation carried by WebSocket.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WebSocketGeneration {
    /// Completion request; `stream` is implicit.
    Completion {
        /// Public completion fields.
        #[serde(flatten)]
        request: CompletionRequest,
    },
    /// Chat request; `stream` is implicit.
    Chat {
        /// Public chat fields.
        #[serde(flatten)]
        request: ChatCompletionRequest,
    },
}

impl WebSocketGeneration {
    /// Converts to the shared engine request and ignores the HTTP-only `stream` switch.
    #[must_use]
    pub fn into_generation(self) -> GenerationRequest {
        match self {
            Self::Completion { request } => request.into_generation(),
            Self::Chat { request } => request.into_generation(),
        }
    }
}

/// One versioned WebSocket server message.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebSocketServerMessage<'a> {
    /// Session is ready for one active request.
    Ready {
        /// Protocol version.
        version: u8,
        /// Curated model identity.
        model: &'a str,
    },
    /// Ordered text fragment.
    Delta {
        /// Caller-selected request identifier.
        id: &'a str,
        /// Text fragment.
        delta: &'a str,
    },
    /// Final usage and reason.
    Final {
        /// Caller-selected request identifier.
        id: &'a str,
        /// Stop or length.
        finish_reason: &'a str,
        /// Prompt tokens.
        prompt_tokens: u32,
        /// Completion tokens.
        completion_tokens: u32,
        /// Total tokens.
        total_tokens: u32,
    },
    /// Stable public error.
    Error {
        /// Related request identifier when available.
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<&'a str>,
        /// Stable error code.
        code: &'a str,
        /// Stable public message.
        message: &'a str,
    },
    /// Application-level heartbeat response.
    Pong,
}

/// Returns the only model identifier accepted by v0.1 transports.
#[must_use]
pub const fn model_id() -> &'static str {
    CURATED_MODEL_ID
}

const fn default_max_tokens() -> u32 {
    DEFAULT_MAX_TOKENS
}

const fn default_temperature() -> f32 {
    DEFAULT_TEMPERATURE
}

const fn default_top_p() -> f32 {
    DEFAULT_TOP_P
}
