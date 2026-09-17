//! Transport-independent, bounded token-generation contracts.

use serde::{Deserialize, Serialize};

/// Stable identity of the curated v0.1 model.
pub const CURATED_MODEL_ID: &str = "qwen2.5-0.5b-instruct-q4-k-m";

/// A normalized text-generation input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GenerationInput {
    /// Continue a raw text prompt.
    Completion {
        /// Prompt passed to the curated model.
        prompt: String,
    },
    /// Generate the next assistant message from an ordered conversation.
    Chat {
        /// Ordered, non-empty conversation.
        messages: Vec<ChatMessage>,
    },
}

/// One chat message accepted by the curated template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    /// Message role.
    pub role: ChatRole,
    /// Plain-text message content.
    pub content: String,
}

/// Roles supported by the curated Qwen `ChatML` template.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChatRole {
    /// Instructions that apply to the conversation.
    System,
    /// User input.
    User,
    /// Prior model output.
    Assistant,
}

/// Normalized generation controls shared by every public transport.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenerationRequest {
    /// Curated model identity. No implicit substitution is allowed.
    pub model: String,
    /// Completion or chat input.
    pub input: GenerationInput,
    /// Maximum number of new tokens.
    pub max_tokens: u32,
    /// Sampling temperature in the inclusive range `0..=2`.
    pub temperature: f32,
    /// Nucleus-sampling probability in the range `(0, 1]`.
    pub top_p: f32,
    /// Optional deterministic llama.cpp seed.
    pub seed: Option<u32>,
    /// Ordered stop strings.
    pub stop: Vec<String>,
}

/// One ordered event emitted by the transport-neutral engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerationEvent {
    /// A non-empty incremental text fragment.
    Delta(String),
    /// Final token accounting reported by the runtime.
    Usage(TokenUsage),
    /// Deterministic terminal event. No event follows it.
    Finished(FinishReason),
}

/// Normalized token accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    /// Tokens consumed by the prompt and template.
    pub prompt_tokens: u32,
    /// Tokens generated for the response.
    pub completion_tokens: u32,
    /// Sum of prompt and completion tokens.
    pub total_tokens: u32,
}

/// Why generation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    /// The runtime reached an end token or a requested stop sequence.
    Stop,
    /// The configured output-token bound was reached.
    Length,
    /// The caller, server, or deadline stopped the request.
    Cancelled,
}
