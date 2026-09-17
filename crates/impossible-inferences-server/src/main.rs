//! Command-line entry point for Impossible Inferences.

use std::{
    fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

use clap::{Args as ClapArgs, Parser, Subcommand};
use impossible_inferences_artifacts::{SetupMode, Verification, inspect, setup};
use impossible_inferences_engine::{EngineConfig, EngineStatus, GenerationEngine};
use impossible_inferences_server::{InferenceServer, LocalInference, PendingInference};
use impossible_server_core::{CancellationToken, ServerLimits};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

const DEFAULT_BIND: &str = "127.0.0.1:8080";
const DEFAULT_GRPC_BIND: &str = "127.0.0.1:50051";
const DEFAULT_MAX_REQUEST_BYTES: usize = 1_048_576;
const DEFAULT_QUEUE_CAPACITY: usize = 128;
const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 8;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 10_000;
const MAX_CONFIG_BYTES: u64 = 65_536;
const DEFAULT_ARTIFACT_ROOT: &str = "runtime-artifacts";

#[derive(Debug, Parser)]
#[command(
    name = "impossible-inferences",
    version,
    about = "Ready-made local token-completion server"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Download and verify the curated runtime and model.
    Setup(SetupArgs),
    /// Start the local HTTP control plane.
    Serve(ConfigArgs),
    /// Fully verify artifacts and probe the private generation engine.
    Doctor(ConfigArgs),
    /// Report the pre-generation runtime and model state.
    Status(ConfigArgs),
}

#[derive(Debug, Clone, ClapArgs)]
struct SetupArgs {
    /// Managed installation root. Models and runtimes remain ignored by Git.
    #[arg(
        long,
        env = "IMPOSSIBLE_INFERENCES_ARTIFACT_ROOT",
        default_value = DEFAULT_ARTIFACT_ROOT
    )]
    artifact_root: PathBuf,
    /// Verify and recover exclusively from already-downloaded local artifacts.
    #[arg(long, default_value_t = false)]
    offline: bool,
}

#[derive(Debug, Clone, ClapArgs)]
struct ConfigArgs {
    /// Optional bounded JSON configuration file.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_CONFIG")]
    config: Option<PathBuf>,
    /// Managed installation root. Ordinary serve never downloads into it.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_ARTIFACT_ROOT")]
    artifact_root: Option<PathBuf>,
    /// Loopback bind address. Command-line values override environment and file values.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_BIND")]
    bind: Option<SocketAddr>,
    /// Loopback gRPC bind address. Command-line values override environment and file values.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_GRPC_BIND")]
    grpc_bind: Option<SocketAddr>,
    /// Maximum encoded request body in bytes.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_MAX_REQUEST_BYTES")]
    max_request_bytes: Option<usize>,
    /// Maximum requests waiting behind the execution concurrency bound.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_QUEUE_CAPACITY")]
    queue_capacity: Option<usize>,
    /// Maximum concurrently executing workload requests.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_MAX_CONCURRENT_REQUESTS")]
    max_concurrent_requests: Option<usize>,
    /// Overall request deadline in milliseconds, including upload and queue time.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_REQUEST_TIMEOUT_MS")]
    request_timeout_ms: Option<u64>,
    /// Total HTTP drain and workload-cleanup deadline in milliseconds.
    #[arg(long, env = "IMPOSSIBLE_INFERENCES_SHUTDOWN_TIMEOUT_MS")]
    shutdown_timeout_ms: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    artifact_root: Option<PathBuf>,
    bind: Option<SocketAddr>,
    grpc_bind: Option<SocketAddr>,
    max_request_bytes: Option<usize>,
    queue_capacity: Option<usize>,
    max_concurrent_requests: Option<usize>,
    request_timeout_ms: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone)]
struct EffectiveConfig {
    bind: SocketAddr,
    grpc_bind: SocketAddr,
    limits: ServerLimits,
    artifact_root: PathBuf,
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    artifacts: impossible_inferences_artifacts::InstallStatus,
    engine: EngineStatus,
}

#[derive(Debug, Serialize)]
struct StatusReport {
    artifacts: impossible_inferences_artifacts::InstallStatus,
    engine: EngineStatus,
}

impl ConfigArgs {
    fn resolve(&self) -> Result<EffectiveConfig, Box<dyn std::error::Error>> {
        let file = self
            .config
            .as_deref()
            .map(read_config)
            .transpose()?
            .unwrap_or_default();
        let bind = self.bind.or(file.bind).unwrap_or(DEFAULT_BIND.parse()?);
        if !bind.ip().is_loopback() {
            return Err("v0.1 only permits loopback binding".into());
        }
        let grpc_bind = self
            .grpc_bind
            .or(file.grpc_bind)
            .unwrap_or(DEFAULT_GRPC_BIND.parse()?);
        if !grpc_bind.ip().is_loopback() {
            return Err("v0.1 only permits loopback gRPC binding".into());
        }
        if grpc_bind == bind {
            return Err("HTTP and gRPC bind addresses must be different".into());
        }
        let limits = ServerLimits::new(
            self.max_request_bytes
                .or(file.max_request_bytes)
                .unwrap_or(DEFAULT_MAX_REQUEST_BYTES),
            self.queue_capacity
                .or(file.queue_capacity)
                .unwrap_or(DEFAULT_QUEUE_CAPACITY),
            self.max_concurrent_requests
                .or(file.max_concurrent_requests)
                .unwrap_or(DEFAULT_MAX_CONCURRENT_REQUESTS),
            Duration::from_millis(
                self.request_timeout_ms
                    .or(file.request_timeout_ms)
                    .unwrap_or(DEFAULT_REQUEST_TIMEOUT_MS),
            ),
            Duration::from_millis(
                self.shutdown_timeout_ms
                    .or(file.shutdown_timeout_ms)
                    .unwrap_or(DEFAULT_SHUTDOWN_TIMEOUT_MS),
            ),
        )?;
        let artifact_root = self
            .artifact_root
            .clone()
            .or(file.artifact_root)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_ARTIFACT_ROOT));
        if artifact_root.as_os_str().is_empty() {
            return Err("artifact root must not be empty".into());
        }
        Ok(EffectiveConfig {
            bind,
            grpc_bind,
            limits,
            artifact_root,
        })
    }
}

fn read_config(path: &Path) -> Result<FileConfig, Box<dyn std::error::Error>> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        return Err("configuration must be a regular JSON file no larger than 65536 bytes".into());
    }
    let bytes = fs::read(path)?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match Cli::parse().command {
        Command::Setup(arguments) => {
            let mode = if arguments.offline {
                SetupMode::Offline
            } else {
                SetupMode::Online
            };
            let artifact_root = arguments.artifact_root;
            let status = tokio::task::spawn_blocking(move || setup(&artifact_root, mode)).await??;
            println!("{}", serde_json::to_string(&status)?);
            Ok(())
        }
        Command::Serve(arguments) => serve(arguments.resolve()?).await,
        Command::Doctor(arguments) => {
            let config = arguments.resolve()?;
            let artifacts = inspect(&config.artifact_root, Verification::Full);
            let engine = match GenerationEngine::start_from_store(
                &config.artifact_root,
                EngineConfig::default(),
            )
            .await
            {
                Ok(engine) => {
                    let status = engine.status().await;
                    let _ = engine.shutdown().await;
                    status
                }
                Err(_) => unavailable_engine_status(),
            };
            println!(
                "{}",
                serde_json::to_string(&DoctorReport { artifacts, engine })?
            );
            Ok(())
        }
        Command::Status(arguments) => {
            let artifacts = inspect(&arguments.resolve()?.artifact_root, Verification::Fast);
            println!(
                "{}",
                serde_json::to_string(&StatusReport {
                    artifacts,
                    engine: unavailable_engine_status(),
                })?
            );
            Ok(())
        }
    }
}

async fn serve(config: EffectiveConfig) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(config.bind).await?;
    let cancellation = CancellationToken::new();
    let signal = cancellation.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = signal.cancel();
        }
    });
    match GenerationEngine::start_from_store(&config.artifact_root, EngineConfig::default()).await {
        Ok(engine) => {
            let grpc_listener = TcpListener::bind(config.grpc_bind).await?;
            let local = LocalInference::new(engine);
            let grpc = local.clone();
            let server = InferenceServer::with_limits(local, config.limits);
            let http_shutdown = cancellation.clone();
            let grpc_shutdown = cancellation.clone();
            let mut http = Box::pin(server.serve(listener, http_shutdown));
            let mut grpc_server = Box::pin(grpc.serve_grpc(grpc_listener, grpc_shutdown));
            tokio::select! {
                result = &mut http => {
                    let _ = cancellation.cancel();
                    result?;
                    grpc_server.await?;
                }
                result = &mut grpc_server => {
                    let _ = cancellation.cancel();
                    result?;
                    http.await?;
                }
            }
        }
        Err(_) => {
            InferenceServer::with_limits(PendingInference, config.limits)
                .serve(listener, cancellation)
                .await?;
        }
    }
    Ok(())
}

const fn unavailable_engine_status() -> EngineStatus {
    EngineStatus {
        ready: false,
        runtime: "not_running",
        model: "not_loaded",
        profile: impossible_inferences_domain::CURATED_MODEL_ID,
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, net::SocketAddr};

    use clap::Parser;

    use super::{Cli, Command};

    #[test]
    fn defaults_are_loopback_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let cli = Cli::try_parse_from(["impossible-inferences", "doctor"])?;
        let Command::Doctor(arguments) = cli.command else {
            return Err("doctor subcommand was not parsed".into());
        };
        let effective = arguments.resolve()?;
        assert!(effective.bind.ip().is_loopback());
        assert_eq!(effective.bind.port(), 8080);
        assert_eq!(effective.grpc_bind.port(), 50051);
        assert_eq!(effective.limits.max_request_bytes(), 1_048_576);
        assert_eq!(
            effective.artifact_root,
            std::path::Path::new("runtime-artifacts")
        );
        Ok(())
    }

    #[test]
    fn command_line_overrides_file_and_unknown_fields_fail()
    -> Result<(), Box<dyn std::error::Error>> {
        let fixture = std::env::temp_dir().join(format!(
            "impossible-inferences-config-{}-{}",
            std::process::id(),
            std::thread::current()
                .name()
                .unwrap_or("test")
                .replace("::", "-")
        ));
        let _ = fs::remove_file(&fixture);
        fs::write(
            &fixture,
            br#"{"bind":"127.0.0.1:9000","max_request_bytes":2048}"#,
        )?;
        let cli = Cli::try_parse_from([
            "impossible-inferences",
            "doctor",
            "--config",
            fixture.to_str().ok_or("fixture path is not UTF-8")?,
            "--bind",
            "127.0.0.1:9100",
        ])?;
        let Command::Doctor(arguments) = cli.command else {
            return Err("doctor subcommand was not parsed".into());
        };
        let effective = arguments.resolve()?;
        assert_eq!(effective.bind, "127.0.0.1:9100".parse::<SocketAddr>()?);
        assert_eq!(effective.limits.max_request_bytes(), 2048);

        fs::write(&fixture, br#"{"unexpected":true}"#)?;
        assert!(arguments.resolve().is_err());
        fs::remove_file(fixture)?;
        Ok(())
    }

    #[test]
    fn non_loopback_bind_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let cli =
            Cli::try_parse_from(["impossible-inferences", "doctor", "--bind", "0.0.0.0:8080"])?;
        let Command::Doctor(arguments) = cli.command else {
            return Err("doctor subcommand was not parsed".into());
        };
        assert!(arguments.resolve().is_err());
        let cli = Cli::try_parse_from([
            "impossible-inferences",
            "doctor",
            "--grpc-bind",
            "0.0.0.0:50051",
        ])?;
        let Command::Doctor(arguments) = cli.command else {
            return Err("doctor subcommand was not parsed".into());
        };
        assert!(arguments.resolve().is_err());
        Ok(())
    }
}
