//! Verified installation and offline validation of the curated v0.1 artifacts.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Component, Path, PathBuf},
    time::Duration,
};

use flate2::read::GzDecoder;
use fs2::FileExt;
use reqwest::{Url, blocking::Client, redirect::Policy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const EMBEDDED_MANIFEST: &str = include_str!("../../../manifests/artifacts-v1.json");
const MANAGED_MARKER: &str = ".impossible-inferences-managed";
const MANAGED_MARKER_VALUE: &str = "impossible-inferences-artifacts-v1\n";
const STATE_FILE: &str = "install-state.json";
const CURRENT_DIRECTORY: &str = "current";
const STAGING_DIRECTORY: &str = ".staging";
const PREVIOUS_DIRECTORY: &str = ".previous";
const RUNTIME_RECOVERY_DIRECTORY: &str = ".runtime-recovery";

/// Whether setup may access the network.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupMode {
    /// Download missing or invalid artifacts from their immutable manifest URLs.
    Online,
    /// Prohibit network access and require a fully verifiable local installation.
    Offline,
}

/// Cost of installation inspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verification {
    /// Check the pinned identity, expected sizes, and required paths.
    Fast,
    /// Recompute the runtime archive and model SHA-256 digests.
    Full,
}

/// Privacy-safe aggregate installation status.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct InstallStatus {
    /// Status schema version.
    pub schema_version: u8,
    /// Curated profile identifier.
    pub profile: &'static str,
    /// Rust target identifier selected for the host.
    pub target: &'static str,
    /// Overall status (`ready` or `not_ready`).
    pub status: &'static str,
    /// Runtime state.
    pub runtime: &'static str,
    /// Model state.
    pub model: &'static str,
    /// Stable sanitized reason when not ready.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

/// Fully verified local paths required to launch the curated inference runtime.
///
/// This value deliberately omits `Serialize` and redacts paths from `Debug` so diagnostics cannot
/// disclose a user's filesystem layout.
#[derive(Clone)]
pub struct InstalledArtifacts {
    profile: &'static str,
    target: &'static str,
    runtime_directory: PathBuf,
    runtime_executable: PathBuf,
    model_file: PathBuf,
}

impl fmt::Debug for InstalledArtifacts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstalledArtifacts")
            .field("profile", &self.profile)
            .field("target", &self.target)
            .field("paths", &"redacted")
            .finish_non_exhaustive()
    }
}

impl InstalledArtifacts {
    /// Returns the stable curated profile identifier.
    #[must_use]
    pub const fn profile(&self) -> &'static str {
        self.profile
    }

    /// Returns the supported Rust target identifier selected for this host.
    #[must_use]
    pub const fn target(&self) -> &'static str {
        self.target
    }

    /// Returns the directory that must be the child runtime's working directory.
    #[must_use]
    pub fn runtime_directory(&self) -> &Path {
        &self.runtime_directory
    }

    /// Returns the exact verified `llama-server` executable.
    #[must_use]
    pub fn runtime_executable(&self) -> &Path {
        &self.runtime_executable
    }

    /// Returns the exact verified curated GGUF model.
    #[must_use]
    pub fn model_file(&self) -> &Path {
        &self.model_file
    }
}

impl InstallStatus {
    fn ready(profile: &'static str, target: &'static str) -> Self {
        Self {
            schema_version: 1,
            profile,
            target,
            status: "ready",
            runtime: "installed",
            model: "installed",
            reason: None,
        }
    }

    fn not_ready(profile: &'static str, target: &'static str, reason: &'static str) -> Self {
        Self {
            schema_version: 1,
            profile,
            target,
            status: "not_ready",
            runtime: "not_ready",
            model: "not_ready",
            reason: Some(reason),
        }
    }
}

/// Artifact installation failure without local paths, URLs, or response bodies.
#[derive(Debug)]
pub enum ArtifactError {
    /// Local filesystem operation failed.
    Filesystem(io::Error),
    /// The committed or installed JSON failed validation.
    Manifest(&'static str),
    /// The host platform has no curated runtime.
    UnsupportedPlatform,
    /// A network request failed.
    Download(reqwest::Error),
    /// An artifact did not match the committed size or digest.
    Integrity(&'static str),
    /// An archive contained an unsafe or unsupported entry.
    Archive(&'static str),
    /// Offline setup could not verify a complete installation.
    OfflineUnavailable,
}

impl fmt::Display for ArtifactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem(_) => formatter.write_str("artifact filesystem operation failed"),
            Self::Manifest(message) => write!(formatter, "artifact manifest is invalid: {message}"),
            Self::UnsupportedPlatform => {
                formatter.write_str("no curated runtime exists for this platform")
            }
            Self::Download(_) => formatter.write_str("artifact download failed"),
            Self::Integrity(kind) => write!(formatter, "{kind} failed integrity verification"),
            Self::Archive(message) => write!(formatter, "runtime archive is invalid: {message}"),
            Self::OfflineUnavailable => {
                formatter.write_str("offline artifacts are missing or invalid")
            }
        }
    }
}

impl Error for ArtifactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Filesystem(error) => Some(error),
            Self::Download(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ArtifactError {
    fn from(error: io::Error) -> Self {
        Self::Filesystem(error)
    }
}

impl From<reqwest::Error> for ArtifactError {
    fn from(error: reqwest::Error) -> Self {
        Self::Download(error)
    }
}

#[derive(Debug, Deserialize)]
struct ArtifactManifest {
    schema_version: u8,
    product: String,
    profile_id: String,
    model: ModelArtifact,
    runtime: RuntimeManifest,
}

#[derive(Debug, Deserialize)]
struct ModelArtifact {
    repository: String,
    revision: String,
    filename: String,
    url: String,
    size: u64,
    sha256: String,
    license: String,
    license_url: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeManifest {
    project: String,
    release_tag: String,
    semantic_version: String,
    source_commit: String,
    license: String,
    license_url: String,
    platforms: Vec<RuntimeArtifact>,
}

#[derive(Debug, Deserialize)]
struct RuntimeArtifact {
    target: String,
    archive_format: ArchiveFormat,
    filename: String,
    url: String,
    size: u64,
    sha256: String,
    strip_prefix: String,
    executable: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ArchiveFormat {
    Zip,
    TarGz,
}

#[derive(Debug, Serialize, Deserialize)]
struct InstallState {
    schema_version: u8,
    product: String,
    profile_id: String,
    target: String,
    runtime_release: String,
    runtime_sha256: String,
    model_revision: String,
    model_sha256: String,
    runtime_archive: String,
    runtime_executable: String,
    model_file: String,
}

/// Returns the current host target supported by the curated manifest.
#[must_use]
pub const fn host_target() -> Option<&'static str> {
    #[cfg(all(target_arch = "x86_64", target_os = "windows"))]
    {
        return Some("x86_64-pc-windows-msvc");
    }
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        return Some("x86_64-unknown-linux-gnu");
    }
    #[allow(unreachable_code)]
    None
}

/// Installs or verifies the curated artifacts beneath a dedicated managed root.
///
/// # Errors
/// Returns a sanitized failure when the platform, manifest, download, archive, or local state is
/// invalid. Offline mode never creates a network client or starts a request.
pub fn setup(root: &Path, mode: SetupMode) -> Result<InstallStatus, ArtifactError> {
    let manifest = parse_manifest()?;
    let target = host_target().ok_or(ArtifactError::UnsupportedPlatform)?;
    let runtime = select_runtime(&manifest, target)?;
    ensure_root(root)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("install.lock"))?;
    lock.lock_exclusive()?;

    let current = root.join(CURRENT_DIRECTORY);
    if verify_current(&current, &manifest, runtime, Verification::Full).is_ok() {
        return Ok(InstallStatus::ready(
            profile_name(&manifest)?,
            target_name(runtime)?,
        ));
    }
    if mode == SetupMode::Offline {
        recover_runtime(root, &current, &manifest, runtime)
            .map_err(|_| ArtifactError::OfflineUnavailable)?;
        verify_current(&current, &manifest, runtime, Verification::Full)
            .map_err(|_| ArtifactError::OfflineUnavailable)?;
        return Ok(InstallStatus::ready(
            profile_name(&manifest)?,
            target_name(runtime)?,
        ));
    }

    let staging = root.join(STAGING_DIRECTORY);
    remove_managed_directory_if_present(&staging)?;
    fs::create_dir(&staging)?;
    write_marker(&staging)?;
    let downloads = staging.join("downloads");
    let runtime_directory = staging.join("runtime");
    let model_directory = staging.join("model");
    fs::create_dir(&downloads)?;
    fs::create_dir(&runtime_directory)?;
    fs::create_dir(&model_directory)?;

    let runtime_archive = downloads.join(&runtime.filename);
    let model_file = model_directory.join(&manifest.model.filename);
    let client = download_client()?;
    download_verified(
        &client,
        &runtime.url,
        &runtime_archive,
        runtime.size,
        &runtime.sha256,
        "runtime",
    )?;
    download_verified(
        &client,
        &manifest.model.url,
        &model_file,
        manifest.model.size,
        &manifest.model.sha256,
        "model",
    )?;
    extract_runtime(runtime, &runtime_archive, &runtime_directory)?;
    if !runtime_directory.join(&runtime.executable).is_file() {
        return Err(ArtifactError::Archive("runtime executable is missing"));
    }
    let state = InstallState {
        schema_version: 1,
        product: manifest.product.clone(),
        profile_id: manifest.profile_id.clone(),
        target: runtime.target.clone(),
        runtime_release: manifest.runtime.release_tag.clone(),
        runtime_sha256: runtime.sha256.clone(),
        model_revision: manifest.model.revision.clone(),
        model_sha256: manifest.model.sha256.clone(),
        runtime_archive: format!("downloads/{}", runtime.filename),
        runtime_executable: format!("runtime/{}", runtime.executable),
        model_file: format!("model/{}", manifest.model.filename),
    };
    write_state(&staging, &state)?;
    verify_current(&staging, &manifest, runtime, Verification::Full)?;
    promote(root, &staging, &current)?;
    verify_current(&current, &manifest, runtime, Verification::Full)?;
    Ok(InstallStatus::ready(
        profile_name(&manifest)?,
        target_name(runtime)?,
    ))
}

/// Inspects the current installation without performing setup or network access.
#[must_use]
pub fn inspect(root: &Path, verification: Verification) -> InstallStatus {
    let Ok(manifest) = parse_manifest() else {
        return InstallStatus::not_ready("unknown", "unknown", "manifest_invalid");
    };
    let Some(target) = host_target() else {
        return InstallStatus::not_ready(
            profile_name(&manifest).unwrap_or("unknown"),
            "unsupported",
            "unsupported_platform",
        );
    };
    let Ok(runtime) = select_runtime(&manifest, target) else {
        return InstallStatus::not_ready(
            profile_name(&manifest).unwrap_or("unknown"),
            target,
            "runtime_unavailable",
        );
    };
    match verify_current(
        &root.join(CURRENT_DIRECTORY),
        &manifest,
        runtime,
        verification,
    ) {
        Ok(()) => InstallStatus::ready(
            profile_name(&manifest).unwrap_or("unknown"),
            target_name(runtime).unwrap_or(target),
        ),
        Err(_) => InstallStatus::not_ready(
            profile_name(&manifest).unwrap_or("unknown"),
            target_name(runtime).unwrap_or(target),
            "artifacts_missing_or_invalid",
        ),
    }
}

/// Resolves the exact installed runtime and model after offline verification.
///
/// This function never creates a network client and never repairs or mutates the installation.
///
/// # Errors
/// Returns a sanitized failure if the pinned installation is absent, redirected, or invalid.
pub fn resolve_installed(
    root: &Path,
    verification: Verification,
) -> Result<InstalledArtifacts, ArtifactError> {
    let manifest = parse_manifest()?;
    let target = host_target().ok_or(ArtifactError::UnsupportedPlatform)?;
    let runtime = select_runtime(&manifest, target)?;
    let current = root.join(CURRENT_DIRECTORY);
    verify_current(&current, &manifest, runtime, verification)?;
    let state = read_and_validate_state(&current, &manifest, runtime)?;
    let canonical_current = fs::canonicalize(&current)?;
    let runtime_executable = fs::canonicalize(current.join(&state.runtime_executable))?;
    let model_file = fs::canonicalize(current.join(&state.model_file))?;
    let runtime_directory = runtime_executable
        .parent()
        .ok_or(ArtifactError::Manifest(
            "runtime executable has no directory",
        ))?
        .to_path_buf();
    if !runtime_executable.starts_with(&canonical_current)
        || !model_file.starts_with(&canonical_current)
        || !runtime_directory.starts_with(&canonical_current)
    {
        return Err(ArtifactError::Manifest(
            "installed paths escape managed root",
        ));
    }
    Ok(InstalledArtifacts {
        profile: profile_name(&manifest)?,
        target: target_name(runtime)?,
        runtime_directory,
        runtime_executable,
        model_file,
    })
}

fn parse_manifest() -> Result<ArtifactManifest, ArtifactError> {
    let manifest: ArtifactManifest = serde_json::from_str(EMBEDDED_MANIFEST)
        .map_err(|_| ArtifactError::Manifest("JSON cannot be decoded"))?;
    validate_manifest(&manifest)?;
    Ok(manifest)
}

fn validate_manifest(manifest: &ArtifactManifest) -> Result<(), ArtifactError> {
    if manifest.schema_version != 1 || manifest.product != "impossible-inferences-artifacts" {
        return Err(ArtifactError::Manifest("wrong schema or product"));
    }
    if manifest.profile_id.is_empty()
        || manifest.model.repository != "Qwen/Qwen2.5-0.5B-Instruct-GGUF"
        || manifest.model.revision.len() != 40
        || manifest.model.filename != "qwen2.5-0.5b-instruct-q4_k_m.gguf"
        || manifest.model.size == 0
        || !valid_sha256(&manifest.model.sha256)
        || manifest.model.license != "Apache-2.0"
        || manifest.runtime.project != "ggml-org/llama.cpp"
        || manifest.runtime.release_tag != "b10964"
        || manifest.runtime.semantic_version != "v0.4.1"
        || manifest.runtime.source_commit.len() != 40
        || manifest.runtime.license != "MIT"
        || manifest.runtime.platforms.len() != 2
    {
        return Err(ArtifactError::Manifest("curated identity is incomplete"));
    }
    validate_https(&manifest.model.url)?;
    validate_https(&manifest.model.license_url)?;
    validate_https(&manifest.runtime.license_url)?;
    for runtime in &manifest.runtime.platforms {
        if runtime.size == 0
            || !valid_sha256(&runtime.sha256)
            || safe_relative(Path::new(&runtime.filename)).is_none()
            || safe_relative(Path::new(&runtime.executable)).is_none()
        {
            return Err(ArtifactError::Manifest("runtime entry is incomplete"));
        }
        validate_https(&runtime.url)?;
    }
    Ok(())
}

fn validate_https(value: &str) -> Result<(), ArtifactError> {
    let url = Url::parse(value).map_err(|_| ArtifactError::Manifest("URL cannot be parsed"))?;
    if url.scheme() != "https" || url.host_str().is_none() {
        return Err(ArtifactError::Manifest("URL is not HTTPS"));
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn profile_name(manifest: &ArtifactManifest) -> Result<&'static str, ArtifactError> {
    if manifest.profile_id == "qwen2.5-0.5b-instruct-q4-k-m" {
        Ok("qwen2.5-0.5b-instruct-q4-k-m")
    } else {
        Err(ArtifactError::Manifest("unknown profile"))
    }
}

fn target_name(runtime: &RuntimeArtifact) -> Result<&'static str, ArtifactError> {
    match runtime.target.as_str() {
        "x86_64-pc-windows-msvc" => Ok("x86_64-pc-windows-msvc"),
        "x86_64-unknown-linux-gnu" => Ok("x86_64-unknown-linux-gnu"),
        _ => Err(ArtifactError::Manifest("unknown runtime target")),
    }
}

fn select_runtime<'a>(
    manifest: &'a ArtifactManifest,
    target: &str,
) -> Result<&'a RuntimeArtifact, ArtifactError> {
    manifest
        .runtime
        .platforms
        .iter()
        .find(|runtime| runtime.target == target)
        .ok_or(ArtifactError::UnsupportedPlatform)
}

fn ensure_root(root: &Path) -> Result<(), ArtifactError> {
    if root.as_os_str().is_empty() {
        return Err(ArtifactError::Manifest("artifact root is empty"));
    }
    if let Ok(metadata) = fs::symlink_metadata(root) {
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ArtifactError::Manifest("artifact root is not a directory"));
        }
    } else {
        fs::create_dir_all(root)?;
    }
    Ok(())
}

fn write_marker(directory: &Path) -> Result<(), ArtifactError> {
    fs::write(directory.join(MANAGED_MARKER), MANAGED_MARKER_VALUE)?;
    Ok(())
}

fn assert_managed_directory(directory: &Path) -> Result<(), ArtifactError> {
    let metadata = fs::symlink_metadata(directory)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ArtifactError::Manifest("managed path is redirected"));
    }
    let marker = fs::read_to_string(directory.join(MANAGED_MARKER))?;
    if marker != MANAGED_MARKER_VALUE {
        return Err(ArtifactError::Manifest(
            "managed directory marker is invalid",
        ));
    }
    Ok(())
}

fn remove_managed_directory_if_present(directory: &Path) -> Result<(), ArtifactError> {
    if !directory.exists() {
        return Ok(());
    }
    assert_managed_directory(directory)?;
    fs::remove_dir_all(directory)?;
    Ok(())
}

fn download_client() -> Result<Client, ArtifactError> {
    Ok(Client::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(3600))
        .redirect(Policy::custom(|attempt| {
            if attempt.previous().len() >= 10 || attempt.url().scheme() != "https" {
                attempt.stop()
            } else {
                attempt.follow()
            }
        }))
        .user_agent("Impossible-Inferences/0.1 artifact-installer")
        .build()?)
}

fn download_verified(
    client: &Client,
    url: &str,
    destination: &Path,
    expected_size: u64,
    expected_sha256: &str,
    kind: &'static str,
) -> Result<(), ArtifactError> {
    validate_https(url)?;
    let mut response = client.get(url).send()?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length != expected_size)
    {
        return Err(ArtifactError::Integrity(kind));
    }
    let temporary = destination.with_extension("part");
    let file = File::create(&temporary)?;
    let mut writer = BufWriter::new(file);
    let mut hasher = Sha256::new();
    let mut received = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = response.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        received = received
            .checked_add(u64::try_from(count).map_err(|_| ArtifactError::Integrity(kind))?)
            .ok_or(ArtifactError::Integrity(kind))?;
        if received > expected_size {
            return Err(ArtifactError::Integrity(kind));
        }
        writer.write_all(&buffer[..count])?;
        hasher.update(&buffer[..count]);
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    let digest = format!("{:x}", hasher.finalize());
    if received != expected_size || digest != expected_sha256 {
        let _ = fs::remove_file(&temporary);
        return Err(ArtifactError::Integrity(kind));
    }
    fs::rename(temporary, destination)?;
    Ok(())
}

fn verify_file(
    path: &Path,
    expected_size: u64,
    expected_sha256: &str,
    verification: Verification,
    kind: &'static str,
) -> Result<(), ArtifactError> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() != expected_size {
        return Err(ArtifactError::Integrity(kind));
    }
    if verification == Verification::Full {
        let mut reader = BufReader::new(File::open(path)?);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            let count = reader.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        if format!("{:x}", hasher.finalize()) != expected_sha256 {
            return Err(ArtifactError::Integrity(kind));
        }
    }
    Ok(())
}

fn verify_current(
    directory: &Path,
    manifest: &ArtifactManifest,
    runtime: &RuntimeArtifact,
    verification: Verification,
) -> Result<(), ArtifactError> {
    assert_managed_directory(directory)?;
    let state = read_and_validate_state(directory, manifest, runtime)?;
    verify_file(
        &directory.join(&state.runtime_archive),
        runtime.size,
        &runtime.sha256,
        verification,
        "runtime",
    )?;
    verify_file(
        &directory.join(&state.model_file),
        manifest.model.size,
        &manifest.model.sha256,
        verification,
        "model",
    )?;
    let executable = directory.join(&state.runtime_executable);
    let executable_metadata = fs::symlink_metadata(&executable)?;
    if executable_metadata.file_type().is_symlink() || !executable_metadata.is_file() {
        return Err(ArtifactError::Integrity("runtime"));
    }
    if verification == Verification::Full {
        verify_runtime_tree(
            &directory.join(&state.runtime_archive),
            &directory.join("runtime"),
            runtime,
        )?;
    }
    Ok(())
}

fn read_and_validate_state(
    directory: &Path,
    manifest: &ArtifactManifest,
    runtime: &RuntimeArtifact,
) -> Result<InstallState, ArtifactError> {
    let state_bytes = fs::read(directory.join(STATE_FILE))?;
    let state: InstallState = serde_json::from_slice(&state_bytes)
        .map_err(|_| ArtifactError::Manifest("install state cannot be decoded"))?;
    if state.schema_version != 1
        || state.product != manifest.product
        || state.profile_id != manifest.profile_id
        || state.target != runtime.target
        || state.runtime_release != manifest.runtime.release_tag
        || state.runtime_sha256 != runtime.sha256
        || state.model_revision != manifest.model.revision
        || state.model_sha256 != manifest.model.sha256
        || state.runtime_archive != format!("downloads/{}", runtime.filename)
        || state.runtime_executable != format!("runtime/{}", runtime.executable)
        || state.model_file != format!("model/{}", manifest.model.filename)
        || safe_relative(Path::new(&state.runtime_archive)).is_none()
        || safe_relative(Path::new(&state.runtime_executable)).is_none()
        || safe_relative(Path::new(&state.model_file)).is_none()
    {
        return Err(ArtifactError::Manifest("install state identity mismatch"));
    }
    Ok(state)
}

fn verify_runtime_tree(
    archive: &Path,
    installed: &Path,
    runtime: &RuntimeArtifact,
) -> Result<(), ArtifactError> {
    let expected = archive_tree_digests(archive, runtime)?;
    if !expected.contains_key(Path::new(&runtime.executable)) {
        return Err(ArtifactError::Archive("runtime executable is missing"));
    }
    let mut actual = BTreeMap::new();
    collect_tree_digests(installed, installed, &mut actual)?;
    if expected != actual {
        return Err(ArtifactError::Integrity("runtime"));
    }
    Ok(())
}

fn archive_tree_digests(
    archive: &Path,
    runtime: &RuntimeArtifact,
) -> Result<BTreeMap<PathBuf, String>, ArtifactError> {
    match runtime.archive_format {
        ArchiveFormat::Zip => zip_tree_digests(archive, &runtime.strip_prefix),
        ArchiveFormat::TarGz => tar_tree_digests(archive, &runtime.strip_prefix),
    }
}

fn zip_tree_digests(
    archive: &Path,
    prefix: &str,
) -> Result<BTreeMap<PathBuf, String>, ArtifactError> {
    let file = File::open(archive)?;
    let mut zip = zip::ZipArchive::new(file).map_err(|_| ArtifactError::Archive("ZIP rejected"))?;
    let mut files = BTreeMap::new();
    for index in 0..zip.len() {
        let mut entry = zip
            .by_index(index)
            .map_err(|_| ArtifactError::Archive("ZIP entry rejected"))?;
        let enclosed = entry
            .enclosed_name()
            .ok_or(ArtifactError::Archive("ZIP path escapes destination"))?;
        let Some(relative) = strip_archive_prefix(&enclosed, prefix)? else {
            continue;
        };
        if entry.is_dir() {
            continue;
        }
        if !entry.is_file() || files.insert(relative, digest_reader(&mut entry)?).is_some() {
            return Err(ArtifactError::Archive("unsupported or duplicate ZIP entry"));
        }
    }
    Ok(files)
}

fn tar_tree_digests(
    archive: &Path,
    prefix: &str,
) -> Result<BTreeMap<PathBuf, String>, ArtifactError> {
    let file = File::open(archive)?;
    let decoder = GzDecoder::new(file);
    let mut tar = tar::Archive::new(decoder);
    let entries = tar
        .entries()
        .map_err(|_| ArtifactError::Archive("tar rejected"))?;
    let mut files = BTreeMap::new();
    let mut links = BTreeMap::<PathBuf, PathBuf>::new();
    for entry in entries {
        let mut entry = entry.map_err(|_| ArtifactError::Archive("tar entry rejected"))?;
        let path = entry
            .path()
            .map_err(|_| ArtifactError::Archive("tar path rejected"))?;
        let Some(relative) = strip_archive_prefix(&path, prefix)? else {
            continue;
        };
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            continue;
        }
        if entry_type.is_file() {
            if files.insert(relative, digest_reader(&mut entry)?).is_some() {
                return Err(ArtifactError::Archive("duplicate tar entry"));
            }
        } else if entry_type.is_symlink() {
            let target = entry
                .link_name()
                .map_err(|_| ArtifactError::Archive("tar link rejected"))?
                .ok_or(ArtifactError::Archive("tar link target missing"))?;
            if target.is_absolute() || links.insert(relative, target.into_owned()).is_some() {
                return Err(ArtifactError::Archive("invalid tar link"));
            }
        } else {
            return Err(ArtifactError::Archive("unsupported tar entry type"));
        }
    }
    for link in links.keys() {
        let source = resolve_link_source(link, &links)?;
        let digest = files
            .get(&source)
            .cloned()
            .ok_or(ArtifactError::Archive("tar link target is not a file"))?;
        if files.insert(link.clone(), digest).is_some() {
            return Err(ArtifactError::Archive("duplicate tar entry"));
        }
    }
    Ok(files)
}

fn collect_tree_digests(
    root: &Path,
    directory: &Path,
    files: &mut BTreeMap<PathBuf, String>,
) -> Result<(), ArtifactError> {
    let metadata = fs::symlink_metadata(directory)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ArtifactError::Integrity("runtime"));
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            return Err(ArtifactError::Integrity("runtime"));
        }
        if metadata.is_dir() {
            collect_tree_digests(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| ArtifactError::Integrity("runtime"))?
                .to_path_buf();
            if files
                .insert(relative, digest_reader(BufReader::new(File::open(path)?))?)
                .is_some()
            {
                return Err(ArtifactError::Integrity("runtime"));
            }
        } else {
            return Err(ArtifactError::Integrity("runtime"));
        }
    }
    Ok(())
}

fn digest_reader(mut reader: impl Read) -> Result<String, ArtifactError> {
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn recover_runtime(
    root: &Path,
    current: &Path,
    manifest: &ArtifactManifest,
    runtime: &RuntimeArtifact,
) -> Result<(), ArtifactError> {
    assert_managed_directory(current)?;
    let state = read_and_validate_state(current, manifest, runtime)?;
    let runtime_archive = current.join(&state.runtime_archive);
    verify_file(
        &runtime_archive,
        runtime.size,
        &runtime.sha256,
        Verification::Full,
        "runtime",
    )?;
    verify_file(
        &current.join(&state.model_file),
        manifest.model.size,
        &manifest.model.sha256,
        Verification::Full,
        "model",
    )?;

    let recovery = root.join(RUNTIME_RECOVERY_DIRECTORY);
    remove_managed_directory_if_present(&recovery)?;
    fs::create_dir(&recovery)?;
    write_marker(&recovery)?;
    let recovered_runtime = recovery.join("runtime");
    fs::create_dir(&recovered_runtime)?;
    extract_runtime(runtime, &runtime_archive, &recovered_runtime)?;
    if !recovered_runtime.join(&runtime.executable).is_file() {
        return Err(ArtifactError::Archive("runtime executable is missing"));
    }

    let installed_runtime = current.join("runtime");
    if installed_runtime.exists() {
        let metadata = fs::symlink_metadata(&installed_runtime)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ArtifactError::Manifest(
                "installed runtime path is redirected",
            ));
        }
        fs::remove_dir_all(&installed_runtime)?;
    }
    fs::rename(&recovered_runtime, &installed_runtime)?;
    remove_managed_directory_if_present(&recovery)?;
    Ok(())
}

fn write_state(directory: &Path, state: &InstallState) -> Result<(), ArtifactError> {
    let mut bytes = serde_json::to_vec_pretty(state)
        .map_err(|_| ArtifactError::Manifest("install state cannot be encoded"))?;
    bytes.push(b'\n');
    let path = directory.join(STATE_FILE);
    let mut file = File::create(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn promote(root: &Path, staging: &Path, current: &Path) -> Result<(), ArtifactError> {
    let previous = root.join(PREVIOUS_DIRECTORY);
    remove_managed_directory_if_present(&previous)?;
    if current.exists() {
        assert_managed_directory(current)?;
        fs::rename(current, &previous)?;
    }
    if let Err(error) = fs::rename(staging, current) {
        if previous.exists() && !current.exists() {
            let _ = fs::rename(&previous, current);
        }
        return Err(error.into());
    }
    remove_managed_directory_if_present(&previous)?;
    Ok(())
}

fn extract_runtime(
    runtime: &RuntimeArtifact,
    archive: &Path,
    destination: &Path,
) -> Result<(), ArtifactError> {
    match runtime.archive_format {
        ArchiveFormat::Zip => extract_zip(archive, destination, &runtime.strip_prefix),
        ArchiveFormat::TarGz => extract_tar_gz(archive, destination, &runtime.strip_prefix),
    }
}

fn extract_zip(archive: &Path, destination: &Path, prefix: &str) -> Result<(), ArtifactError> {
    let file = File::open(archive)?;
    let mut zip = zip::ZipArchive::new(file).map_err(|_| ArtifactError::Archive("ZIP rejected"))?;
    for index in 0..zip.len() {
        let mut entry = zip
            .by_index(index)
            .map_err(|_| ArtifactError::Archive("ZIP entry rejected"))?;
        let enclosed = entry
            .enclosed_name()
            .ok_or(ArtifactError::Archive("ZIP path escapes destination"))?;
        let Some(relative) = strip_archive_prefix(&enclosed, prefix)? else {
            continue;
        };
        let target = destination.join(relative);
        if entry.is_dir() {
            fs::create_dir_all(target)?;
            continue;
        }
        if !entry.is_file() {
            return Err(ArtifactError::Archive("unsupported ZIP entry type"));
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut output = File::create(target)?;
        io::copy(&mut entry, &mut output)?;
    }
    Ok(())
}

fn extract_tar_gz(archive: &Path, destination: &Path, prefix: &str) -> Result<(), ArtifactError> {
    let file = File::open(archive)?;
    let decoder = GzDecoder::new(file);
    let mut tar = tar::Archive::new(decoder);
    let mut links = BTreeMap::<PathBuf, PathBuf>::new();
    let entries = tar
        .entries()
        .map_err(|_| ArtifactError::Archive("tar rejected"))?;
    for entry in entries {
        let mut entry = entry.map_err(|_| ArtifactError::Archive("tar entry rejected"))?;
        let path = entry
            .path()
            .map_err(|_| ArtifactError::Archive("tar path rejected"))?;
        let Some(relative) = strip_archive_prefix(&path, prefix)? else {
            continue;
        };
        let target = destination.join(&relative);
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            fs::create_dir_all(target)?;
        } else if entry_type.is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            entry
                .unpack(&target)
                .map_err(|_| ArtifactError::Archive("tar file could not be unpacked"))?;
        } else if entry_type.is_symlink() {
            let link = entry
                .link_name()
                .map_err(|_| ArtifactError::Archive("tar link rejected"))?
                .ok_or(ArtifactError::Archive("tar link target missing"))?;
            if link.is_absolute() {
                return Err(ArtifactError::Archive("absolute tar link rejected"));
            }
            links.insert(relative, link.into_owned());
        } else {
            return Err(ArtifactError::Archive("unsupported tar entry type"));
        }
    }
    for link in links.keys() {
        let source = resolve_link_source(link, &links)?;
        let source_path = destination.join(source);
        if !source_path.is_file() {
            return Err(ArtifactError::Archive("tar link target is not a file"));
        }
        let target_path = destination.join(link);
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source_path, target_path)?;
    }
    Ok(())
}

fn strip_archive_prefix(path: &Path, prefix: &str) -> Result<Option<PathBuf>, ArtifactError> {
    let safe = safe_relative(path).ok_or(ArtifactError::Archive("unsafe archive path"))?;
    let relative = if prefix.is_empty() {
        safe
    } else {
        match safe.strip_prefix(prefix) {
            Ok(path) if path.as_os_str().is_empty() => return Ok(None),
            Ok(path) => path.to_path_buf(),
            Err(_) => return Err(ArtifactError::Archive("archive prefix mismatch")),
        }
    };
    Ok(Some(relative))
}

fn safe_relative(path: &Path) -> Option<PathBuf> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        return None;
    }
    let mut safe = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => safe.push(value),
            _ => return None,
        }
    }
    (!safe.as_os_str().is_empty()).then_some(safe)
}

fn resolve_link_source(
    link: &Path,
    links: &BTreeMap<PathBuf, PathBuf>,
) -> Result<PathBuf, ArtifactError> {
    let mut current = link.to_path_buf();
    for _ in 0..=links.len() {
        let Some(target) = links.get(&current) else {
            return Ok(current);
        };
        let parent = current.parent().unwrap_or_else(|| Path::new(""));
        current = normalize_join(parent, target)
            .ok_or(ArtifactError::Archive("tar link escapes destination"))?;
    }
    Err(ArtifactError::Archive("tar link cycle rejected"))
}

fn normalize_join(base: &Path, target: &Path) -> Option<PathBuf> {
    if target.is_absolute() {
        return None;
    }
    let mut parts: Vec<_> = base
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_os_string()),
            _ => None,
        })
        .collect();
    for component in target.components() {
        match component {
            Component::Normal(value) => parts.push(value.to_os_string()),
            Component::ParentDir => {
                parts.pop()?;
            }
            Component::CurDir => {}
            _ => return None,
        }
    }
    let mut path = PathBuf::new();
    for part in parts {
        path.push(part);
    }
    (!path.as_os_str().is_empty()).then_some(path)
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use flate2::{Compression, write::GzEncoder};

    use super::{
        ArchiveFormat, ArtifactError, SetupMode, extract_runtime, parse_manifest, safe_relative,
        select_runtime, setup, verify_runtime_tree,
    };

    #[test]
    fn embedded_manifest_is_exact_and_complete() -> Result<(), ArtifactError> {
        let manifest = parse_manifest()?;
        assert_eq!(manifest.model.size, 491_400_032);
        assert_eq!(manifest.runtime.platforms.len(), 2);
        assert!(select_runtime(&manifest, "x86_64-pc-windows-msvc").is_ok());
        assert!(select_runtime(&manifest, "x86_64-unknown-linux-gnu").is_ok());
        Ok(())
    }

    #[test]
    fn unsafe_relative_paths_are_rejected() {
        assert!(safe_relative(std::path::Path::new("../escape")).is_none());
        assert!(safe_relative(std::path::Path::new("/escape")).is_none());
        assert!(safe_relative(std::path::Path::new("runtime/llama-server")).is_some());
    }

    #[test]
    fn zip_extraction_rejects_escape_and_extracts_regular_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = fixture_root("zip")?;
        let archive = root.join("runtime.zip");
        let file = fs::File::create(&archive)?;
        let mut writer = zip::ZipWriter::new(file);
        writer.start_file("llama-server.exe", zip::write::SimpleFileOptions::default())?;
        writer.write_all(b"runtime")?;
        writer.finish()?;
        let destination = root.join("runtime");
        fs::create_dir(&destination)?;
        let runtime = super::RuntimeArtifact {
            target: "fixture".into(),
            archive_format: ArchiveFormat::Zip,
            filename: "runtime.zip".into(),
            url: "https://example.invalid/runtime.zip".into(),
            size: 1,
            sha256: "0".repeat(64),
            strip_prefix: String::new(),
            executable: "llama-server.exe".into(),
        };
        extract_runtime(&runtime, &archive, &destination)?;
        assert_eq!(fs::read(destination.join("llama-server.exe"))?, b"runtime");
        verify_runtime_tree(&archive, &destination, &runtime)?;
        fs::write(destination.join("llama-server.exe"), b"modified")?;
        assert!(matches!(
            verify_runtime_tree(&archive, &destination, &runtime),
            Err(ArtifactError::Integrity("runtime"))
        ));
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn tar_extraction_materializes_only_contained_relative_links()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = fixture_root("tar")?;
        let archive = root.join("runtime.tar.gz");
        let encoder = GzEncoder::new(fs::File::create(&archive)?, Compression::default());
        let mut builder = tar::Builder::new(encoder);
        let bytes = b"runtime";
        let mut file_header = tar::Header::new_gnu();
        file_header.set_size(u64::try_from(bytes.len())?);
        file_header.set_mode(0o755);
        file_header.set_cksum();
        builder.append_data(
            &mut file_header,
            "llama-fixture/libllama.so.1",
            bytes.as_slice(),
        )?;
        let mut link_header = tar::Header::new_gnu();
        link_header.set_entry_type(tar::EntryType::Symlink);
        link_header.set_size(0);
        link_header.set_mode(0o755);
        link_header.set_link_name("libllama.so.1")?;
        link_header.set_cksum();
        builder.append_data(
            &mut link_header,
            "llama-fixture/libllama.so",
            std::io::empty(),
        )?;
        let encoder = builder.into_inner()?;
        encoder.finish()?;
        let destination = root.join("runtime");
        fs::create_dir(&destination)?;
        let runtime = super::RuntimeArtifact {
            target: "fixture".into(),
            archive_format: ArchiveFormat::TarGz,
            filename: "runtime.tar.gz".into(),
            url: "https://example.invalid/runtime.tar.gz".into(),
            size: 1,
            sha256: "0".repeat(64),
            strip_prefix: "llama-fixture".into(),
            executable: "libllama.so".into(),
        };
        extract_runtime(&runtime, &archive, &destination)?;
        assert_eq!(fs::read(destination.join("libllama.so"))?, bytes);
        verify_runtime_tree(&archive, &destination, &runtime)?;
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn offline_setup_never_creates_download_staging() -> Result<(), Box<dyn std::error::Error>> {
        let root = fixture_root("offline")?;
        assert!(matches!(
            setup(&root, SetupMode::Offline),
            Err(ArtifactError::OfflineUnavailable)
        ));
        assert!(!root.join(".staging").exists());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    fn fixture_root(name: &str) -> Result<std::path::PathBuf, std::io::Error> {
        let root = std::env::temp_dir().join(format!(
            "impossible-inferences-artifacts-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir(&root)?;
        Ok(root)
    }
}
