//! SQLite, Git, filesystem, and identifier adapters for Kiln core.

use std::{
    env,
    fs::{self, OpenOptions},
    future::Future,
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use directories::ProjectDirs;
use kiln_core::{
    Approval, ApprovalId, ApprovalPolicy, ApprovalState, Artifact, ContentHash,
    CreateTaskDisposition, CreateTaskMutation, DETERMINISTIC_SUBPROCESS_CAPABILITY,
    DiscoveredWorkspaceRoot, EventCursor, EventId, FilesystemIdentity, Message, MessageId,
    MessageRole, PersistedToolCall, RootDiscoveryError, Run, RunId, RunIdGenerator, RunMutation,
    RunSnapshot, RunState, RunStore, RunStoreError, Session, SessionEvent, SessionEventPage,
    SessionEventPayload, SessionId, SessionIdGenerator, SessionStore, StartRunDisposition,
    StartRunMutation, StoreError, StoredSessionEvent, SubprocessExecution, SubprocessExecutor,
    SubprocessOutput, SubprocessRequest, Task, TaskId, TaskIdGenerator, TaskState, TaskStore,
    TaskStoreError, ToolCall, ToolCallId, ToolCallState, ToolOutputStream, Workspace, WorkspaceId,
    WorkspaceIdGenerator, WorkspacePathScope, WorkspaceRoot, WorkspaceRootDiscovery,
    WorkspaceRootId, WorkspaceRootState, WorkspaceStore,
};
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use tokio::{process::Command, sync::Mutex};
use ulid::Ulid;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};

#[cfg(unix)]
use rustix::{
    fs::{Mode, OFlags, fstat, open, openat},
    io::Errno,
    process::{Pid, Signal, kill_process_group},
};

#[derive(Debug)]
pub enum InfrastructureError {
    DataDirectoryUnavailable,
    Filesystem(std::io::Error),
    Database(sqlx::Error),
    Migration(sqlx::migrate::MigrateError),
}

const AUTH_DIRECTORY: &str = "auth";
const AUTH_FILE: &str = "token";
const AUTH_TOKEN_LENGTH: usize = 80;

pub struct LocalAuthCredential {
    token: [u8; AUTH_TOKEN_LENGTH],
    path: PathBuf,
}

impl LocalAuthCredential {
    pub fn open_default() -> Result<Self, InfrastructureError> {
        let data_directory =
            data_directory().ok_or(InfrastructureError::DataDirectoryUnavailable)?;
        Self::open(data_directory)
    }

    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, InfrastructureError> {
        let data_dir = data_dir.as_ref();
        fs::create_dir_all(data_dir).map_err(InfrastructureError::Filesystem)?;
        let auth_dir = data_dir.join(AUTH_DIRECTORY);
        ensure_private_directory(&auth_dir).map_err(InfrastructureError::Filesystem)?;
        let path = auth_dir.join(AUTH_FILE);
        #[cfg(unix)]
        let token = {
            let auth_dir =
                open_private_directory(&auth_dir).map_err(InfrastructureError::Filesystem)?;
            match read_auth_token_at(&auth_dir) {
                Ok(token) => token,
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    create_auth_token_at(&auth_dir)?
                }
                Err(error) => return Err(InfrastructureError::Filesystem(error)),
            }
        };
        #[cfg(not(unix))]
        let token = match read_auth_token(&path) {
            Ok(token) => token,
            Err(error) if error.kind() == ErrorKind::NotFound => create_auth_token(&path)?,
            Err(error) => return Err(InfrastructureError::Filesystem(error)),
        };
        Ok(Self { token, path })
    }

    pub fn token(&self) -> [u8; AUTH_TOKEN_LENGTH] {
        self.token
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn ensure_private_directory(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_dir() || !is_private(&metadata) {
                return Err(insecure_auth_storage());
            }
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            #[cfg(unix)]
            {
                fs::DirBuilder::new().mode(0o700).create(path)?;
            }
            #[cfg(not(unix))]
            fs::create_dir(path)?;
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_dir() && is_private(&metadata) {
                Ok(())
            } else {
                Err(insecure_auth_storage())
            }
        }
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn open_private_directory(path: &Path) -> io::Result<fs::File> {
    let directory = fs::File::from(
        open(
            path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    let metadata = directory.metadata()?;
    if !metadata.file_type().is_dir() || !is_private(&metadata) {
        return Err(insecure_auth_storage());
    }
    Ok(directory)
}

#[cfg(unix)]
fn read_auth_token_at(auth_dir: &fs::File) -> io::Result<[u8; AUTH_TOKEN_LENGTH]> {
    let file = fs::File::from(
        openat(
            auth_dir,
            AUTH_FILE,
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(io::Error::from)?,
    );
    read_auth_token_file(file)
}

#[cfg(unix)]
fn create_auth_token_at(
    auth_dir: &fs::File,
) -> Result<[u8; AUTH_TOKEN_LENGTH], InfrastructureError> {
    let token = generated_auth_token();
    let file = match openat(
        auth_dir,
        AUTH_FILE,
        OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    ) {
        Ok(file) => fs::File::from(file),
        Err(Errno::EXIST) => {
            return read_auth_token_at(auth_dir).map_err(InfrastructureError::Filesystem);
        }
        Err(error) => return Err(InfrastructureError::Filesystem(io::Error::from(error))),
    };
    write_auth_token(file, token)?;
    Ok(token)
}

#[cfg(not(unix))]
fn read_auth_token(path: &Path) -> io::Result<[u8; AUTH_TOKEN_LENGTH]> {
    let file = OpenOptions::new().read(true).open(path)?;
    read_auth_token_file(file)
}

fn read_auth_token_file(mut file: fs::File) -> io::Result<[u8; AUTH_TOKEN_LENGTH]> {
    let metadata = file.metadata()?;
    if !metadata.file_type().is_file() || !is_private(&metadata) {
        return Err(insecure_auth_storage());
    }
    let mut bytes = Vec::with_capacity(AUTH_TOKEN_LENGTH);
    file.read_to_end(&mut bytes)?;
    let token: [u8; AUTH_TOKEN_LENGTH] = bytes
        .try_into()
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "local auth credential is invalid"))?;
    if !token
        .iter()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "local auth credential is invalid",
        ));
    }
    Ok(token)
}

#[cfg(not(unix))]
fn create_auth_token(path: &Path) -> Result<[u8; AUTH_TOKEN_LENGTH], InfrastructureError> {
    let token = generated_auth_token();
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    match options.open(path) {
        Ok(file) => {
            write_auth_token(file, token)?;
            Ok(token)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            read_auth_token(path).map_err(InfrastructureError::Filesystem)
        }
        Err(error) => Err(InfrastructureError::Filesystem(error)),
    }
}

fn write_auth_token(
    mut file: fs::File,
    token: [u8; AUTH_TOKEN_LENGTH],
) -> Result<(), InfrastructureError> {
    file.write_all(&token)
        .and_then(|()| file.sync_all())
        .map_err(InfrastructureError::Filesystem)
}

fn generated_auth_token() -> [u8; AUTH_TOKEN_LENGTH] {
    let mut token = [0; AUTH_TOKEN_LENGTH];
    for portion in token.as_chunks_mut::<20>().0 {
        let random = Ulid::generate().random().to_be_bytes();
        for (index, byte) in random[6..].iter().enumerate() {
            portion[index * 2] = b"0123456789abcdef"[(byte >> 4) as usize];
            portion[index * 2 + 1] = b"0123456789abcdef"[(byte & 0x0f) as usize];
        }
    }
    token
}

#[cfg(unix)]
fn is_private(metadata: &fs::Metadata) -> bool {
    metadata.permissions().mode() & 0o077 == 0
        && metadata.uid() == rustix::process::geteuid().as_raw()
}

#[cfg(not(unix))]
fn is_private(_metadata: &fs::Metadata) -> bool {
    true
}

fn insecure_auth_storage() -> io::Error {
    io::Error::new(
        ErrorKind::PermissionDenied,
        "local auth credential storage is insecure",
    )
}

const ARTIFACT_DIRECTORY: &str = "artifacts";

#[derive(Debug)]
pub enum ArtifactStoreError {
    Filesystem(io::Error),
    Corrupt,
}

#[derive(Debug, Clone)]
pub struct FileArtifactStore {
    root: Arc<PathBuf>,
}

impl FileArtifactStore {
    pub fn open_default() -> Result<Self, InfrastructureError> {
        let data_directory =
            data_directory().ok_or(InfrastructureError::DataDirectoryUnavailable)?;
        Self::open(data_directory)
    }

    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self, InfrastructureError> {
        let root = data_dir.as_ref().join(ARTIFACT_DIRECTORY);
        ensure_private_directory(&root).map_err(InfrastructureError::Filesystem)?;
        Ok(Self {
            root: Arc::new(root),
        })
    }

    pub fn store(&self, bytes: &[u8], media_type: &str) -> Result<Artifact, ArtifactStoreError> {
        let content_hash = hash_bytes(bytes);
        let artifact = Artifact::new(
            content_hash.clone(),
            media_type,
            u64::try_from(bytes.len()).map_err(|_| ArtifactStoreError::Corrupt)?,
        )
        .map_err(|_| ArtifactStoreError::Corrupt)?;
        let directory = self.bucket(&content_hash);
        ensure_private_directory(&directory).map_err(ArtifactStoreError::Filesystem)?;
        let path = directory.join(content_hash.as_str());
        if path.exists() {
            verify_artifact_file(&path, &artifact)?;
            return Ok(artifact);
        }

        let temporary = directory.join(format!(
            ".{}.{}.tmp",
            content_hash.as_str(),
            Ulid::generate()
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .map_err(ArtifactStoreError::Filesystem)?;
        if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(ArtifactStoreError::Filesystem(error));
        }
        drop(file);
        match fs::hard_link(&temporary, &path) {
            Ok(()) => {
                fs::remove_file(&temporary).map_err(ArtifactStoreError::Filesystem)?;
                #[cfg(unix)]
                fs::File::open(&directory)
                    .and_then(|directory| directory.sync_all())
                    .map_err(ArtifactStoreError::Filesystem)?;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary).map_err(ArtifactStoreError::Filesystem)?;
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(ArtifactStoreError::Filesystem(error));
            }
        }
        verify_artifact_file(&path, &artifact)?;
        Ok(artifact)
    }

    pub fn read(&self, content_hash: &ContentHash) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        let path = self.bucket(content_hash).join(content_hash.as_str());
        let mut file = match open_artifact_file(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ArtifactStoreError::Filesystem(error)),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(ArtifactStoreError::Filesystem)?;
        if hash_bytes(&bytes) != *content_hash {
            return Err(ArtifactStoreError::Corrupt);
        }
        Ok(Some(bytes))
    }

    fn bucket(&self, content_hash: &ContentHash) -> PathBuf {
        self.root.join(&content_hash.as_str()[..2])
    }
}

fn hash_bytes(bytes: &[u8]) -> ContentHash {
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
    }
    ContentHash::parse(encoded).expect("SHA-256 is a lowercase 64-character hexadecimal value")
}

fn verify_artifact_file(path: &Path, artifact: &Artifact) -> Result<(), ArtifactStoreError> {
    let mut file = open_artifact_file(path).map_err(ArtifactStoreError::Filesystem)?;
    let metadata = file.metadata().map_err(ArtifactStoreError::Filesystem)?;
    if !metadata.file_type().is_file() || metadata.len() != artifact.size() {
        return Err(ArtifactStoreError::Corrupt);
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(ArtifactStoreError::Filesystem)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let digest = hasher.finalize();
    if digest.as_slice()
        != artifact_hash_bytes(artifact.content_hash()).ok_or(ArtifactStoreError::Corrupt)?
    {
        return Err(ArtifactStoreError::Corrupt);
    }
    Ok(())
}

fn artifact_hash_bytes(content_hash: &ContentHash) -> Option<[u8; 32]> {
    let mut bytes = [0_u8; 32];
    for (index, pair) in content_hash
        .as_str()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
    {
        bytes[index] = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Some(bytes)
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[cfg(unix)]
fn open_artifact_file(path: &Path) -> io::Result<fs::File> {
    open(
        path,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map(fs::File::from)
    .map_err(io::Error::from)
}

#[cfg(not(unix))]
fn open_artifact_file(path: &Path) -> io::Result<fs::File> {
    OpenOptions::new().read(true).open(path)
}

#[derive(Clone)]
pub struct SqliteStore {
    connection: Arc<Mutex<SqliteConnection>>,
}

pub const DETERMINISTIC_SUBPROCESS_ARGUMENT: &str = "--kiln-deterministic-subprocess";
pub const DETERMINISTIC_SUCCESS_ARGUMENT: &str = "success";
pub const DETERMINISTIC_FAILURE_ARGUMENT: &str = "failure";
pub const DETERMINISTIC_BLOCKING_TREE_ARGUMENT: &str = "blocking-tree";
pub const DETERMINISTIC_LARGE_OUTPUT_ARGUMENT: &str = "large-output";
pub const KILN_DETERMINISTIC_PID_FILE: &str = "KILN_DETERMINISTIC_PID_FILE";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeterministicOutcome {
    Success,
    Failure,
    BlockingTree,
    LargeOutput,
}

impl DeterministicOutcome {
    pub const fn argument(self) -> &'static str {
        match self {
            Self::Success => DETERMINISTIC_SUCCESS_ARGUMENT,
            Self::Failure => DETERMINISTIC_FAILURE_ARGUMENT,
            Self::BlockingTree => DETERMINISTIC_BLOCKING_TREE_ARGUMENT,
            Self::LargeOutput => DETERMINISTIC_LARGE_OUTPUT_ARGUMENT,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DeterministicSubprocessExecutor {
    outcome: DeterministicOutcome,
}

impl DeterministicSubprocessExecutor {
    pub const fn new(outcome: DeterministicOutcome) -> Self {
        Self { outcome }
    }
}

impl SubprocessExecutor for DeterministicSubprocessExecutor {
    async fn execute<C>(&self, request: SubprocessRequest, cancellation: C) -> SubprocessExecution
    where
        C: Future<Output = ()> + Send,
    {
        if validate_subprocess_request(&request).is_err() {
            return SubprocessExecution::Finished(SubprocessOutput::spawn_failure(
                "path is outside workspace root",
            ));
        }
        let executable = match env::current_exe() {
            Ok(path) => path,
            Err(_) => {
                return SubprocessExecution::Finished(SubprocessOutput::spawn_failure(
                    "deterministic subprocess unavailable",
                ));
            }
        };

        let pid_file = env::var_os(KILN_DETERMINISTIC_PID_FILE);
        let mut command = Command::new(executable);
        command
            .arg(DETERMINISTIC_SUBPROCESS_ARGUMENT)
            .arg(self.outcome.argument())
            .env_clear()
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        let directory_fd = match pin_subprocess_directory(&request) {
            Ok(fd) => fd,
            Err(_) => {
                return SubprocessExecution::Finished(SubprocessOutput::spawn_failure(
                    "path is outside workspace root",
                ));
            }
        };
        #[cfg(unix)]
        unsafe {
            command.pre_exec(move || {
                rustix::process::fchdir(&directory_fd).map_err(std::io::Error::from)
            });
        }
        #[cfg(not(unix))]
        if let Ok(working_directory) = canonical_subprocess_directory(&request) {
            command.current_dir(working_directory);
        } else {
            return SubprocessExecution::Finished(SubprocessOutput::spawn_failure(
                "path is outside workspace root",
            ));
        }
        if let Some(pid_file) = pid_file {
            command.env(KILN_DETERMINISTIC_PID_FILE, pid_file);
        }
        #[cfg(unix)]
        command.process_group(0);

        let child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                return SubprocessExecution::Finished(SubprocessOutput::spawn_failure(
                    "deterministic subprocess failed to start",
                ));
            }
        };
        let pid = child.id();
        let mut process_group = pid.and_then(ProcessGroupGuard::new);
        let output = child.wait_with_output();
        tokio::pin!(output);
        tokio::pin!(cancellation);
        tokio::select! {
            result = &mut output => match result {
                Ok(output) => {
                    if let Some(group) = process_group.as_mut()
                        && group.stop_remaining_members().is_err()
                    {
                        return SubprocessExecution::CancellationFailed;
                    }
                    SubprocessExecution::Finished(subprocess_output(output))
                }
                Err(_) => SubprocessExecution::CancellationFailed,
            },
            _ = &mut cancellation => {
                let Some(group) = process_group.as_mut() else {
                    return SubprocessExecution::CancellationFailed;
                };
                if group.kill().is_err() {
                    return SubprocessExecution::CancellationFailed;
                }
                let output = match output.await {
                    Ok(output) => output,
                    Err(_) => return SubprocessExecution::CancellationFailed,
                };
                // The signal was sent while the group leader was still alive. A later
                // numeric process-group probe could target a reused ID after this wait.
                group.disarm();
                SubprocessExecution::Cancelled(subprocess_output(output))
            }
        }
    }
}

pub fn validate_subprocess_request(request: &SubprocessRequest) -> Result<(), kiln_core::RunError> {
    #[cfg(unix)]
    {
        drop(pin_subprocess_directory(request)?);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        canonical_subprocess_directory(request).map(drop)
    }
}

fn canonical_subprocess_target(
    root: &Path,
    request: &SubprocessRequest,
) -> Result<PathBuf, kiln_core::RunError> {
    let relative = request.scope().relative_directory();
    let target = std::fs::canonicalize(root.join(relative))
        .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    if !target.starts_with(root) || !target.is_dir() {
        return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
    }
    Ok(target)
}

#[cfg(not(unix))]
fn canonical_subprocess_directory(
    request: &SubprocessRequest,
) -> Result<PathBuf, kiln_core::RunError> {
    let stored_root = Path::new(request.workspace_root_path());
    let root = std::fs::canonicalize(stored_root)
        .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    let identity = workspace_root_filesystem_identity(&root)
        .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    if root != stored_root || &identity != request.workspace_root_filesystem_identity() {
        return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
    }
    canonical_subprocess_target(&root, request)
}

#[cfg(unix)]
fn pin_subprocess_directory(
    request: &SubprocessRequest,
) -> Result<rustix::fd::OwnedFd, kiln_core::RunError> {
    let root = Path::new(request.workspace_root_path());
    if !root.is_absolute() {
        return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
    }
    let flags = OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let root_fd = open(root, flags, Mode::empty())
        .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    let root_stat = fstat(&root_fd).map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    let root_identity =
        FilesystemIdentity::new(format!("unix:{}:{}", root_stat.st_dev, root_stat.st_ino))
            .expect("filesystem identity format is valid");
    if &root_identity != request.workspace_root_filesystem_identity() {
        return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
    }
    let root_device = root_stat.st_dev;
    let target = canonical_subprocess_target(root, request)?;
    let mut current = root_fd;
    let relative = target
        .strip_prefix(root)
        .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
    for component in relative.components() {
        let std::path::Component::Normal(component) = component else {
            return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
        };
        let next = openat(&current, component, flags, Mode::empty())
            .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?;
        let device = fstat(&next)
            .map_err(|_| kiln_core::RunError::PathOutsideWorkspaceRoot)?
            .st_dev;
        if device != root_device {
            return Err(kiln_core::RunError::PathOutsideWorkspaceRoot);
        }
        current = next;
    }
    Ok(current)
}

fn workspace_root_filesystem_identity(path: &Path) -> io::Result<FilesystemIdentity> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "workspace root is not a directory",
        ));
    }
    #[cfg(unix)]
    let value = format!("unix:{}:{}", metadata.dev(), metadata.ino());
    #[cfg(not(unix))]
    let value = format!("path:{}", std::fs::canonicalize(path)?.display());
    FilesystemIdentity::new(value).ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            "workspace root filesystem identity is invalid",
        )
    })
}

fn subprocess_output(output: std::process::Output) -> SubprocessOutput {
    SubprocessOutput {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        stdout_artifact: None,
        stderr_artifact: None,
        exit_code: output.status.code(),
        spawn_error: None,
    }
}

#[cfg(unix)]
struct ProcessGroupGuard {
    pid: Option<Pid>,
}

#[cfg(unix)]
impl ProcessGroupGuard {
    fn new(pid: u32) -> Option<Self> {
        let pid = i32::try_from(pid).ok().and_then(Pid::from_raw)?;
        Some(Self { pid: Some(pid) })
    }

    fn kill(&self) -> Result<(), Errno> {
        let pid = self.pid.ok_or(Errno::INVAL)?;
        loop {
            match kill_process_group(pid, Signal::KILL) {
                Ok(()) | Err(Errno::SRCH) => return Ok(()),
                Err(Errno::INTR) => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn stop_remaining_members(&mut self) -> Result<(), Errno> {
        self.kill()?;
        self.disarm();
        Ok(())
    }

    fn disarm(&mut self) {
        self.pid = None;
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        let _ = self.kill();
    }
}

#[cfg(not(unix))]
struct ProcessGroupGuard;

#[cfg(not(unix))]
impl ProcessGroupGuard {
    fn new(_pid: u32) -> Option<Self> {
        None
    }
}

impl SqliteStore {
    pub async fn open_default() -> Result<Self, InfrastructureError> {
        let data_directory =
            data_directory().ok_or(InfrastructureError::DataDirectoryUnavailable)?;
        Self::open(data_directory).await
    }

    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self, InfrastructureError> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(InfrastructureError::Filesystem)?;
        let database_path = data_dir.join("kiln.sqlite3");
        let options = SqliteConnectOptions::new()
            .filename(database_path)
            .create_if_missing(true)
            .foreign_keys(true);
        let mut connection = SqliteConnection::connect_with(&options)
            .await
            .map_err(InfrastructureError::Database)?;
        sqlx::migrate!("./migrations")
            .run(&mut connection)
            .await
            .map_err(InfrastructureError::Migration)?;
        backfill_workspace_root_filesystem_identities(&mut connection)
            .await
            .map_err(InfrastructureError::Database)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub async fn get_artifact_metadata(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<Artifact>, StoreError> {
        let mut connection = self.connection.lock().await;
        let row = sqlx::query(
            "SELECT content_hash, media_type, size FROM artifacts WHERE content_hash = ?",
        )
        .bind(content_hash.as_str())
        .fetch_optional(&mut *connection)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        row.map(|row| parse_artifact(&row)).transpose()
    }
}

fn parse_artifact(row: &sqlx::sqlite::SqliteRow) -> Result<Artifact, StoreError> {
    let content_hash = ContentHash::parse(
        row.try_get::<String, _>("content_hash")
            .map_err(|_| StoreError::Unavailable)?,
    )
    .map_err(|_| StoreError::Unavailable)?;
    let media_type = row
        .try_get::<String, _>("media_type")
        .map_err(|_| StoreError::Unavailable)?;
    let size = row
        .try_get::<i64, _>("size")
        .map_err(|_| StoreError::Unavailable)?;
    Artifact::new(
        content_hash,
        media_type,
        u64::try_from(size).map_err(|_| StoreError::Unavailable)?,
    )
    .map_err(|_| StoreError::Unavailable)
}

fn parse_optional_artifact(
    row: &sqlx::sqlite::SqliteRow,
    hash_column: &str,
    media_type_column: &str,
    size_column: &str,
) -> Result<Option<Artifact>, StoreError> {
    let content_hash = row
        .try_get::<Option<String>, _>(hash_column)
        .map_err(|_| StoreError::Unavailable)?;
    let media_type = row
        .try_get::<Option<String>, _>(media_type_column)
        .map_err(|_| StoreError::Unavailable)?;
    let size = row
        .try_get::<Option<i64>, _>(size_column)
        .map_err(|_| StoreError::Unavailable)?;
    match (content_hash, media_type, size) {
        (None, None, None) => Ok(None),
        (Some(content_hash), Some(media_type), Some(size)) => Artifact::new(
            ContentHash::parse(content_hash).map_err(|_| StoreError::Unavailable)?,
            media_type,
            u64::try_from(size).map_err(|_| StoreError::Unavailable)?,
        )
        .map(Some)
        .map_err(|_| StoreError::Unavailable),
        _ => Err(StoreError::Unavailable),
    }
}

async fn backfill_workspace_root_filesystem_identities(
    connection: &mut SqliteConnection,
) -> Result<(), sqlx::Error> {
    let roots = sqlx::query(
        "SELECT workspace_root_id, canonical_path FROM workspace_roots WHERE filesystem_identity IS NULL",
    )
    .fetch_all(&mut *connection)
    .await?;
    for root in roots {
        let id: String = root.try_get("workspace_root_id")?;
        let path: String = root.try_get("canonical_path")?;
        // ponytail: Pre-identity databases need one trust-on-first-use read. Missing or
        // replaced paths stay fail-closed and must be registered again.
        let Ok(identity) = workspace_root_filesystem_identity(Path::new(&path)) else {
            continue;
        };
        sqlx::query(
            "UPDATE workspace_roots SET filesystem_identity = ? WHERE workspace_root_id = ? AND filesystem_identity IS NULL",
        )
        .bind(identity.as_str())
        .bind(id)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

impl WorkspaceStore for SqliteStore {
    async fn create_workspace(&self, workspace: &Workspace) -> Result<(), StoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("INSERT INTO workspaces (workspace_id, name) VALUES (?, ?)")
            .bind(workspace.id().as_str())
            .bind(workspace.name())
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        for root in workspace.roots() {
            sqlx::query("INSERT INTO workspace_roots (workspace_root_id, workspace_id, name, display_path, canonical_path, git_common_directory_path, filesystem_identity, position, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .bind(root.id().as_str())
                .bind(workspace.id().as_str())
                .bind(root.name())
                .bind(root.display_path())
                .bind(root.canonical_path())
                .bind(root.git_common_directory_path())
                .bind(root.filesystem_identity().as_str())
                .bind(i64::try_from(root.position()).map_err(|_| StoreError::Unavailable)?)
                .bind(root.state().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        }
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn get_workspace(&self, id: &WorkspaceId) -> Result<Option<Workspace>, StoreError> {
        let mut connection = self.connection.lock().await;
        let workspace =
            sqlx::query("SELECT workspace_id, name FROM workspaces WHERE workspace_id = ?")
                .bind(id.as_str())
                .fetch_optional(&mut *connection)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        let Some(workspace) = workspace else {
            return Ok(None);
        };
        let roots = sqlx::query("SELECT workspace_root_id, name, display_path, canonical_path, git_common_directory_path, filesystem_identity, position, state FROM workspace_roots WHERE workspace_id = ? ORDER BY position")
            .bind(id.as_str())
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut domain_roots = Vec::with_capacity(roots.len());
        for (expected_position, row) in roots.into_iter().enumerate() {
            let position: i64 = row
                .try_get("position")
                .map_err(|_| StoreError::Unavailable)?;
            let state: String = row.try_get("state").map_err(|_| StoreError::Unavailable)?;
            if position < 0 || state != WorkspaceRootState::Available.as_str() {
                return Err(StoreError::Unavailable);
            }
            let position = usize::try_from(position).map_err(|_| StoreError::Unavailable)?;
            if position != expected_position {
                return Err(StoreError::Unavailable);
            }
            domain_roots.push(
                WorkspaceRoot::new(
                    WorkspaceRootId::parse(
                        row.try_get::<String, _>("workspace_root_id")
                            .map_err(|_| StoreError::Unavailable)?,
                    )
                    .map_err(|_| StoreError::Unavailable)?,
                    row.try_get("name").map_err(|_| StoreError::Unavailable)?,
                    row.try_get("display_path")
                        .map_err(|_| StoreError::Unavailable)?,
                    DiscoveredWorkspaceRoot {
                        canonical_path: row
                            .try_get("canonical_path")
                            .map_err(|_| StoreError::Unavailable)?,
                        git_common_directory_path: row
                            .try_get("git_common_directory_path")
                            .map_err(|_| StoreError::Unavailable)?,
                        filesystem_identity: FilesystemIdentity::new(
                            row.try_get::<String, _>("filesystem_identity")
                                .map_err(|_| StoreError::Unavailable)?,
                        )
                        .ok_or(StoreError::Unavailable)?,
                    },
                    position,
                    WorkspaceRootState::Available,
                )
                .map_err(|_| StoreError::Unavailable)?,
            );
        }
        Workspace::new(
            WorkspaceId::parse(
                workspace
                    .try_get::<String, _>("workspace_id")
                    .map_err(|_| StoreError::Unavailable)?,
            )
            .map_err(|_| StoreError::Unavailable)?,
            workspace
                .try_get("name")
                .map_err(|_| StoreError::Unavailable)?,
            domain_roots,
        )
        .map(Some)
        .map_err(|_| StoreError::Unavailable)
    }
}

async fn load_task(
    connection: &mut SqliteConnection,
    task_id: &TaskId,
) -> Result<Option<Task>, StoreError> {
    let row = sqlx::query(
        "SELECT task_id, session_id, objective, state, parent_task_id, assigned_run_id
         FROM tasks WHERE task_id = ?",
    )
    .bind(task_id.as_str())
    .fetch_optional(&mut *connection)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let dependency_rows = sqlx::query(
        "SELECT dependency_task_id FROM task_dependencies
         WHERE task_id = ? ORDER BY position ASC",
    )
    .bind(task_id.as_str())
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    let dependency_task_ids = dependency_rows
        .into_iter()
        .map(|row| {
            TaskId::parse(
                row.try_get::<String, _>("dependency_task_id")
                    .map_err(|_| StoreError::Unavailable)?,
            )
            .map_err(|_| StoreError::Unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Task::from_persisted(
        TaskId::parse(
            row.try_get::<String, _>("task_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?,
        SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?,
        row.try_get("objective")
            .map_err(|_| StoreError::Unavailable)?,
        TaskState::parse(
            &row.try_get::<String, _>("state")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?,
        row.try_get::<Option<String>, _>("parent_task_id")
            .map_err(|_| StoreError::Unavailable)?
            .map(TaskId::parse)
            .transpose()
            .map_err(|_| StoreError::Unavailable)?,
        dependency_task_ids,
        row.try_get::<Option<String>, _>("assigned_run_id")
            .map_err(|_| StoreError::Unavailable)?
            .map(RunId::parse)
            .transpose()
            .map_err(|_| StoreError::Unavailable)?,
    )
    .map(Some)
    .map_err(|_| StoreError::Unavailable)
}

impl SessionStore for SqliteStore {
    async fn create_session(
        &self,
        session: &Session,
        event: &SessionEvent,
    ) -> Result<(), StoreError> {
        let SessionEventPayload::SessionCreated { workspace_id } = event.payload() else {
            return Err(StoreError::Unavailable);
        };
        if event.session_id() != session.id() || workspace_id != session.workspace_id() {
            return Err(StoreError::Unavailable);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("INSERT INTO sessions (session_id, workspace_id) VALUES (?, ?)")
            .bind(session.id().as_str())
            .bind(session.workspace_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type, message_id) VALUES (?, ?, 'session.created', NULL)",
        )
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        let mut connection = self.connection.lock().await;
        let row = sqlx::query("SELECT session_id, workspace_id FROM sessions WHERE session_id = ?")
            .bind(id.as_str())
            .fetch_optional(&mut *connection)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let workspace_id = WorkspaceId::parse(
            row.try_get::<String, _>("workspace_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        Ok(Some(Session::new(session_id, workspace_id)))
    }

    async fn append_message(
        &self,
        message: &Message,
        event: &SessionEvent,
    ) -> Result<(), StoreError> {
        let SessionEventPayload::MessageAppended {
            message: event_message,
        } = event.payload()
        else {
            return Err(StoreError::Unavailable);
        };
        if event.session_id() != message.session_id() || event_message != message {
            return Err(StoreError::Unavailable);
        }

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO messages (message_id, session_id, role, content) VALUES (?, ?, ?, ?)",
        )
        .bind(message.id().as_str())
        .bind(message.session_id().as_str())
        .bind(message.role().as_str())
        .bind(message.content())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO session_events (event_id, session_id, event_type, message_id) VALUES (?, ?, 'message.appended', ?)",
        )
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .bind(message.id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    async fn list_session_events(
        &self,
        session_id: &SessionId,
        after: EventCursor,
    ) -> Result<SessionEventPage, StoreError> {
        self.list_events(after, Some(session_id)).await
    }

    async fn list_events_after(&self, after: EventCursor) -> Result<SessionEventPage, StoreError> {
        self.list_events(after, None).await
    }

    async fn current_event_cursor(&self) -> Result<Option<EventCursor>, StoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let cursor = current_cursor(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if cursor == EventCursor::zero() {
            Ok(None)
        } else {
            Ok(Some(cursor))
        }
    }
}

impl SqliteStore {
    async fn list_events(
        &self,
        after: EventCursor,
        session_id: Option<&SessionId>,
    ) -> Result<SessionEventPage, StoreError> {
        let after = i64::try_from(after.value()).unwrap_or(i64::MAX);
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let rows = match session_id {
            Some(session_id) => {
                sqlx::query(
                    "SELECT e.event_id, e.session_id, e.cursor, e.event_type, e.message_id,
                        e.task_id, e.task_objective, e.task_state, e.parent_task_id,
                        e.dependency_task_ids, e.assigned_run_id,
                        e.run_id, e.tool_call_id, e.run_state, e.tool_call_state,
                        e.approval_id, e.approval_state, e.approval_policy,
                        e.requested_workspace_root_id, e.requested_relative_directory,
                        e.effective_workspace_root_id, e.effective_relative_directory,
                        e.capability, e.stdout, e.stderr, e.exit_code,
                        e.output_stream, e.output_content, e.artifact_hash,
                        e.stdout_artifact_hash, e.stderr_artifact_hash,
                        a.media_type AS artifact_media_type, a.size AS artifact_size,
                        osa.media_type AS stdout_artifact_media_type,
                        osa.size AS stdout_artifact_size,
                        esa.media_type AS stderr_artifact_media_type,
                        esa.size AS stderr_artifact_size,
                        m.message_id AS loaded_message_id, m.session_id AS message_session_id,
                        m.role, m.content, s.workspace_id
                 FROM session_events e
                 JOIN sessions s ON s.session_id = e.session_id
                 LEFT JOIN messages m ON m.message_id = e.message_id
                 LEFT JOIN artifacts a ON a.content_hash = e.artifact_hash
                 LEFT JOIN artifacts osa ON osa.content_hash = e.stdout_artifact_hash
                 LEFT JOIN artifacts esa ON esa.content_hash = e.stderr_artifact_hash
                 WHERE e.session_id = ? AND e.cursor > ?
                 ORDER BY e.cursor ASC",
                )
                .bind(session_id.as_str())
                .bind(after)
                .fetch_all(&mut *transaction)
                .await
            }
            None => {
                sqlx::query(
                    "SELECT e.event_id, e.session_id, e.cursor, e.event_type, e.message_id,
                        e.task_id, e.task_objective, e.task_state, e.parent_task_id,
                        e.dependency_task_ids, e.assigned_run_id,
                        e.run_id, e.tool_call_id, e.run_state, e.tool_call_state,
                        e.approval_id, e.approval_state, e.approval_policy,
                        e.requested_workspace_root_id, e.requested_relative_directory,
                        e.effective_workspace_root_id, e.effective_relative_directory,
                        e.capability, e.stdout, e.stderr, e.exit_code,
                        e.output_stream, e.output_content, e.artifact_hash,
                        e.stdout_artifact_hash, e.stderr_artifact_hash,
                        a.media_type AS artifact_media_type, a.size AS artifact_size,
                        osa.media_type AS stdout_artifact_media_type,
                        osa.size AS stdout_artifact_size,
                        esa.media_type AS stderr_artifact_media_type,
                        esa.size AS stderr_artifact_size,
                        m.message_id AS loaded_message_id, m.session_id AS message_session_id,
                        m.role, m.content, s.workspace_id
                 FROM session_events e
                 JOIN sessions s ON s.session_id = e.session_id
                 LEFT JOIN messages m ON m.message_id = e.message_id
                 LEFT JOIN artifacts a ON a.content_hash = e.artifact_hash
                 LEFT JOIN artifacts osa ON osa.content_hash = e.stdout_artifact_hash
                 LEFT JOIN artifacts esa ON esa.content_hash = e.stderr_artifact_hash
                 WHERE e.cursor > ?
                 ORDER BY e.cursor ASC",
                )
                .bind(after)
                .fetch_all(&mut *transaction)
                .await
            }
        }
        .map_err(|_| StoreError::Unavailable)?;
        let events = parse_event_rows(rows)?;
        let current_cursor = current_cursor(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(SessionEventPage::new(events, current_cursor))
    }
}

impl TaskStore for SqliteStore {
    async fn create_task(
        &self,
        task: &Task,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> Result<CreateTaskMutation, TaskStoreError> {
        if idempotency_key.is_empty() {
            return Err(TaskStoreError::IdempotencyKeyRequired);
        }
        let SessionEventPayload::TaskCreated { task: event_task } = event.payload() else {
            return Err(TaskStoreError::InvalidTask);
        };
        if event.session_id() != task.session_id()
            || event_task != task
            || task.state() != TaskState::Pending
            || task.assigned_run_id().is_some()
        {
            return Err(TaskStoreError::InvalidTask);
        }
        let dependency_task_ids = serde_json::to_string(
            &task
                .dependency_task_ids()
                .iter()
                .map(TaskId::as_str)
                .collect::<Vec<_>>(),
        )
        .map_err(|_| TaskStoreError::InvalidTask)?;

        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| TaskStoreError::Unavailable)?;
        let existing = sqlx::query(
            "SELECT task_id, objective, parent_task_id, dependency_task_ids
             FROM create_task_idempotencies
             WHERE session_id = ? AND idempotency_key = ?",
        )
        .bind(task.session_id().as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| TaskStoreError::Unavailable)?;
        if let Some(existing) = existing {
            let existing_task_id = existing
                .try_get::<String, _>("task_id")
                .map_err(|_| TaskStoreError::Unavailable)?;
            let same_request = existing
                .try_get::<String, _>("objective")
                .map_err(|_| TaskStoreError::Unavailable)?
                == task.objective()
                && existing
                    .try_get::<Option<String>, _>("parent_task_id")
                    .map_err(|_| TaskStoreError::Unavailable)?
                    .as_deref()
                    == task.parent_task_id().map(TaskId::as_str)
                && existing
                    .try_get::<String, _>("dependency_task_ids")
                    .map_err(|_| TaskStoreError::Unavailable)?
                    == dependency_task_ids;
            if !same_request {
                return Err(TaskStoreError::IdempotencyConflict);
            }
            let existing_task_id =
                TaskId::parse(existing_task_id).map_err(|_| TaskStoreError::Unavailable)?;
            let existing_task = load_task(&mut transaction, &existing_task_id)
                .await
                .map_err(|_| TaskStoreError::Unavailable)?
                .ok_or(TaskStoreError::Unavailable)?;
            transaction
                .commit()
                .await
                .map_err(|_| TaskStoreError::Unavailable)?;
            return Ok(CreateTaskMutation::new(
                existing_task,
                Vec::new(),
                CreateTaskDisposition::Duplicate,
            ));
        }

        if let Some(parent_task_id) = task.parent_task_id() {
            let parent_session_id =
                sqlx::query_scalar::<_, String>("SELECT session_id FROM tasks WHERE task_id = ?")
                    .bind(parent_task_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| TaskStoreError::Unavailable)?
                    .ok_or(TaskStoreError::ParentTaskNotFound)?;
            if parent_session_id != task.session_id().as_str() {
                return Err(TaskStoreError::TaskLinkOutsideSession);
            }
        }
        for dependency_task_id in task.dependency_task_ids() {
            let dependency_session_id =
                sqlx::query_scalar::<_, String>("SELECT session_id FROM tasks WHERE task_id = ?")
                    .bind(dependency_task_id.as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| TaskStoreError::Unavailable)?
                    .ok_or(TaskStoreError::DependencyTaskNotFound)?;
            if dependency_session_id != task.session_id().as_str() {
                return Err(TaskStoreError::TaskLinkOutsideSession);
            }
        }

        sqlx::query(
            "INSERT INTO tasks
                (task_id, session_id, objective, state, parent_task_id, assigned_run_id)
             VALUES (?, ?, ?, ?, ?, NULL)",
        )
        .bind(task.task_id().as_str())
        .bind(task.session_id().as_str())
        .bind(task.objective())
        .bind(task.state().as_str())
        .bind(task.parent_task_id().map(TaskId::as_str))
        .execute(&mut *transaction)
        .await
        .map_err(|_| TaskStoreError::Unavailable)?;
        for (position, dependency_task_id) in task.dependency_task_ids().iter().enumerate() {
            sqlx::query(
                "INSERT INTO task_dependencies (task_id, dependency_task_id, position)
                 VALUES (?, ?, ?)",
            )
            .bind(task.task_id().as_str())
            .bind(dependency_task_id.as_str())
            .bind(i64::try_from(position).map_err(|_| TaskStoreError::Unavailable)?)
            .execute(&mut *transaction)
            .await
            .map_err(|_| TaskStoreError::Unavailable)?;
        }
        sqlx::query(
            "INSERT INTO create_task_idempotencies
                (session_id, idempotency_key, task_id, objective, parent_task_id, dependency_task_ids)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(task.session_id().as_str())
        .bind(idempotency_key)
        .bind(task.task_id().as_str())
        .bind(task.objective())
        .bind(task.parent_task_id().map(TaskId::as_str))
        .bind(&dependency_task_ids)
        .execute(&mut *transaction)
        .await
        .map_err(|_| TaskStoreError::Unavailable)?;
        let result = sqlx::query(
            "INSERT INTO session_events (
                event_id, session_id, event_type, task_id, task_objective, task_state,
                parent_task_id, dependency_task_ids, assigned_run_id
             ) VALUES (?, ?, 'task.created', ?, ?, ?, ?, ?, NULL)",
        )
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .bind(task.task_id().as_str())
        .bind(task.objective())
        .bind(task.state().as_str())
        .bind(task.parent_task_id().map(TaskId::as_str))
        .bind(&dependency_task_ids)
        .execute(&mut *transaction)
        .await
        .map_err(|_| TaskStoreError::Unavailable)?;
        let stored_event = StoredSessionEvent::from_event(
            event,
            committed_cursor(result.last_insert_rowid())
                .map_err(|_| TaskStoreError::Unavailable)?,
        )
        .map_err(|_| TaskStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| TaskStoreError::Unavailable)?;
        Ok(CreateTaskMutation::new(
            task.clone(),
            vec![stored_event],
            CreateTaskDisposition::Created,
        ))
    }

    async fn get_task(&self, task_id: &TaskId) -> Result<Option<Task>, StoreError> {
        let mut connection = self.connection.lock().await;
        load_task(&mut connection, task_id).await
    }
}

fn parse_event_rows(
    rows: Vec<sqlx::sqlite::SqliteRow>,
) -> Result<Vec<StoredSessionEvent>, StoreError> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let event_id = EventId::parse(
            row.try_get::<String, _>("event_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let stored_session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;
        let cursor = committed_cursor(
            row.try_get::<i64, _>("cursor")
                .map_err(|_| StoreError::Unavailable)?,
        )?;
        let event_type: String = row
            .try_get("event_type")
            .map_err(|_| StoreError::Unavailable)?;
        let message_id: Option<String> = row
            .try_get("message_id")
            .map_err(|_| StoreError::Unavailable)?;
        let task_id: Option<String> = row
            .try_get("task_id")
            .map_err(|_| StoreError::Unavailable)?;
        let task_objective: Option<String> = row
            .try_get("task_objective")
            .map_err(|_| StoreError::Unavailable)?;
        let task_state: Option<String> = row
            .try_get("task_state")
            .map_err(|_| StoreError::Unavailable)?;
        let parent_task_id: Option<String> = row
            .try_get("parent_task_id")
            .map_err(|_| StoreError::Unavailable)?;
        let dependency_task_ids: Option<String> = row
            .try_get("dependency_task_ids")
            .map_err(|_| StoreError::Unavailable)?;
        let assigned_run_id: Option<String> = row
            .try_get("assigned_run_id")
            .map_err(|_| StoreError::Unavailable)?;
        let run_id: Option<String> = row.try_get("run_id").map_err(|_| StoreError::Unavailable)?;
        let tool_call_id: Option<String> = row
            .try_get("tool_call_id")
            .map_err(|_| StoreError::Unavailable)?;
        let approval_id: Option<String> = row
            .try_get("approval_id")
            .map_err(|_| StoreError::Unavailable)?;
        let approval_state: Option<String> = row
            .try_get("approval_state")
            .map_err(|_| StoreError::Unavailable)?;
        let approval_policy: Option<String> = row
            .try_get("approval_policy")
            .map_err(|_| StoreError::Unavailable)?;
        let requested_root: Option<String> = row
            .try_get("requested_workspace_root_id")
            .map_err(|_| StoreError::Unavailable)?;
        let requested_directory: Option<String> = row
            .try_get("requested_relative_directory")
            .map_err(|_| StoreError::Unavailable)?;
        let effective_root: Option<String> = row
            .try_get("effective_workspace_root_id")
            .map_err(|_| StoreError::Unavailable)?;
        let effective_directory: Option<String> = row
            .try_get("effective_relative_directory")
            .map_err(|_| StoreError::Unavailable)?;
        let run_state: Option<String> = row
            .try_get("run_state")
            .map_err(|_| StoreError::Unavailable)?;
        let tool_call_state: Option<String> = row
            .try_get("tool_call_state")
            .map_err(|_| StoreError::Unavailable)?;
        let capability: Option<String> = row
            .try_get("capability")
            .map_err(|_| StoreError::Unavailable)?;
        let stdout: Option<String> = row.try_get("stdout").map_err(|_| StoreError::Unavailable)?;
        let stderr: Option<String> = row.try_get("stderr").map_err(|_| StoreError::Unavailable)?;
        let exit_code: Option<i64> = row
            .try_get("exit_code")
            .map_err(|_| StoreError::Unavailable)?;
        let output_stream: Option<String> = row
            .try_get("output_stream")
            .map_err(|_| StoreError::Unavailable)?;
        let output_content: Option<String> = row
            .try_get("output_content")
            .map_err(|_| StoreError::Unavailable)?;
        let artifact = parse_optional_artifact(
            &row,
            "artifact_hash",
            "artifact_media_type",
            "artifact_size",
        )?;
        let stdout_artifact = parse_optional_artifact(
            &row,
            "stdout_artifact_hash",
            "stdout_artifact_media_type",
            "stdout_artifact_size",
        )?;
        let stderr_artifact = parse_optional_artifact(
            &row,
            "stderr_artifact_hash",
            "stderr_artifact_media_type",
            "stderr_artifact_size",
        )?;
        let workspace_id = WorkspaceId::parse(
            row.try_get::<String, _>("workspace_id")
                .map_err(|_| StoreError::Unavailable)?,
        )
        .map_err(|_| StoreError::Unavailable)?;

        let event = match event_type.as_str() {
            "session.created" => {
                if message_id.is_some() {
                    return Err(StoreError::Unavailable);
                }
                StoredSessionEvent::session_created(
                    event_id,
                    stored_session_id,
                    cursor,
                    workspace_id,
                )
                .map_err(|_| StoreError::Unavailable)?
            }
            "message.appended" => {
                let message_id = MessageId::parse(message_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let loaded_message_id = MessageId::parse(
                    row.try_get::<String, _>("loaded_message_id")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                if loaded_message_id != message_id {
                    return Err(StoreError::Unavailable);
                }
                let message_session_id = SessionId::parse(
                    row.try_get::<String, _>("message_session_id")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                if message_session_id != stored_session_id {
                    return Err(StoreError::Unavailable);
                }
                let role = MessageRole::parse(
                    row.try_get::<String, _>("role")
                        .map_err(|_| StoreError::Unavailable)?
                        .as_str(),
                )
                .map_err(|_| StoreError::Unavailable)?;
                let message = Message::new(
                    message_id,
                    message_session_id,
                    role,
                    row.try_get::<String, _>("content")
                        .map_err(|_| StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?;
                StoredSessionEvent::message_appended(event_id, stored_session_id, cursor, message)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "task.created" => {
                let task_id = TaskId::parse(task_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let parent_task_id = parent_task_id
                    .map(TaskId::parse)
                    .transpose()
                    .map_err(|_| StoreError::Unavailable)?;
                let dependency_task_ids = serde_json::from_str::<Vec<String>>(
                    &dependency_task_ids.ok_or(StoreError::Unavailable)?,
                )
                .map_err(|_| StoreError::Unavailable)?
                .into_iter()
                .map(TaskId::parse)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| StoreError::Unavailable)?;
                let assigned_run_id = assigned_run_id
                    .map(RunId::parse)
                    .transpose()
                    .map_err(|_| StoreError::Unavailable)?;
                let task = Task::from_persisted(
                    task_id,
                    stored_session_id.clone(),
                    task_objective.ok_or(StoreError::Unavailable)?,
                    TaskState::parse(&task_state.ok_or(StoreError::Unavailable)?)
                        .map_err(|_| StoreError::Unavailable)?,
                    parent_task_id,
                    dependency_task_ids,
                    assigned_run_id,
                )
                .map_err(|_| StoreError::Unavailable)?;
                StoredSessionEvent::from_parts(
                    event_id,
                    stored_session_id,
                    cursor,
                    SessionEventPayload::TaskCreated { task },
                )
                .map_err(|_| StoreError::Unavailable)?
            }
            "run.created" | "run.state_changed" => {
                if message_id.is_some()
                    || tool_call_id.is_some()
                    || run_state.is_none()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                    || output_stream.is_some()
                    || output_content.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let state = RunState::parse(&run_state.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                if (event_type == "run.created" && state != RunState::Queued)
                    || (event_type == "run.state_changed" && state == RunState::Queued)
                {
                    return Err(StoreError::Unavailable);
                }
                let payload = if event_type == "run.created" {
                    let policy = approval_policy
                        .as_deref()
                        .map(ApprovalPolicy::parse)
                        .transpose()
                        .map_err(|_| StoreError::Unavailable)?;
                    let requested_scope = parse_scope(requested_root, requested_directory)
                        .map_err(|_| StoreError::Unavailable)?;
                    SessionEventPayload::RunCreated {
                        run_id,
                        state,
                        approval_policy: policy,
                        requested_scope,
                    }
                } else {
                    SessionEventPayload::RunStateChanged { run_id, state }
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "run.cancellation_requested" => {
                if message_id.is_some()
                    || tool_call_id.is_some()
                    || run_state.is_some()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                    || output_stream.is_some()
                    || output_content.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                StoredSessionEvent::from_parts(
                    event_id,
                    stored_session_id,
                    cursor,
                    SessionEventPayload::RunCancellationRequested { run_id },
                )
                .map_err(|_| StoreError::Unavailable)?
            }
            "tool_call.requested" | "tool_call.state_changed" | "tool_call.denied" => {
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let state = ToolCallState::parse(&tool_call_state.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let capability = capability.ok_or(StoreError::Unavailable)?;
                if message_id.is_some()
                    || run_state.is_some()
                    || output_stream.is_some()
                    || output_content.is_some()
                    || (event_type == "tool_call.requested"
                        && (state != ToolCallState::Requested
                            || stdout.is_some()
                            || stderr.is_some()
                            || exit_code.is_some()))
                {
                    return Err(StoreError::Unavailable);
                }
                let exit_code = exit_code
                    .map(|value| i32::try_from(value).map_err(|_| StoreError::Unavailable))
                    .transpose()?;
                let tool_call = ToolCall::from_persisted_event(PersistedToolCall {
                    tool_call_id,
                    run_id,
                    capability,
                    requested_scope: parse_scope(requested_root, requested_directory)
                        .map_err(|_| StoreError::Unavailable)?,
                    effective_scope: parse_scope(effective_root, effective_directory)
                        .map_err(|_| StoreError::Unavailable)?,
                    state,
                    stdout,
                    stderr,
                    stdout_artifact,
                    stderr_artifact,
                    exit_code,
                })
                .map_err(|_| StoreError::Unavailable)?;
                let payload = if event_type == "tool_call.requested" {
                    SessionEventPayload::ToolCallRequested { tool_call }
                } else if event_type == "tool_call.denied" {
                    SessionEventPayload::ToolCallDenied { tool_call }
                } else {
                    SessionEventPayload::ToolCallStateChanged { tool_call }
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "approval.requested" | "approval.decided" => {
                let approval_id = ApprovalId::parse(approval_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let state =
                    ApprovalState::parse(approval_state.as_deref().ok_or(StoreError::Unavailable)?)
                        .map_err(|_| StoreError::Unavailable)?;
                let scope = parse_scope(requested_root, requested_directory)
                    .map_err(|_| StoreError::Unavailable)?
                    .ok_or(StoreError::Unavailable)?;
                let approval =
                    Approval::from_persisted(approval_id, run_id, tool_call_id, scope, state)
                        .map_err(|_| StoreError::Unavailable)?;
                let payload = if event_type == "approval.requested" {
                    SessionEventPayload::ApprovalRequested { approval }
                } else {
                    SessionEventPayload::ApprovalDecided { approval }
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "tool_call.output" => {
                if message_id.is_some()
                    || run_state.is_some()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let stream =
                    ToolOutputStream::parse(&output_stream.ok_or(StoreError::Unavailable)?)
                        .map_err(|_| StoreError::Unavailable)?;
                let payload = SessionEventPayload::ToolCallOutput {
                    run_id,
                    tool_call_id,
                    stream,
                    content: output_content.ok_or(StoreError::Unavailable)?,
                };
                StoredSessionEvent::from_parts(event_id, stored_session_id, cursor, payload)
                    .map_err(|_| StoreError::Unavailable)?
            }
            "artifact.registered" => {
                if message_id.is_some()
                    || run_state.is_some()
                    || tool_call_state.is_some()
                    || capability.is_some()
                    || stdout.is_some()
                    || stderr.is_some()
                    || exit_code.is_some()
                    || output_content.is_some()
                    || stdout_artifact.is_some()
                    || stderr_artifact.is_some()
                {
                    return Err(StoreError::Unavailable);
                }
                let run_id = RunId::parse(run_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let tool_call_id = ToolCallId::parse(tool_call_id.ok_or(StoreError::Unavailable)?)
                    .map_err(|_| StoreError::Unavailable)?;
                let stream =
                    ToolOutputStream::parse(&output_stream.ok_or(StoreError::Unavailable)?)
                        .map_err(|_| StoreError::Unavailable)?;
                StoredSessionEvent::from_parts(
                    event_id,
                    stored_session_id,
                    cursor,
                    SessionEventPayload::ArtifactRegistered {
                        run_id,
                        tool_call_id,
                        stream,
                        artifact: artifact.ok_or(StoreError::Unavailable)?,
                    },
                )
                .map_err(|_| StoreError::Unavailable)?
            }
            _ => return Err(StoreError::Unavailable),
        };
        events.push(event);
    }

    Ok(events)
}

impl RunStore for SqliteStore {
    async fn start_root_run(
        &self,
        run: &Run,
        event: &SessionEvent,
        idempotency_key: &str,
    ) -> Result<StartRunMutation, RunStoreError> {
        if idempotency_key.is_empty() {
            return Err(RunStoreError::IdempotencyKeyRequired);
        }
        if run.state() != RunState::Queued
            || event != &SessionEvent::run_created(event.event_id().clone(), run)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let existing = sqlx::query(
            "SELECT run_id, approval_policy, workspace_root_id, relative_directory
             FROM start_run_idempotencies WHERE session_id = ? AND idempotency_key = ?",
        )
        .bind(run.session_id().as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if let Some(existing) = existing {
            let existing_run_id = existing
                .try_get::<String, _>("run_id")
                .map_err(|_| RunStoreError::Unavailable)?;
            let existing_policy = existing
                .try_get::<Option<String>, _>("approval_policy")
                .map_err(|_| RunStoreError::Unavailable)?;
            let existing_root = existing
                .try_get::<Option<String>, _>("workspace_root_id")
                .map_err(|_| RunStoreError::Unavailable)?;
            let existing_directory = existing
                .try_get::<Option<String>, _>("relative_directory")
                .map_err(|_| RunStoreError::Unavailable)?;
            let scope = run
                .requested_scope()
                .ok_or(RunStoreError::InvalidTransition)?;
            let policy = run
                .approval_policy()
                .ok_or(RunStoreError::InvalidTransition)?;
            if existing_policy.as_deref() != Some(policy.as_str())
                || existing_root.as_deref() != Some(scope.workspace_root_id().as_str())
                || existing_directory.as_deref() != Some(scope.relative_directory())
            {
                return Err(RunStoreError::IdempotencyConflict);
            }
            let existing_run_id =
                RunId::parse(existing_run_id).map_err(|_| RunStoreError::Unavailable)?;
            let existing_run = load_run(&mut transaction, &existing_run_id)
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            if existing_run.session_id() != run.session_id() {
                return Err(RunStoreError::IdempotencyConflict);
            }
            let snapshot = load_snapshot(&mut transaction, &existing_run_id)
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(StartRunMutation::new(
                snapshot,
                Vec::new(),
                StartRunDisposition::Duplicate,
            ));
        }

        let scope = run
            .requested_scope()
            .ok_or(RunStoreError::InvalidTransition)?;
        let policy = run
            .approval_policy()
            .ok_or(RunStoreError::InvalidTransition)?;
        let result = sqlx::query("INSERT INTO runs (run_id, session_id, state, approval_policy, workspace_root_id, relative_directory) VALUES (?, ?, ?, ?, ?, ?)")
            .bind(run.run_id().as_str())
            .bind(run.session_id().as_str())
            .bind(run.state().as_str())
            .bind(policy.as_str())
            .bind(scope.workspace_root_id().as_str())
            .bind(scope.relative_directory())
            .execute(&mut *transaction)
            .await;
        if let Err(error) = result {
            if is_unique_constraint(&error) {
                let active: Option<String> = sqlx::query_scalar(
                        "SELECT run_id FROM runs WHERE session_id = ? AND state IN ('queued', 'running', 'waiting_for_approval', 'cancelling') LIMIT 1",
                )
                .bind(run.session_id().as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
                if active.is_some() {
                    return Err(RunStoreError::ActiveRootRunExists);
                }
            }
            return Err(RunStoreError::Unavailable);
        }
        let stored_event = insert_run_event(&mut transaction, event).await?;
        sqlx::query(
            "INSERT INTO start_run_idempotencies (session_id, idempotency_key, run_id, approval_policy, workspace_root_id, relative_directory) VALUES (?, ?, ?, ?, ?, ?)",
        )
        .bind(run.session_id().as_str())
        .bind(idempotency_key)
        .bind(run.run_id().as_str())
        .bind(policy.as_str())
        .bind(scope.workspace_root_id().as_str())
        .bind(scope.relative_directory())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(StartRunMutation::new(
            RunSnapshot::new(run.clone(), Vec::new()),
            vec![stored_event],
            StartRunDisposition::Created,
        ))
    }

    async fn get_run(&self, id: &RunId) -> Result<Option<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let snapshot = load_snapshot(&mut transaction, id).await?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(snapshot)
    }

    async fn get_tool_call(
        &self,
        id: &ToolCallId,
    ) -> Result<Option<(Run, ToolCall)>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT r.run_id, r.session_id, r.state, r.approval_policy, r.workspace_root_id, r.relative_directory,
                    t.tool_call_id, t.run_id AS tool_run_id, t.capability, t.state AS tool_state,
                    t.requested_workspace_root_id, t.requested_relative_directory,
                    t.effective_workspace_root_id, t.effective_relative_directory,
                    t.stdout, t.stderr, t.exit_code,
                    osa.content_hash AS stdout_artifact_hash,
                    osa.media_type AS stdout_artifact_media_type,
                    osa.size AS stdout_artifact_size,
                    esa.content_hash AS stderr_artifact_hash,
                    esa.media_type AS stderr_artifact_media_type,
                    esa.size AS stderr_artifact_size
             FROM tool_calls t
             JOIN runs r ON r.run_id = t.run_id
             LEFT JOIN artifacts osa ON osa.content_hash = t.stdout_artifact_hash
             LEFT JOIN artifacts esa ON esa.content_hash = t.stderr_artifact_hash
             WHERE t.tool_call_id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let result = row.map(|row| parse_run_and_tool_call(&row)).transpose()?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(result)
    }

    async fn get_approval(
        &self,
        id: &ApprovalId,
    ) -> Result<Option<(Run, Approval)>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT a.approval_id, a.run_id AS approval_run_id, a.tool_call_id AS approval_tool_call_id,
                    a.workspace_root_id AS approval_workspace_root_id,
                    a.relative_directory AS approval_relative_directory, a.state AS approval_state
             FROM approvals a WHERE a.approval_id = ?",
        )
        .bind(id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let result = match row {
            Some(row) => {
                let approval = parse_approval(&row)?;
                let run = load_run(&mut transaction, approval.run_id())
                    .await?
                    .ok_or(RunStoreError::Unavailable)?;
                Some((run, approval))
            }
            None => None,
        };
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(result)
    }

    async fn get_approval_decision(
        &self,
        approval_id: &ApprovalId,
        idempotency_key: &str,
    ) -> Result<Option<(ApprovalState, RunSnapshot)>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let decision: Option<String> = sqlx::query_scalar(
            "SELECT decision FROM approval_decision_idempotencies
             WHERE approval_id = ? AND idempotency_key = ?",
        )
        .bind(approval_id.as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let result = match decision {
            Some(decision) => {
                let decision =
                    ApprovalState::parse(&decision).map_err(|_| RunStoreError::Unavailable)?;
                let approval = sqlx::query_scalar::<_, String>(
                    "SELECT run_id FROM approvals WHERE approval_id = ?",
                )
                .bind(approval_id.as_str())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?
                .ok_or(RunStoreError::ApprovalNotFound)?;
                let run_id = RunId::parse(approval).map_err(|_| RunStoreError::Unavailable)?;
                let snapshot = load_snapshot(&mut transaction, &run_id)
                    .await?
                    .ok_or(RunStoreError::Unavailable)?;
                Some((decision, snapshot))
            }
            None => None,
        };
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(result)
    }

    async fn begin_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        approval: Option<&Approval>,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        if tool_call.run_id() != run.run_id() {
            return Err(RunStoreError::InvalidTransition);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        if current.state() != RunState::Queued || current.session_id() != run.session_id() {
            return Err(RunStoreError::InvalidTransition);
        }
        let approval_policy = current
            .approval_policy()
            .ok_or(RunStoreError::InvalidTransition)?;
        let requested_scope = current
            .requested_scope()
            .cloned()
            .ok_or(RunStoreError::InvalidTransition)?;
        let running = current
            .transition(RunState::Running)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        let requested_tool_call = ToolCall::new(
            tool_call.tool_call_id().clone(),
            current.run_id().clone(),
            DETERMINISTIC_SUBPROCESS_CAPABILITY.to_owned(),
            requested_scope.clone(),
        );
        let (expected_run, expected_tool_call, expected_approval, expected_events) =
            match approval_policy {
                ApprovalPolicy::Ask if events.len() == 5 => {
                    let supplied_approval = approval.ok_or(RunStoreError::InvalidTransition)?;
                    let expected_run = running
                        .transition(RunState::WaitingForApproval)
                        .map_err(|_| RunStoreError::InvalidTransition)?;
                    let expected_tool_call = requested_tool_call
                        .clone()
                        .transition(ToolCallState::AwaitingApproval)
                        .map_err(|_| RunStoreError::InvalidTransition)?;
                    let expected_approval = Approval::new(
                        supplied_approval.approval_id().clone(),
                        current.run_id().clone(),
                        expected_tool_call.tool_call_id().clone(),
                        requested_scope,
                    );
                    let expected_events = vec![
                        SessionEvent::run_state_changed(events[0].event_id().clone(), &running),
                        SessionEvent::tool_call_requested(
                            events[1].event_id().clone(),
                            current.session_id().clone(),
                            requested_tool_call,
                        ),
                        SessionEvent::tool_call_state_changed(
                            events[2].event_id().clone(),
                            current.session_id().clone(),
                            expected_tool_call.clone(),
                        ),
                        SessionEvent::approval_requested(
                            events[3].event_id().clone(),
                            current.session_id().clone(),
                            expected_approval.clone(),
                        ),
                        SessionEvent::run_state_changed(
                            events[4].event_id().clone(),
                            &expected_run,
                        ),
                    ];
                    (
                        expected_run,
                        expected_tool_call,
                        Some(expected_approval),
                        expected_events,
                    )
                }
                ApprovalPolicy::FullAccess if events.len() == 3 && approval.is_none() => {
                    let expected_tool_call = requested_tool_call
                        .clone()
                        .with_effective_scope(requested_scope)
                        .map_err(|_| RunStoreError::InvalidTransition)?;
                    let expected_events = vec![
                        SessionEvent::run_state_changed(events[0].event_id().clone(), &running),
                        SessionEvent::tool_call_requested(
                            events[1].event_id().clone(),
                            current.session_id().clone(),
                            requested_tool_call,
                        ),
                        SessionEvent::tool_call_state_changed(
                            events[2].event_id().clone(),
                            current.session_id().clone(),
                            expected_tool_call.clone(),
                        ),
                    ];
                    (running, expected_tool_call, None, expected_events)
                }
                ApprovalPolicy::ReadOnly if events.len() == 3 && approval.is_none() => {
                    let expected_tool_call = requested_tool_call
                        .clone()
                        .transition(ToolCallState::Denied)
                        .map_err(|_| RunStoreError::InvalidTransition)?;
                    let expected_events = vec![
                        SessionEvent::run_state_changed(events[0].event_id().clone(), &running),
                        SessionEvent::tool_call_requested(
                            events[1].event_id().clone(),
                            current.session_id().clone(),
                            requested_tool_call,
                        ),
                        SessionEvent::tool_call_denied(
                            events[2].event_id().clone(),
                            current.session_id().clone(),
                            expected_tool_call.clone(),
                        ),
                    ];
                    (running, expected_tool_call, None, expected_events)
                }
                _ => return Err(RunStoreError::InvalidTransition),
            };
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tool_calls WHERE run_id = ?")
            .bind(run.run_id().as_str())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        if count != 0
            || run != &expected_run
            || tool_call != &expected_tool_call
            || approval != expected_approval.as_ref()
            || events != expected_events
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let updated =
            sqlx::query("UPDATE runs SET state = ? WHERE run_id = ? AND state = 'queued'")
                .bind(run.state().as_str())
                .bind(run.run_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        sqlx::query(
            "INSERT INTO tool_calls (tool_call_id, run_id, capability, state,
                requested_workspace_root_id, requested_relative_directory,
                effective_workspace_root_id, effective_relative_directory)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(tool_call.tool_call_id().as_str())
        .bind(tool_call.run_id().as_str())
        .bind(tool_call.capability())
        .bind(tool_call.state().as_str())
        .bind(
            tool_call
                .requested_scope()
                .map(|s| s.workspace_root_id().as_str()),
        )
        .bind(
            tool_call
                .requested_scope()
                .map(WorkspacePathScope::relative_directory),
        )
        .bind(
            tool_call
                .effective_scope()
                .map(|s| s.workspace_root_id().as_str()),
        )
        .bind(
            tool_call
                .effective_scope()
                .map(WorkspacePathScope::relative_directory),
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if let Some(approval) = approval {
            if approval.run_id() != run.run_id()
                || approval.tool_call_id() != tool_call.tool_call_id()
                || approval.state() != ApprovalState::Pending
            {
                return Err(RunStoreError::InvalidTransition);
            }
            sqlx::query(
                "INSERT INTO approvals
                 (approval_id, run_id, tool_call_id, workspace_root_id, relative_directory, state)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
            .bind(approval.approval_id().as_str())
            .bind(approval.run_id().as_str())
            .bind(approval.tool_call_id().as_str())
            .bind(approval.scope().workspace_root_id().as_str())
            .bind(approval.scope().relative_directory())
            .bind(approval.state().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        }
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }

    async fn decide_approval(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        approval: &Approval,
        idempotency_key: &str,
        events: &[SessionEvent],
    ) -> Result<kiln_core::ApprovalDecisionMutation, RunStoreError> {
        if idempotency_key.is_empty() || approval.state() == ApprovalState::Pending {
            return Err(RunStoreError::InvalidTransition);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let duplicate: Option<String> = sqlx::query_scalar(
            "SELECT decision FROM approval_decision_idempotencies
             WHERE approval_id = ? AND idempotency_key = ?",
        )
        .bind(approval.approval_id().as_str())
        .bind(idempotency_key)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if let Some(duplicate) = duplicate {
            let duplicate =
                ApprovalState::parse(&duplicate).map_err(|_| RunStoreError::Unavailable)?;
            if duplicate != approval.state() {
                return Err(RunStoreError::IdempotencyConflict);
            }
            let duplicate_run_id: String =
                sqlx::query_scalar("SELECT run_id FROM approvals WHERE approval_id = ?")
                    .bind(approval.approval_id().as_str())
                    .fetch_optional(&mut *transaction)
                    .await
                    .map_err(|_| RunStoreError::Unavailable)?
                    .ok_or(RunStoreError::ApprovalNotFound)?;
            let duplicate_run_id =
                RunId::parse(duplicate_run_id).map_err(|_| RunStoreError::Unavailable)?;
            let snapshot = load_snapshot(&mut transaction, &duplicate_run_id)
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(kiln_core::ApprovalDecisionMutation::new(
                snapshot,
                Vec::new(),
                kiln_core::ApprovalDecisionDisposition::Duplicate,
            ));
        }
        let current_run = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::ApprovalNotFound)?;
        let current_row = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state,
                    requested_workspace_root_id, requested_relative_directory,
                    effective_workspace_root_id, effective_relative_directory,
                    stdout, stderr, exit_code,
                    NULL AS stdout_artifact_hash, NULL AS stdout_artifact_media_type,
                    NULL AS stdout_artifact_size, NULL AS stderr_artifact_hash,
                    NULL AS stderr_artifact_media_type, NULL AS stderr_artifact_size
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call.tool_call_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::ApprovalNotFound)?;
        let current_tool = parse_tool_call(&current_row)?;
        let current_approval_row = sqlx::query(
            "SELECT approval_id, run_id AS approval_run_id, tool_call_id AS approval_tool_call_id,
                    workspace_root_id AS approval_workspace_root_id,
                    relative_directory AS approval_relative_directory, state AS approval_state
             FROM approvals WHERE approval_id = ?",
        )
        .bind(approval.approval_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::ApprovalNotFound)?;
        let current_approval = parse_approval(&current_approval_row)?;
        if current_run.state() != RunState::WaitingForApproval
            || current_tool.state() != ToolCallState::AwaitingApproval
            || current_approval.state() != ApprovalState::Pending
            || current_tool.run_id() != current_run.run_id()
            || current_approval.run_id() != current_run.run_id()
            || current_approval.tool_call_id() != current_tool.tool_call_id()
        {
            return Err(RunStoreError::ApprovalAlreadyDecided);
        }
        let expected_approval = current_approval
            .decide(approval.state())
            .map_err(|_| RunStoreError::InvalidTransition)?;
        let expected_run = current_run
            .transition(RunState::Running)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        let expected_tool_call = match approval.state() {
            ApprovalState::Approved => current_tool
                .with_effective_scope(current_approval.scope().clone())
                .map_err(|_| RunStoreError::InvalidTransition)?,
            ApprovalState::Rejected => current_tool
                .transition(ToolCallState::Denied)
                .map_err(|_| RunStoreError::InvalidTransition)?,
            ApprovalState::Pending => return Err(RunStoreError::InvalidTransition),
        };
        if events.len() != 3 {
            return Err(RunStoreError::InvalidTransition);
        }
        let expected_events = vec![
            SessionEvent::approval_decided(
                events[0].event_id().clone(),
                current_run.session_id().clone(),
                expected_approval.clone(),
            ),
            match approval.state() {
                ApprovalState::Approved => SessionEvent::tool_call_state_changed(
                    events[1].event_id().clone(),
                    current_run.session_id().clone(),
                    expected_tool_call.clone(),
                ),
                ApprovalState::Rejected => SessionEvent::tool_call_denied(
                    events[1].event_id().clone(),
                    current_run.session_id().clone(),
                    expected_tool_call.clone(),
                ),
                ApprovalState::Pending => unreachable!(),
            },
            SessionEvent::run_state_changed(events[2].event_id().clone(), &expected_run),
        ];
        if run != &expected_run
            || tool_call != &expected_tool_call
            || approval != &expected_approval
            || events != expected_events
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let approval_updated = sqlx::query(
            "UPDATE approvals SET state = ? WHERE approval_id = ? AND state = 'pending'",
        )
        .bind(approval.state().as_str())
        .bind(approval.approval_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let tool_updated = sqlx::query(
            "UPDATE tool_calls SET state = ?, effective_workspace_root_id = ?, effective_relative_directory = ?
             WHERE tool_call_id = ? AND state = 'awaiting_approval'",
        )
        .bind(tool_call.state().as_str())
        .bind(tool_call.effective_scope().map(|scope| scope.workspace_root_id().as_str()))
        .bind(tool_call.effective_scope().map(WorkspacePathScope::relative_directory))
        .bind(tool_call.tool_call_id().as_str())
        .execute(&mut *transaction).await
        .map_err(|_| RunStoreError::Unavailable)?;
        let run_updated = sqlx::query(
            "UPDATE runs SET state = 'running' WHERE run_id = ? AND state = 'waiting_for_approval'",
        )
        .bind(run.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if approval_updated.rows_affected() != 1
            || tool_updated.rows_affected() != 1
            || run_updated.rows_affected() != 1
        {
            return Err(RunStoreError::InvalidTransition);
        }
        sqlx::query("INSERT INTO approval_decision_idempotencies (approval_id, idempotency_key, decision) VALUES (?, ?, ?)")
            .bind(approval.approval_id().as_str()).bind(idempotency_key).bind(approval.state().as_str())
            .execute(&mut *transaction).await.map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(kiln_core::ApprovalDecisionMutation::new(
            snapshot,
            stored_events,
            kiln_core::ApprovalDecisionDisposition::Applied,
        ))
    }

    async fn finish_denied_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        if events.len() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        let current_tool_call = current
            .tool_call(tool_call.tool_call_id())
            .ok_or(RunStoreError::InvalidTransition)?;
        let expected_run = current
            .run()
            .transition(RunState::Failed)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        if current.run().state() != RunState::Running
            || current_tool_call.run_id() != current.run().run_id()
            || current_tool_call.state() != ToolCallState::Denied
            || run != &expected_run
            || tool_call != current_tool_call
            || events[0]
                != SessionEvent::run_state_changed(events[0].event_id().clone(), &expected_run)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let updated =
            sqlx::query("UPDATE runs SET state = 'failed' WHERE run_id = ? AND state = 'running'")
                .bind(run.run_id().as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }

    async fn begin_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        events: &[SessionEvent],
    ) -> Result<RunMutation<ToolCall>, RunStoreError> {
        if events.len() != 1 {
            return Err(RunStoreError::Unavailable);
        }
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state,
                    requested_workspace_root_id, requested_relative_directory,
                    effective_workspace_root_id, effective_relative_directory,
                    stdout, stderr, exit_code,
                    NULL AS stdout_artifact_hash, NULL AS stdout_artifact_media_type,
                    NULL AS stdout_artifact_size, NULL AS stderr_artifact_hash,
                    NULL AS stderr_artifact_media_type, NULL AS stderr_artifact_size
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::Unavailable)?;
        let current = parse_tool_call(&row)?;
        let current_run = load_run(&mut transaction, current.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        if !matches!(
            current.state(),
            ToolCallState::Requested | ToolCallState::Ready
        ) || current_run.state() != RunState::Running
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let running = current
            .transition(ToolCallState::Running)
            .map_err(|_| RunStoreError::InvalidTransition)?;
        if events[0]
            != SessionEvent::tool_call_state_changed(
                events[0].event_id().clone(),
                current_run.session_id().clone(),
                running.clone(),
            )
        {
            return Err(RunStoreError::InvalidTransition);
        }
        sqlx::query("UPDATE tool_calls SET state = 'running' WHERE tool_call_id = ? AND state IN ('requested', 'ready')")
            .bind(tool_call_id.as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(running, stored_events))
    }

    async fn finish_execution(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current_run = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        let current_tool = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state,
                    requested_workspace_root_id, requested_relative_directory,
                    effective_workspace_root_id, effective_relative_directory,
                    stdout, stderr, exit_code,
                    NULL AS stdout_artifact_hash, NULL AS stdout_artifact_media_type,
                    NULL AS stdout_artifact_size, NULL AS stderr_artifact_hash,
                    NULL AS stderr_artifact_media_type, NULL AS stderr_artifact_size
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call.tool_call_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::Unavailable)
        .and_then(|row| parse_tool_call(&row))?;
        if current_run.state() != RunState::Running
            || current_tool.state() != ToolCallState::Running
            || current_run.session_id() != run.session_id()
            || current_tool.tool_call_id() != tool_call.tool_call_id()
            || current_tool.run_id() != run.run_id()
            || tool_call.run_id() != run.run_id()
            || current_tool.capability() != tool_call.capability()
            || !matches!(
                (run.state(), tool_call.state()),
                (RunState::Completed, ToolCallState::Completed)
                    | (RunState::Failed, ToolCallState::Failed)
            )
            || !finish_events_match(events, run, tool_call)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        insert_tool_call_artifacts(&mut transaction, tool_call).await?;
        sqlx::query("UPDATE tool_calls SET state = ?, stdout = ?, stderr = ?, stdout_artifact_hash = ?, stderr_artifact_hash = ?, exit_code = ? WHERE tool_call_id = ? AND state = 'running'")
            .bind(tool_call.state().as_str())
            .bind(tool_call.stdout())
            .bind(tool_call.stderr())
            .bind(tool_call.stdout_artifact().map(|artifact| artifact.content_hash().as_str()))
            .bind(tool_call.stderr_artifact().map(|artifact| artifact.content_hash().as_str()))
            .bind(tool_call.exit_code())
            .bind(tool_call.tool_call_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        sqlx::query("UPDATE runs SET state = ? WHERE run_id = ? AND state = 'running'")
            .bind(run.state().as_str())
            .bind(run.run_id().as_str())
            .execute(&mut *transaction)
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }

    async fn request_cancellation(
        &self,
        run: &Run,
        tool_call: Option<&ToolCall>,
        approval: Option<&Approval>,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current_snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        let current = current_snapshot.run().clone();
        if current.session_id() != run.session_id() {
            return Err(RunStoreError::InvalidTransition);
        }
        if current.state() == RunState::WaitingForApproval {
            let Some(tool_call) = tool_call else {
                return Err(RunStoreError::InvalidTransition);
            };
            let Some(approval) = approval else {
                return Err(RunStoreError::InvalidTransition);
            };
            if events.len() != 5 {
                return Err(RunStoreError::InvalidTransition);
            }
            let current_approval = current_snapshot
                .approval(approval.approval_id())
                .ok_or(RunStoreError::InvalidTransition)?;
            let current_tool_call = current_snapshot
                .tool_call(tool_call.tool_call_id())
                .ok_or(RunStoreError::InvalidTransition)?;
            if current_approval.run_id() != current.run_id()
                || current_approval.tool_call_id() != current_tool_call.tool_call_id()
                || current_approval.state() != ApprovalState::Pending
                || current_tool_call.run_id() != current.run_id()
                || current_tool_call.state() != ToolCallState::AwaitingApproval
            {
                return Err(RunStoreError::InvalidTransition);
            }
            let expected_approval = current_approval
                .decide(ApprovalState::Rejected)
                .map_err(|_| RunStoreError::InvalidTransition)?;
            let expected_tool_call = current_tool_call
                .transition(ToolCallState::Denied)
                .map_err(|_| RunStoreError::InvalidTransition)?;
            let cancelling = current
                .transition(RunState::Cancelling)
                .map_err(|_| RunStoreError::InvalidTransition)?;
            let expected_run = cancelling
                .transition(RunState::Cancelled)
                .map_err(|_| RunStoreError::InvalidTransition)?;
            let expected_events = vec![
                SessionEvent::run_cancellation_requested(events[0].event_id().clone(), &current),
                SessionEvent::run_state_changed(events[1].event_id().clone(), &cancelling),
                SessionEvent::approval_decided(
                    events[2].event_id().clone(),
                    current.session_id().clone(),
                    expected_approval.clone(),
                ),
                SessionEvent::tool_call_denied(
                    events[3].event_id().clone(),
                    current.session_id().clone(),
                    expected_tool_call.clone(),
                ),
                SessionEvent::run_state_changed(events[4].event_id().clone(), &expected_run),
            ];
            if run != &expected_run
                || tool_call != &expected_tool_call
                || approval != &expected_approval
                || events != expected_events
            {
                return Err(RunStoreError::InvalidTransition);
            }
            let approval_updated = sqlx::query("UPDATE approvals SET state = 'rejected' WHERE approval_id = ? AND state = 'pending'")
                .bind(approval.approval_id().as_str()).execute(&mut *transaction).await
                .map_err(|_| RunStoreError::Unavailable)?;
            let tool_updated = sqlx::query("UPDATE tool_calls SET state = 'denied' WHERE tool_call_id = ? AND state = 'awaiting_approval'")
                .bind(tool_call.tool_call_id().as_str()).execute(&mut *transaction).await
                .map_err(|_| RunStoreError::Unavailable)?;
            let run_updated = sqlx::query("UPDATE runs SET state = 'cancelled' WHERE run_id = ? AND state = 'waiting_for_approval'")
                .bind(run.run_id().as_str()).execute(&mut *transaction).await
                .map_err(|_| RunStoreError::Unavailable)?;
            if approval_updated.rows_affected() != 1
                || tool_updated.rows_affected() != 1
                || run_updated.rows_affected() != 1
            {
                return Err(RunStoreError::InvalidTransition);
            }
            let stored_events = insert_events(&mut transaction, events).await?;
            let snapshot = load_snapshot(&mut transaction, run.run_id())
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(RunMutation::new(snapshot, stored_events));
        }

        let transition = match (current.state(), run.state()) {
            (RunState::Queued, RunState::Cancelled) => Some(RunState::Queued),
            (RunState::Running, RunState::Cancelling) => Some(RunState::Running),
            (RunState::Cancelling, RunState::Cancelling)
            | (RunState::Completed, RunState::Completed)
            | (RunState::Failed, RunState::Failed)
            | (RunState::Cancelled, RunState::Cancelled) => None,
            _ => return Err(RunStoreError::InvalidTransition),
        };

        let Some(expected_old_state) = transition else {
            if !events.is_empty() {
                return Err(RunStoreError::InvalidTransition);
            }
            let snapshot = load_snapshot(&mut transaction, run.run_id())
                .await?
                .ok_or(RunStoreError::Unavailable)?;
            transaction
                .commit()
                .await
                .map_err(|_| RunStoreError::Unavailable)?;
            return Ok(RunMutation::new(snapshot, Vec::new()));
        };

        if events.len() != 2
            || events[0]
                != SessionEvent::run_cancellation_requested(events[0].event_id().clone(), &current)
            || events[1] != SessionEvent::run_state_changed(events[1].event_id().clone(), run)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        let updated = sqlx::query(
            "UPDATE runs SET state = ? WHERE run_id = ? AND session_id = ? AND state = ?",
        )
        .bind(run.state().as_str())
        .bind(run.run_id().as_str())
        .bind(run.session_id().as_str())
        .bind(expected_old_state.as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if updated.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }

    async fn finish_cancellation(
        &self,
        run: &Run,
        tool_call: &ToolCall,
        events: &[SessionEvent],
    ) -> Result<RunMutation<RunSnapshot>, RunStoreError> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        let current_run = load_run(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        let current_tool = sqlx::query(
            "SELECT tool_call_id, run_id, capability, state,
                    requested_workspace_root_id, requested_relative_directory,
                    effective_workspace_root_id, effective_relative_directory,
                    stdout, stderr, exit_code,
                    NULL AS stdout_artifact_hash, NULL AS stdout_artifact_media_type,
                    NULL AS stdout_artifact_size, NULL AS stderr_artifact_hash,
                    NULL AS stderr_artifact_media_type, NULL AS stderr_artifact_size
             FROM tool_calls WHERE tool_call_id = ?",
        )
        .bind(tool_call.tool_call_id().as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?
        .ok_or(RunStoreError::Unavailable)
        .and_then(|row| parse_tool_call(&row))?;
        if current_run.state() != RunState::Cancelling
            || run.state() != RunState::Cancelled
            || current_run.session_id() != run.session_id()
            || !matches!(
                current_tool.state(),
                ToolCallState::Requested | ToolCallState::Ready | ToolCallState::Running
            )
            || current_tool.run_id() != current_run.run_id()
            || tool_call.run_id() != run.run_id()
            || current_tool.tool_call_id() != tool_call.tool_call_id()
            || current_tool.capability() != tool_call.capability()
            || current_tool.requested_scope() != tool_call.requested_scope()
            || current_tool.effective_scope() != tool_call.effective_scope()
            || tool_call.state() != ToolCallState::Cancelled
            || (tool_call.stdout().is_none() == tool_call.stdout_artifact().is_none())
            || (tool_call.stderr().is_none() == tool_call.stderr_artifact().is_none())
            || !finish_events_match(events, run, tool_call)
        {
            return Err(RunStoreError::InvalidTransition);
        }
        insert_tool_call_artifacts(&mut transaction, tool_call).await?;
        let updated_tool = sqlx::query(
            "UPDATE tool_calls
             SET state = 'cancelled', stdout = ?, stderr = ?, stdout_artifact_hash = ?, stderr_artifact_hash = ?, exit_code = ?
             WHERE tool_call_id = ? AND run_id = ? AND state IN ('requested', 'ready', 'running')",
        )
        .bind(tool_call.stdout())
        .bind(tool_call.stderr())
        .bind(tool_call.stdout_artifact().map(|artifact| artifact.content_hash().as_str()))
        .bind(tool_call.stderr_artifact().map(|artifact| artifact.content_hash().as_str()))
        .bind(tool_call.exit_code())
        .bind(tool_call.tool_call_id().as_str())
        .bind(run.run_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if updated_tool.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let updated_run = sqlx::query(
            "UPDATE runs SET state = 'cancelled' WHERE run_id = ? AND session_id = ? AND state = 'cancelling'",
        )
        .bind(run.run_id().as_str())
        .bind(run.session_id().as_str())
        .execute(&mut *transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if updated_run.rows_affected() != 1 {
            return Err(RunStoreError::InvalidTransition);
        }
        let stored_events = insert_events(&mut transaction, events).await?;
        let snapshot = load_snapshot(&mut transaction, run.run_id())
            .await?
            .ok_or(RunStoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| RunStoreError::Unavailable)?;
        Ok(RunMutation::new(snapshot, stored_events))
    }
}

fn finish_events_match(events: &[SessionEvent], run: &Run, tool_call: &ToolCall) -> bool {
    let Some(index) = output_event_index(
        events,
        run,
        tool_call,
        ToolOutputStream::Stdout,
        tool_call.stdout(),
        tool_call.stdout_artifact(),
        0,
    ) else {
        return false;
    };
    let Some(mut index) = output_event_index(
        events,
        run,
        tool_call,
        ToolOutputStream::Stderr,
        tool_call.stderr(),
        tool_call.stderr_artifact(),
        index,
    ) else {
        return false;
    };
    let Some(tool_event) = events.get(index) else {
        return false;
    };
    if tool_event
        != &SessionEvent::tool_call_state_changed(
            tool_event.event_id().clone(),
            run.session_id().clone(),
            tool_call.clone(),
        )
    {
        return false;
    }
    index += 1;
    let Some(run_event) = events.get(index) else {
        return false;
    };
    run_event == &SessionEvent::run_state_changed(run_event.event_id().clone(), run)
        && index + 1 == events.len()
}

fn output_event_index(
    events: &[SessionEvent],
    run: &Run,
    tool_call: &ToolCall,
    stream: ToolOutputStream,
    inline: Option<&str>,
    artifact: Option<&Artifact>,
    index: usize,
) -> Option<usize> {
    let expected = match (inline, artifact) {
        (Some(""), None) => return Some(index),
        (Some(content), None) => SessionEvent::tool_call_output(
            events.get(index)?.event_id().clone(),
            run.session_id().clone(),
            run.run_id().clone(),
            tool_call.tool_call_id().clone(),
            stream,
            content.to_owned(),
        ),
        (None, Some(artifact)) => SessionEvent::artifact_registered(
            events.get(index)?.event_id().clone(),
            run.session_id().clone(),
            run.run_id().clone(),
            tool_call.tool_call_id().clone(),
            stream,
            artifact.clone(),
        ),
        _ => return None,
    };
    (events.get(index)? == &expected).then_some(index + 1)
}

fn committed_cursor(value: i64) -> Result<EventCursor, StoreError> {
    if value < 1 {
        return Err(StoreError::Unavailable);
    }
    Ok(EventCursor::from_value(
        u64::try_from(value).map_err(|_| StoreError::Unavailable)?,
    ))
}

async fn current_cursor(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<EventCursor, StoreError> {
    let row = sqlx::query("SELECT MAX(cursor) AS current_cursor FROM session_events")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
    match row
        .try_get::<Option<i64>, _>("current_cursor")
        .map_err(|_| StoreError::Unavailable)?
    {
        Some(value) => committed_cursor(value),
        None => Ok(EventCursor::zero()),
    }
}

async fn insert_events(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    events: &[SessionEvent],
) -> Result<Vec<StoredSessionEvent>, RunStoreError> {
    let mut stored = Vec::with_capacity(events.len());
    for event in events {
        stored.push(insert_run_event(transaction, event).await?);
    }
    Ok(stored)
}

async fn insert_tool_call_artifacts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    tool_call: &ToolCall,
) -> Result<(), RunStoreError> {
    for artifact in [tool_call.stdout_artifact(), tool_call.stderr_artifact()]
        .into_iter()
        .flatten()
    {
        let size = i64::try_from(artifact.size()).map_err(|_| RunStoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO artifacts (content_hash, media_type, size, storage_reference)
             VALUES (?, ?, ?, ?) ON CONFLICT(content_hash) DO NOTHING",
        )
        .bind(artifact.content_hash().as_str())
        .bind(artifact.media_type())
        .bind(size)
        .bind(artifact.content_hash().as_str())
        .execute(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        let row = sqlx::query(
            "SELECT media_type, size, storage_reference FROM artifacts WHERE content_hash = ?",
        )
        .bind(artifact.content_hash().as_str())
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
        if row
            .try_get::<String, _>("media_type")
            .map_err(|_| RunStoreError::Unavailable)?
            != artifact.media_type()
            || row
                .try_get::<i64, _>("size")
                .map_err(|_| RunStoreError::Unavailable)?
                != size
            || row
                .try_get::<String, _>("storage_reference")
                .map_err(|_| RunStoreError::Unavailable)?
                != artifact.content_hash().as_str()
        {
            return Err(RunStoreError::Unavailable);
        }
    }
    Ok(())
}

struct RunEventColumns<'a> {
    event_type: &'static str,
    message_id: Option<&'a str>,
    run_id: Option<&'a str>,
    tool_call_id: Option<&'a str>,
    run_state: Option<&'a str>,
    tool_call_state: Option<&'a str>,
    capability: Option<&'a str>,
    stdout: Option<&'a str>,
    stderr: Option<&'a str>,
    exit_code: Option<i32>,
    output_stream: Option<&'a str>,
    output_content: Option<&'a str>,
    artifact_hash: Option<&'a str>,
    stdout_artifact_hash: Option<&'a str>,
    stderr_artifact_hash: Option<&'a str>,
    approval_id: Option<&'a str>,
    approval_state: Option<&'a str>,
    approval_policy: Option<&'a str>,
    requested_workspace_root_id: Option<&'a str>,
    requested_relative_directory: Option<&'a str>,
    effective_workspace_root_id: Option<&'a str>,
    effective_relative_directory: Option<&'a str>,
}

fn run_event_columns(event: &SessionEvent) -> Result<RunEventColumns<'_>, RunStoreError> {
    match event.payload() {
        SessionEventPayload::RunCreated {
            run_id,
            state,
            approval_policy,
            requested_scope,
        } => Ok(RunEventColumns {
            event_type: "run.created",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: None,
            run_state: Some(state.as_str()),
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: approval_policy.map(ApprovalPolicy::as_str),
            requested_workspace_root_id: requested_scope
                .as_ref()
                .map(|s| s.workspace_root_id().as_str()),
            requested_relative_directory: requested_scope
                .as_ref()
                .map(WorkspacePathScope::relative_directory),
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::RunStateChanged { run_id, state } => Ok(RunEventColumns {
            event_type: "run.state_changed",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: None,
            run_state: Some(state.as_str()),
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: None,
            requested_relative_directory: None,
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::RunCancellationRequested { run_id } => Ok(RunEventColumns {
            event_type: "run.cancellation_requested",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: None,
            run_state: None,
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: None,
            requested_relative_directory: None,
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::ToolCallRequested { tool_call } => Ok(RunEventColumns {
            event_type: "tool_call.requested",
            message_id: None,
            run_id: Some(tool_call.run_id().as_str()),
            tool_call_id: Some(tool_call.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: Some(tool_call.state().as_str()),
            capability: Some(tool_call.capability()),
            stdout: tool_call.stdout(),
            stderr: tool_call.stderr(),
            exit_code: tool_call.exit_code(),
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: tool_call
                .requested_scope()
                .map(|s| s.workspace_root_id().as_str()),
            requested_relative_directory: tool_call
                .requested_scope()
                .map(WorkspacePathScope::relative_directory),
            effective_workspace_root_id: tool_call
                .effective_scope()
                .map(|s| s.workspace_root_id().as_str()),
            effective_relative_directory: tool_call
                .effective_scope()
                .map(WorkspacePathScope::relative_directory),
        }),
        SessionEventPayload::ToolCallStateChanged { tool_call } => Ok(RunEventColumns {
            event_type: "tool_call.state_changed",
            message_id: None,
            run_id: Some(tool_call.run_id().as_str()),
            tool_call_id: Some(tool_call.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: Some(tool_call.state().as_str()),
            capability: Some(tool_call.capability()),
            stdout: tool_call.stdout(),
            stderr: tool_call.stderr(),
            exit_code: tool_call.exit_code(),
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: tool_call
                .stdout_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
            stderr_artifact_hash: tool_call
                .stderr_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: tool_call
                .requested_scope()
                .map(|s| s.workspace_root_id().as_str()),
            requested_relative_directory: tool_call
                .requested_scope()
                .map(WorkspacePathScope::relative_directory),
            effective_workspace_root_id: tool_call
                .effective_scope()
                .map(|s| s.workspace_root_id().as_str()),
            effective_relative_directory: tool_call
                .effective_scope()
                .map(WorkspacePathScope::relative_directory),
        }),
        SessionEventPayload::ToolCallOutput {
            run_id,
            tool_call_id,
            stream,
            content,
        } => Ok(RunEventColumns {
            event_type: "tool_call.output",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: Some(tool_call_id.as_str()),
            run_state: None,
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: Some(stream.as_str()),
            output_content: Some(content.as_str()),
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: None,
            requested_relative_directory: None,
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::ArtifactRegistered {
            run_id,
            tool_call_id,
            stream,
            artifact,
        } => Ok(RunEventColumns {
            event_type: "artifact.registered",
            message_id: None,
            run_id: Some(run_id.as_str()),
            tool_call_id: Some(tool_call_id.as_str()),
            run_state: None,
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: Some(stream.as_str()),
            output_content: None,
            artifact_hash: Some(artifact.content_hash().as_str()),
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: None,
            requested_relative_directory: None,
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::ApprovalRequested { approval }
        | SessionEventPayload::ApprovalDecided { approval } => Ok(RunEventColumns {
            event_type: if matches!(
                event.payload(),
                SessionEventPayload::ApprovalRequested { .. }
            ) {
                "approval.requested"
            } else {
                "approval.decided"
            },
            message_id: None,
            run_id: Some(approval.run_id().as_str()),
            tool_call_id: Some(approval.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: None,
            capability: None,
            stdout: None,
            stderr: None,
            exit_code: None,
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: None,
            stderr_artifact_hash: None,
            approval_id: Some(approval.approval_id().as_str()),
            approval_state: Some(approval.state().as_str()),
            approval_policy: None,
            requested_workspace_root_id: Some(approval.scope().workspace_root_id().as_str()),
            requested_relative_directory: Some(approval.scope().relative_directory()),
            effective_workspace_root_id: None,
            effective_relative_directory: None,
        }),
        SessionEventPayload::ToolCallDenied { tool_call } => Ok(RunEventColumns {
            event_type: "tool_call.denied",
            message_id: None,
            run_id: Some(tool_call.run_id().as_str()),
            tool_call_id: Some(tool_call.tool_call_id().as_str()),
            run_state: None,
            tool_call_state: Some(tool_call.state().as_str()),
            capability: Some(tool_call.capability()),
            stdout: tool_call.stdout(),
            stderr: tool_call.stderr(),
            exit_code: tool_call.exit_code(),
            output_stream: None,
            output_content: None,
            artifact_hash: None,
            stdout_artifact_hash: tool_call
                .stdout_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
            stderr_artifact_hash: tool_call
                .stderr_artifact()
                .map(|artifact| artifact.content_hash().as_str()),
            approval_id: None,
            approval_state: None,
            approval_policy: None,
            requested_workspace_root_id: tool_call
                .requested_scope()
                .map(|s| s.workspace_root_id().as_str()),
            requested_relative_directory: tool_call
                .requested_scope()
                .map(WorkspacePathScope::relative_directory),
            effective_workspace_root_id: tool_call
                .effective_scope()
                .map(|s| s.workspace_root_id().as_str()),
            effective_relative_directory: tool_call
                .effective_scope()
                .map(WorkspacePathScope::relative_directory),
        }),
        SessionEventPayload::SessionCreated { .. }
        | SessionEventPayload::MessageAppended { .. }
        | SessionEventPayload::TaskCreated { .. } => Err(RunStoreError::Unavailable),
    }
}

async fn insert_run_event(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: &SessionEvent,
) -> Result<StoredSessionEvent, RunStoreError> {
    let mut query = sqlx::query(
        "INSERT INTO session_events (
            event_id, session_id, event_type, message_id, run_id, tool_call_id,
            run_state, tool_call_state, capability, stdout, stderr, exit_code,
            output_stream, output_content, approval_id, approval_state, approval_policy,
            requested_workspace_root_id, requested_relative_directory,
            effective_workspace_root_id, effective_relative_directory,
            artifact_hash, stdout_artifact_hash, stderr_artifact_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    );
    let columns = run_event_columns(event)?;
    query = query
        .bind(event.event_id().as_str())
        .bind(event.session_id().as_str())
        .bind(columns.event_type)
        .bind(columns.message_id)
        .bind(columns.run_id)
        .bind(columns.tool_call_id)
        .bind(columns.run_state)
        .bind(columns.tool_call_state)
        .bind(columns.capability)
        .bind(columns.stdout)
        .bind(columns.stderr)
        .bind(columns.exit_code)
        .bind(columns.output_stream)
        .bind(columns.output_content)
        .bind(columns.approval_id)
        .bind(columns.approval_state)
        .bind(columns.approval_policy)
        .bind(columns.requested_workspace_root_id)
        .bind(columns.requested_relative_directory)
        .bind(columns.effective_workspace_root_id)
        .bind(columns.effective_relative_directory)
        .bind(columns.artifact_hash)
        .bind(columns.stdout_artifact_hash)
        .bind(columns.stderr_artifact_hash);
    query
        .execute(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    let cursor: i64 = sqlx::query_scalar("SELECT last_insert_rowid()")
        .fetch_one(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    StoredSessionEvent::from_event(
        event,
        committed_cursor(cursor).map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)
}

async fn load_run(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &RunId,
) -> Result<Option<Run>, RunStoreError> {
    let row = sqlx::query("SELECT run_id, session_id, state, approval_policy, workspace_root_id, relative_directory FROM runs WHERE run_id = ?")
        .bind(id.as_str())
        .fetch_optional(&mut **transaction)
        .await
        .map_err(|_| RunStoreError::Unavailable)?;
    row.map(|row| {
        let run_id = RunId::parse(
            row.try_get::<String, _>("run_id")
                .map_err(|_| RunStoreError::Unavailable)?,
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        let session_id = SessionId::parse(
            row.try_get::<String, _>("session_id")
                .map_err(|_| RunStoreError::Unavailable)?,
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        let state = RunState::parse(
            row.try_get::<String, _>("state")
                .map_err(|_| RunStoreError::Unavailable)?
                .as_str(),
        )
        .map_err(|_| RunStoreError::Unavailable)?;
        let policy = row
            .try_get::<Option<String>, _>("approval_policy")
            .map_err(|_| RunStoreError::Unavailable)?
            .as_deref()
            .map(ApprovalPolicy::parse)
            .transpose()
            .map_err(|_| RunStoreError::Unavailable)?;
        let scope = parse_scope(
            row.try_get("workspace_root_id")
                .map_err(|_| RunStoreError::Unavailable)?,
            row.try_get("relative_directory")
                .map_err(|_| RunStoreError::Unavailable)?,
        )?;
        Run::from_persisted(run_id, session_id, state, policy, scope)
            .map_err(|_| RunStoreError::Unavailable)
    })
    .transpose()
}

fn parse_scope(
    workspace_root_id: Option<String>,
    relative_directory: Option<String>,
) -> Result<Option<WorkspacePathScope>, RunStoreError> {
    match (workspace_root_id, relative_directory) {
        (None, None) => Ok(None),
        (Some(root), Some(directory)) => {
            let root = WorkspaceRootId::parse(root).map_err(|_| RunStoreError::Unavailable)?;
            WorkspacePathScope::new(root, directory)
                .map(Some)
                .map_err(|_| RunStoreError::Unavailable)
        }
        _ => Err(RunStoreError::Unavailable),
    }
}

fn parse_approval(row: &sqlx::sqlite::SqliteRow) -> Result<Approval, RunStoreError> {
    let approval_id = ApprovalId::parse(
        row.try_get::<String, _>("approval_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let run_id = RunId::parse(
        row.try_get::<String, _>("approval_run_id")
            .or_else(|_| row.try_get::<String, _>("run_id"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let tool_call_id = ToolCallId::parse(
        row.try_get::<String, _>("approval_tool_call_id")
            .or_else(|_| row.try_get::<String, _>("tool_call_id"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let root = WorkspaceRootId::parse(
        row.try_get::<String, _>("approval_workspace_root_id")
            .or_else(|_| row.try_get::<String, _>("workspace_root_id"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let scope = WorkspacePathScope::new(
        root,
        row.try_get::<String, _>("approval_relative_directory")
            .or_else(|_| row.try_get::<String, _>("relative_directory"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let state = ApprovalState::parse(
        row.try_get::<String, _>("approval_state")
            .or_else(|_| row.try_get::<String, _>("state"))
            .map_err(|_| RunStoreError::Unavailable)?
            .as_str(),
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    Approval::from_persisted(approval_id, run_id, tool_call_id, scope, state)
        .map_err(|_| RunStoreError::Unavailable)
}

fn parse_tool_call(row: &sqlx::sqlite::SqliteRow) -> Result<ToolCall, RunStoreError> {
    let tool_call_id = ToolCallId::parse(
        row.try_get::<String, _>("tool_call_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let run_id = RunId::parse(
        row.try_get::<String, _>("run_id")
            .or_else(|_| row.try_get::<String, _>("tool_run_id"))
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let capability = row
        .try_get::<String, _>("capability")
        .map_err(|_| RunStoreError::Unavailable)?;
    let state = ToolCallState::parse(
        row.try_get::<String, _>("tool_state")
            .or_else(|_| row.try_get::<String, _>("state"))
            .map_err(|_| RunStoreError::Unavailable)?
            .as_str(),
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let requested_root: Option<String> = row
        .try_get("requested_workspace_root_id")
        .map_err(|_| RunStoreError::Unavailable)?;
    let requested_directory: Option<String> = row
        .try_get("requested_relative_directory")
        .map_err(|_| RunStoreError::Unavailable)?;
    let effective_root: Option<String> = row
        .try_get("effective_workspace_root_id")
        .map_err(|_| RunStoreError::Unavailable)?;
    let effective_directory: Option<String> = row
        .try_get("effective_relative_directory")
        .map_err(|_| RunStoreError::Unavailable)?;
    let stdout: Option<String> = row
        .try_get("stdout")
        .map_err(|_| RunStoreError::Unavailable)?;
    let stderr: Option<String> = row
        .try_get("stderr")
        .map_err(|_| RunStoreError::Unavailable)?;
    let stdout_artifact = parse_optional_artifact(
        row,
        "stdout_artifact_hash",
        "stdout_artifact_media_type",
        "stdout_artifact_size",
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let stderr_artifact = parse_optional_artifact(
        row,
        "stderr_artifact_hash",
        "stderr_artifact_media_type",
        "stderr_artifact_size",
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let exit_code: Option<i64> = row
        .try_get("exit_code")
        .map_err(|_| RunStoreError::Unavailable)?;
    let exit_code = exit_code
        .map(|value| i32::try_from(value).map_err(|_| RunStoreError::Unavailable))
        .transpose()?;
    ToolCall::from_persisted(PersistedToolCall {
        tool_call_id,
        run_id,
        capability,
        requested_scope: parse_scope(requested_root, requested_directory)?,
        effective_scope: parse_scope(effective_root, effective_directory)?,
        state,
        stdout,
        stderr,
        stdout_artifact,
        stderr_artifact,
        exit_code,
    })
    .map_err(|_| RunStoreError::Unavailable)
}

fn parse_run_and_tool_call(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<(Run, ToolCall), RunStoreError> {
    let run_id = RunId::parse(
        row.try_get::<String, _>("run_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let session_id = SessionId::parse(
        row.try_get::<String, _>("session_id")
            .map_err(|_| RunStoreError::Unavailable)?,
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let run_state = RunState::parse(
        row.try_get::<String, _>("state")
            .map_err(|_| RunStoreError::Unavailable)?
            .as_str(),
    )
    .map_err(|_| RunStoreError::Unavailable)?;
    let tool_call = parse_tool_call(row)?;
    let policy = row
        .try_get::<Option<String>, _>("approval_policy")
        .map_err(|_| RunStoreError::Unavailable)?
        .as_deref()
        .map(ApprovalPolicy::parse)
        .transpose()
        .map_err(|_| RunStoreError::Unavailable)?;
    let scope = parse_scope(
        row.try_get("workspace_root_id")
            .map_err(|_| RunStoreError::Unavailable)?,
        row.try_get("relative_directory")
            .map_err(|_| RunStoreError::Unavailable)?,
    )?;
    Ok((
        Run::from_persisted(run_id, session_id, run_state, policy, scope)
            .map_err(|_| RunStoreError::Unavailable)?,
        tool_call,
    ))
}

async fn load_snapshot(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &RunId,
) -> Result<Option<RunSnapshot>, RunStoreError> {
    let Some(run) = load_run(transaction, id).await? else {
        return Ok(None);
    };
    let rows = sqlx::query(
        "SELECT t.tool_call_id, t.run_id, t.capability, t.state,
                t.requested_workspace_root_id, t.requested_relative_directory,
                t.effective_workspace_root_id, t.effective_relative_directory,
                t.stdout, t.stderr, t.exit_code,
                osa.content_hash AS stdout_artifact_hash,
                osa.media_type AS stdout_artifact_media_type,
                osa.size AS stdout_artifact_size,
                esa.content_hash AS stderr_artifact_hash,
                esa.media_type AS stderr_artifact_media_type,
                esa.size AS stderr_artifact_size
         FROM tool_calls t
         LEFT JOIN artifacts osa ON osa.content_hash = t.stdout_artifact_hash
         LEFT JOIN artifacts esa ON esa.content_hash = t.stderr_artifact_hash
         WHERE t.run_id = ? ORDER BY t.rowid",
    )
    .bind(id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| RunStoreError::Unavailable)?;
    let tool_calls = rows
        .iter()
        .map(parse_tool_call)
        .collect::<Result<Vec<_>, _>>()?;
    let approval_rows = sqlx::query(
        "SELECT approval_id, run_id, tool_call_id, workspace_root_id, relative_directory, state
         FROM approvals WHERE run_id = ? ORDER BY rowid",
    )
    .bind(id.as_str())
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| RunStoreError::Unavailable)?;
    let approvals = approval_rows
        .into_iter()
        .map(|row| {
            let approval_id = ApprovalId::parse(
                row.try_get::<String, _>("approval_id")
                    .map_err(|_| RunStoreError::Unavailable)?,
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            let run_id = RunId::parse(
                row.try_get::<String, _>("run_id")
                    .map_err(|_| RunStoreError::Unavailable)?,
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            let tool_call_id = ToolCallId::parse(
                row.try_get::<String, _>("tool_call_id")
                    .map_err(|_| RunStoreError::Unavailable)?,
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            let root = WorkspaceRootId::parse(
                row.try_get::<String, _>("workspace_root_id")
                    .map_err(|_| RunStoreError::Unavailable)?,
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            let scope = WorkspacePathScope::new(
                root,
                row.try_get::<String, _>("relative_directory")
                    .map_err(|_| RunStoreError::Unavailable)?,
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            let state = ApprovalState::parse(
                row.try_get::<String, _>("state")
                    .map_err(|_| RunStoreError::Unavailable)?
                    .as_str(),
            )
            .map_err(|_| RunStoreError::Unavailable)?;
            Approval::from_persisted(approval_id, run_id, tool_call_id, scope, state)
                .map_err(|_| RunStoreError::Unavailable)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(RunSnapshot::with_approvals(
        run, tool_calls, approvals,
    )))
}

fn is_unique_constraint(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(database) if database.code().as_deref() == Some("2067"))
}

#[derive(Debug, Default, Clone, Copy)]
pub struct GitWorkspaceRootDiscovery;

impl WorkspaceRootDiscovery for GitWorkspaceRootDiscovery {
    async fn discover(&self, path: &Path) -> Result<DiscoveredWorkspaceRoot, RootDiscoveryError> {
        let metadata = std::fs::metadata(path).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                RootDiscoveryError::Missing
            } else {
                RootDiscoveryError::NotDirectory
            }
        })?;
        if !metadata.is_dir() {
            return Err(RootDiscoveryError::NotDirectory);
        }

        let bare = run_git(path, &["rev-parse", "--is-bare-repository"]).await?;
        if bare.trim() == "true" {
            return Err(RootDiscoveryError::NotGitRepository);
        }
        let top = run_git(
            path,
            &["rev-parse", "--path-format=absolute", "--show-toplevel"],
        )
        .await?;
        let common = run_git(
            path,
            &["rev-parse", "--path-format=absolute", "--git-common-dir"],
        )
        .await?;
        let canonical_path = std::fs::canonicalize(git_path(&top)?)
            .map_err(|_| RootDiscoveryError::NotGitRepository)?;
        let git_common_directory_path = std::fs::canonicalize(git_path(&common)?)
            .map_err(|_| RootDiscoveryError::NotGitRepository)?;
        let filesystem_identity = workspace_root_filesystem_identity(&canonical_path)
            .map_err(|_| RootDiscoveryError::NotGitRepository)?;
        Ok(DiscoveredWorkspaceRoot {
            canonical_path: canonical_path
                .to_str()
                .ok_or(RootDiscoveryError::NotGitRepository)?
                .to_owned(),
            git_common_directory_path: git_common_directory_path
                .to_str()
                .ok_or(RootDiscoveryError::NotGitRepository)?
                .to_owned(),
            filesystem_identity,
        })
    }
}

async fn run_git(path: &Path, args: &[&str]) -> Result<String, RootDiscoveryError> {
    let mut command = Command::new("git");
    command.arg("-C").arg(path).args(args);
    for variable in [
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_DIR",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_WORK_TREE",
    ] {
        command.env_remove(variable);
    }
    let output = command.output().await.map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            RootDiscoveryError::GitUnavailable
        } else {
            RootDiscoveryError::NotGitRepository
        }
    })?;
    if !output.status.success() {
        return Err(RootDiscoveryError::NotGitRepository);
    }
    String::from_utf8(output.stdout).map_err(|_| RootDiscoveryError::NotGitRepository)
}

fn git_path(output: &str) -> Result<PathBuf, RootDiscoveryError> {
    let output = output.strip_suffix('\n').unwrap_or(output);
    let output = output.strip_suffix('\r').unwrap_or(output);
    if output.is_empty() {
        return Err(RootDiscoveryError::NotGitRepository);
    }
    Ok(PathBuf::from(output))
}

pub fn data_directory() -> Option<PathBuf> {
    if let Some(path) = env::var_os("KILN_DATA_DIR") {
        return Some(PathBuf::from(path));
    }
    ProjectDirs::from("", "", "kiln").map(|directories| directories.data_dir().to_path_buf())
}

#[derive(Debug, Default, Clone, Copy)]
pub struct UlidIdGenerator;

impl WorkspaceIdGenerator for UlidIdGenerator {
    fn workspace_id(&self) -> WorkspaceId {
        WorkspaceId::from_ulid(Ulid::generate())
    }
    fn workspace_root_id(&self) -> WorkspaceRootId {
        WorkspaceRootId::from_ulid(Ulid::generate())
    }
}

impl SessionIdGenerator for UlidIdGenerator {
    fn session_id(&self) -> SessionId {
        SessionId::from_ulid(Ulid::generate())
    }

    fn message_id(&self) -> MessageId {
        MessageId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

impl TaskIdGenerator for UlidIdGenerator {
    fn task_id(&self) -> TaskId {
        TaskId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

impl RunIdGenerator for UlidIdGenerator {
    fn run_id(&self) -> RunId {
        RunId::from_ulid(Ulid::generate())
    }

    fn tool_call_id(&self) -> ToolCallId {
        ToolCallId::from_ulid(Ulid::generate())
    }

    fn approval_id(&self) -> ApprovalId {
        ApprovalId::from_ulid(Ulid::generate())
    }

    fn event_id(&self) -> EventId {
        EventId::from_ulid(Ulid::generate())
    }
}

#[cfg(test)]
mod tests;
