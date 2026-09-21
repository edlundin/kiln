use std::future::Future;

use kiln_core::{
    ModelToolExecutionRequest, RunError, SubprocessOutput, SubprocessRequest, ToolCallResult,
    ToolCallState, WorkspaceFileReadCommand, WorkspaceRoot,
};

use super::FileArtifactStore;

/// Read-only local execution after a fresh native claim. The caller publishes
/// the result through the native completion store. No Run state changes here.
pub async fn execute_workspace_file_read<C: Future<Output = ()> + Send>(
    request: ModelToolExecutionRequest<WorkspaceFileReadCommand>,
    root: &WorkspaceRoot,
    artifacts: FileArtifactStore,
    cancellation: C,
) -> Result<ToolCallResult, RunError> {
    tokio::pin!(cancellation);
    tokio::select! {
        biased;
        _ = &mut cancellation => return cancelled(),
        _ = std::future::ready(()) => {}
    }
    if request.scope().workspace_root_id() != root.id()
        || request.tool_call().capability() != kiln_core::WORKSPACE_FILE_READ_CAPABILITY
    {
        return failed(FileReadFailure::Scope);
    }
    let scope = SubprocessRequest::new(
        root.canonical_path().to_owned(),
        root.filesystem_identity().clone(),
        request.scope().clone(),
    )?;
    let mut operation =
        tokio::task::spawn_blocking(move || read_file(request.command(), &scope, &artifacts));
    tokio::select! {
        biased;
        _ = &mut cancellation => {
            // A started filesystem read cannot be safely detached then declared
            // stopped. Join it before reporting cancellation; no wall-clock
            // latency guarantee is made for blocked filesystem I/O.
            let _ = operation.await;
            cancelled()
        }
        result = &mut operation => match result {
            Ok(Ok(output)) => ToolCallResult::from_subprocess(ToolCallState::Completed, output),
            Ok(Err(error)) => failed(error),
            Err(_) => failed(FileReadFailure::Unavailable),
        }
    }
}

fn cancelled() -> Result<ToolCallResult, RunError> {
    ToolCallResult::cancelled(SubprocessOutput::success("", "", 0))
}

fn failed(error: FileReadFailure) -> Result<ToolCallResult, RunError> {
    let message = match error {
        FileReadFailure::Scope => "The approved Workspace directory is unavailable.",
        FileReadFailure::Unavailable => "The requested regular file could not be read safely.",
        #[cfg(unix)]
        FileReadFailure::TooLarge => "The requested file exceeds the configured byte limit.",
        #[cfg(unix)]
        FileReadFailure::NotUtf8 => "The requested file is not valid UTF-8.",
        #[cfg(unix)]
        FileReadFailure::Artifact => "The file output could not be stored as an artifact.",
        #[cfg(not(unix))]
        FileReadFailure::UnsupportedPlatform => {
            "Scoped file reading is unavailable on this platform."
        }
    };
    ToolCallResult::new(ToolCallState::Failed, String::new(), message.into(), None)
}

enum FileReadFailure {
    Scope,
    Unavailable,
    #[cfg(unix)]
    TooLarge,
    #[cfg(unix)]
    NotUtf8,
    #[cfg(unix)]
    Artifact,
    #[cfg(not(unix))]
    UnsupportedPlatform,
}

#[cfg(unix)]
fn read_file(
    command: &WorkspaceFileReadCommand,
    scope: &SubprocessRequest,
    artifacts: &FileArtifactStore,
) -> Result<SubprocessOutput, FileReadFailure> {
    use rustix::fs::{Mode, OFlags, fstat, openat};
    use std::io::Read;

    let mut directory =
        super::pin_subprocess_directory(scope).map_err(|_| FileReadFailure::Scope)?;
    let device = fstat(&directory)
        .map_err(|_| FileReadFailure::Scope)?
        .st_dev;
    let components = command.relative_path().split('/').collect::<Vec<_>>();
    let (file_name, parents) = components
        .split_last()
        .ok_or(FileReadFailure::Unavailable)?;
    for component in parents {
        let next = openat(
            &directory,
            *component,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| FileReadFailure::Unavailable)?;
        if fstat(&next)
            .map_err(|_| FileReadFailure::Unavailable)?
            .st_dev
            != device
        {
            return Err(FileReadFailure::Unavailable);
        }
        directory = next;
    }
    let fd = openat(
        &directory,
        *file_name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| FileReadFailure::Unavailable)?;
    if fstat(&fd).map_err(|_| FileReadFailure::Unavailable)?.st_dev != device {
        return Err(FileReadFailure::Unavailable);
    }
    let file = std::fs::File::from(fd);
    let metadata = file.metadata().map_err(|_| FileReadFailure::Unavailable)?;
    if !metadata.is_file() {
        return Err(FileReadFailure::Unavailable);
    }
    let limit = u64::try_from(command.max_file_bytes()).map_err(|_| FileReadFailure::TooLarge)?;
    if metadata.len() > limit {
        return Err(FileReadFailure::TooLarge);
    }
    let mut bytes = Vec::new();
    file.take(limit.checked_add(1).ok_or(FileReadFailure::TooLarge)?)
        .read_to_end(&mut bytes)
        .map_err(|_| FileReadFailure::Unavailable)?;
    if bytes.len() > command.max_file_bytes() {
        return Err(FileReadFailure::TooLarge);
    }
    let text = String::from_utf8(bytes).map_err(|_| FileReadFailure::NotUtf8)?;
    let mut output = SubprocessOutput::success("", "", 0);
    if text.len() > kiln_core::INLINE_TOOL_OUTPUT_LIMIT {
        output.stdout_artifact = Some(
            artifacts
                .store(text.as_bytes(), kiln_core::TOOL_OUTPUT_MEDIA_TYPE)
                .map_err(|_| FileReadFailure::Artifact)?,
        );
    } else {
        output.stdout = text;
    }
    Ok(output)
}

#[cfg(not(unix))]
fn read_file(
    _: &WorkspaceFileReadCommand,
    _: &SubprocessRequest,
    _: &FileArtifactStore,
) -> Result<SubprocessOutput, FileReadFailure> {
    Err(FileReadFailure::UnsupportedPlatform)
}
