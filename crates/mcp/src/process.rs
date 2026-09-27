use std::{
    collections::BTreeMap, ffi::OsString, io, num::NonZeroUsize, path::PathBuf, process::Stdio,
    time::Duration,
};

use rmcp::{
    RoleClient,
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::Transport,
};
use rustix::{
    io::Errno,
    process::{Pid, Signal, kill_process_group},
};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::StdioTransport;

/// Resolved local execution inputs, never portable synchronized configuration.
/// The caller must first authorize the executable, directory, and credentials
/// for the owning scope. No Debug implementation exposes arguments or secrets.
pub struct StdioProcessConfig {
    pub executable: PathBuf,
    pub arguments: Vec<OsString>,
    /// A directory descriptor pinned and authorized by the caller. Moving or
    /// replacing its former path cannot redirect the launched process.
    pub working_directory: rustix::fd::OwnedFd,
    pub environment: BTreeMap<OsString, OsString>,
    pub max_frame_bytes: NonZeroUsize,
    /// Time allowed after stdin closes before forced process-group cleanup.
    /// Zero requests immediate cleanup. There is no product-wide default.
    pub shutdown_grace: Duration,
}

/// Owns one Unix process generation and its stdio. Drop forcibly signals its
/// process group; explicit close also waits for the direct child to be reaped.
/// Descendants that escape the group or change credentials require an OS sandbox.
pub struct StdioProcess {
    transport: StdioTransport<ChildStdout, ChildStdin>,
    child: Option<Child>,
    group: Option<Pid>,
    shutdown_grace: Duration,
    exit_status: Option<std::process::ExitStatus>,
}

impl StdioProcess {
    pub fn spawn(config: StdioProcessConfig) -> io::Result<Self> {
        if !config.executable.is_absolute() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MCP executable must be absolute",
            ));
        }
        let mut command = Command::new(config.executable);
        let directory = config.working_directory;
        rustix::io::fcntl_setfd(&directory, rustix::io::FdFlags::CLOEXEC)?;
        command
            .args(config.arguments)
            .env_clear()
            .envs(config.environment)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        // SAFETY: the post-fork closure only performs fchdir on an owned,
        // pre-opened descriptor. It allocates nothing and takes no locks.
        unsafe {
            command.pre_exec(move || rustix::process::fchdir(&directory).map_err(io::Error::from));
        }
        let mut child = command.spawn()?;
        let group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .and_then(Pid::from_raw)
            .filter(|pid| pid.as_raw_nonzero().get() > 1)
            .ok_or_else(|| io::Error::other("MCP child process ID unavailable"))?;
        // These handles are created by the command above; neither was exposed
        // to another owner between spawn and extraction.
        let stdout = child.stdout.take().expect("piped MCP stdout");
        let stdin = child.stdin.take().expect("piped MCP stdin");
        Ok(Self {
            transport: StdioTransport::new(stdout, stdin, config.max_frame_bytes),
            child: Some(child),
            group: Some(group),
            shutdown_grace: config.shutdown_grace,
            exit_status: None,
        })
    }

    pub fn process_id(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }

    pub fn exit_status(&self) -> Option<std::process::ExitStatus> {
        self.exit_status
    }

    fn stop_group(&mut self) -> io::Result<()> {
        let Some(group) = self.group else {
            return Ok(());
        };
        loop {
            match kill_process_group(group, Signal::KILL) {
                Ok(()) | Err(Errno::SRCH) => {
                    self.group = None;
                    return Ok(());
                }
                Err(Errno::INTR) => continue,
                #[cfg(target_os = "macos")]
                Err(Errno::PERM) if exited_leader(group) => {
                    // XNU killpg1 excludes zombies and returns EPERM when no
                    // member can be signalled. Confirm our leader has exited
                    // without reaping it. Credential-changing descendants are
                    // outside this same-credential process-group boundary.
                    self.group = None;
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[cfg(target_os = "macos")]
fn exited_leader(pid: Pid) -> bool {
    use rustix::process::{WaitId, WaitIdOptions, waitid};
    matches!(
        waitid(
            WaitId::Pid(pid),
            WaitIdOptions::EXITED | WaitIdOptions::NOHANG | WaitIdOptions::NOWAIT
        ),
        Ok(Some(_))
    )
}

impl Drop for StdioProcess {
    fn drop(&mut self) {
        // Signal before Child is dropped/reaped, while the leader's numeric
        // PID cannot be reused. Tokio's kill_on_drop is the direct-child backstop.
        let _ = self.stop_group();
    }
}

impl Transport<RoleClient> for StdioProcess {
    type Error = io::Error;

    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = io::Result<()>> + Send + 'static {
        self.transport.send(message)
    }

    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        self.transport.receive().await
    }

    async fn close(&mut self) -> io::Result<()> {
        if self.child.is_none() {
            return Ok(());
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(self.shutdown_grace)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "MCP shutdown grace is out of range",
                )
            })?;
        let close_result = tokio::time::timeout_at(deadline, self.transport.close())
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "MCP stdin shutdown timed out",
                ))
            });
        if close_result.is_ok() && !self.shutdown_grace.is_zero() {
            // Keep the leader unreaped until group cleanup. Waiting for and
            // reaping it first could let its PID be reused before group kill.
            tokio::time::sleep_until(deadline).await;
        }
        self.stop_group()?;
        if let Some(mut child) = self.child.take() {
            self.exit_status = Some(child.wait().await?);
        }
        close_result
    }
}
