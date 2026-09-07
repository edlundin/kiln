use std::fs::TryLockError;

use super::*;

pub struct DaemonStoreLock {
    _file: fs::File,
}

impl DaemonStoreLock {
    pub fn open_default() -> Result<Self, InfrastructureError> {
        let data_dir = data_directory().ok_or(InfrastructureError::DataDirectoryUnavailable)?;
        fs::create_dir_all(&data_dir).map_err(InfrastructureError::Filesystem)?;
        let auth_dir = data_dir.join(AUTH_DIRECTORY);
        ensure_private_directory(&auth_dir).map_err(InfrastructureError::Filesystem)?;
        let file = open_lock_file(&auth_dir).map_err(InfrastructureError::Filesystem)?;
        let metadata = file.metadata().map_err(InfrastructureError::Filesystem)?;
        if !metadata.file_type().is_file() || !is_private(&metadata) {
            return Err(InfrastructureError::Filesystem(insecure_auth_storage()));
        }
        file.try_lock().map_err(|error| {
            InfrastructureError::Filesystem(match error {
                TryLockError::WouldBlock => {
                    io::Error::new(ErrorKind::WouldBlock, "another daemon owns this store")
                }
                TryLockError::Error(error) => error,
            })
        })?;
        Ok(Self { _file: file })
    }
}

#[cfg(unix)]
fn open_lock_file(auth_dir: &Path) -> io::Result<fs::File> {
    let directory = open_private_directory(auth_dir)?;
    openat(
        &directory,
        "daemon.lock",
        OFlags::RDWR | OFlags::CREATE | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::RUSR | Mode::WUSR,
    )
    .map(fs::File::from)
    .map_err(io::Error::from)
}

#[cfg(not(unix))]
fn open_lock_file(auth_dir: &Path) -> io::Result<fs::File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(auth_dir.join("daemon.lock"))
}
