use std::{io::Read, num::NonZeroUsize};

use kiln_core::Artifact;
use sha2::{Digest, Sha256};

use super::{ArtifactStoreError, FileArtifactStore, artifact_hash_bytes, open_artifact_file};

#[derive(Debug)]
pub enum ArtifactPageError {
    InvalidOffset,
    LimitExceeded,
    Store(ArtifactStoreError),
}

impl From<std::io::Error> for ArtifactPageError {
    fn from(error: std::io::Error) -> Self {
        Self::Store(ArtifactStoreError::Filesystem(error))
    }
}

impl FileArtifactStore {
    /// Read a byte page from a fully verified artifact without retaining the whole file.
    ///
    /// This is a synchronous storage primitive, not an authorization boundary. Callers
    /// must resolve trusted metadata and ownership first and run it off async executors.
    /// `max_artifact_bytes` bounds the full integrity scan; `max_page_bytes` bounds the
    /// returned allocation. Pages are raw bytes and may split UTF-8 code points.
    /// An offset at EOF returns an empty page; an offset beyond EOF is invalid.
    /// Missing files return `None`; no bytes are returned on integrity failure.
    pub fn read_page(
        &self,
        artifact: &Artifact,
        offset: u64,
        max_page_bytes: NonZeroUsize,
        max_artifact_bytes: u64,
    ) -> Result<Option<Vec<u8>>, ArtifactPageError> {
        if artifact.size() > max_artifact_bytes {
            return Err(ArtifactPageError::LimitExceeded);
        }
        if offset > artifact.size() {
            return Err(ArtifactPageError::InvalidOffset);
        }
        let page_length = (artifact.size() - offset).min(
            u64::try_from(max_page_bytes.get()).map_err(|_| ArtifactPageError::LimitExceeded)?,
        );
        let end = offset + page_length;
        let path = self
            .bucket(artifact.content_hash())
            .join(artifact.content_hash().as_str());
        let mut file = match open_artifact_file(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let corrupt = || ArtifactPageError::Store(ArtifactStoreError::Corrupt);
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() != artifact.size() {
            return Err(corrupt());
        }
        let mut page = Vec::new();
        page.try_reserve_exact(
            usize::try_from(page_length).map_err(|_| ArtifactPageError::LimitExceeded)?,
        )
        .map_err(|_| ArtifactPageError::LimitExceeded)?;
        let mut hasher = Sha256::new();
        // Match the existing artifact verifier's fixed scratch-space budget.
        let mut buffer = [0_u8; 64 * 1_024];
        let mut position = 0_u64;
        while position < artifact.size() {
            let wanted = (artifact.size() - position).min(buffer.len() as u64) as usize;
            let read = file.read(&mut buffer[..wanted])?;
            if read == 0 {
                return Err(corrupt());
            }
            let next = position + read as u64;
            hasher.update(&buffer[..read]);
            let from = position.max(offset);
            let to = next.min(end);
            if from < to {
                page.extend_from_slice(
                    &buffer[(from - position) as usize..(to - position) as usize],
                );
            }
            position = next;
        }
        // Detect growth without allowing an unbounded read of a changing file.
        if file.read(&mut buffer[..1])? != 0 {
            return Err(corrupt());
        }
        if hasher.finalize().as_slice()
            != artifact_hash_bytes(artifact.content_hash()).ok_or_else(corrupt)?
        {
            return Err(corrupt());
        }
        // The returned bytes came from the very scan that was hashed: there is no
        // verify-then-seek window in which a second read could return different bytes.
        Ok(Some(page))
    }
}
