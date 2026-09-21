use kiln_core::{Artifact, ProviderContextArtifactReader, ProviderContextError};

use super::{FileArtifactStore, Read, open_artifact_file};

impl ProviderContextArtifactReader for FileArtifactStore {
    async fn read_context_artifact(
        &self,
        artifact: &Artifact,
        max_bytes: u64,
    ) -> Result<Vec<u8>, ProviderContextError> {
        if artifact.size() > max_bytes {
            return Err(ProviderContextError::AttachmentLimitExceeded);
        }
        let path = self
            .bucket(artifact.content_hash())
            .join(artifact.content_hash().as_str());
        let expected_size = artifact.size();
        // Filesystem work is off the async executor. A dropped waiter can leave
        // this single bounded read finishing, but cannot start another read.
        tokio::task::spawn_blocking(move || {
            let file = open_artifact_file(&path).map_err(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    ProviderContextError::ArtifactNotFound
                } else {
                    ProviderContextError::ArtifactUnavailable
                }
            })?;
            let metadata = file
                .metadata()
                .map_err(|_| ProviderContextError::ArtifactUnavailable)?;
            if !metadata.is_file() || metadata.len() != expected_size {
                return Err(ProviderContextError::ArtifactCorrupt);
            }
            // The extra byte detects growth after metadata inspection. It is
            // discarded on failure and never exceeds the snapshot size + 1.
            let read_limit = expected_size
                .checked_add(1)
                .ok_or(ProviderContextError::AttachmentLimitExceeded)?;
            let mut bytes = Vec::new();
            file.take(read_limit)
                .read_to_end(&mut bytes)
                .map_err(|_| ProviderContextError::ArtifactUnavailable)?;
            if bytes.len() as u64 != expected_size {
                return Err(ProviderContextError::ArtifactCorrupt);
            }
            Ok(bytes)
        })
        .await
        .map_err(|_| ProviderContextError::ArtifactUnavailable)?
    }
}
