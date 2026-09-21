use kiln_core::{Artifact, ProviderContextArtifactReader, ProviderContextError};

use super::{FileArtifactStore, Read, open_artifact_file};

/// Read verified private replay and file artifacts for one provider context.
pub struct StoredProviderContextReader {
    store: super::SqliteStore,
    artifacts: FileArtifactStore,
}

impl StoredProviderContextReader {
    pub fn new(store: super::SqliteStore, artifacts: FileArtifactStore) -> Self {
        Self { store, artifacts }
    }
}

impl ProviderContextArtifactReader for StoredProviderContextReader {
    async fn read_context_artifact(
        &self,
        artifact: &Artifact,
        max_bytes: u64,
    ) -> Result<Vec<u8>, ProviderContextError> {
        self.artifacts
            .read_context_artifact(artifact, max_bytes)
            .await
    }
}

impl kiln_core::ProviderContextContinuationReader for StoredProviderContextReader {
    async fn read_context_continuation(
        &self,
        reference: &kiln_core::ModelContinuationReference,
        max_bytes: u64,
    ) -> Result<kiln_core::ModelInvocationContinuation, ProviderContextError> {
        use kiln_core::{ModelContinuationError, ModelContinuationLimits, ModelContinuationStore};

        if reference.payload_size() > max_bytes {
            return Err(ProviderContextError::ContinuationLimitExceeded);
        }
        let continuation =
            self.store
                .get_model_continuation(
                    reference.invocation_id(),
                    ModelContinuationLimits {
                        max_format_bytes: reference.format().len(),
                        max_payload_bytes: usize::try_from(reference.payload_size())
                            .map_err(|_| ProviderContextError::ContinuationLimitExceeded)?,
                    },
                )
                .await
                .map_err(|error| match error {
                    ModelContinuationError::Unavailable
                    | ModelContinuationError::Invocation(
                        kiln_core::ModelInvocationStoreError::Unavailable,
                    )
                    | ModelContinuationError::Requests(
                        kiln_core::ModelToolRequestError::Unavailable,
                    ) => ProviderContextError::ContinuationUnavailable,
                    ModelContinuationError::LimitExceeded => {
                        ProviderContextError::ContinuationCorrupt
                    }
                    _ => ProviderContextError::ContinuationCorrupt,
                })?
                .ok_or(ProviderContextError::ContinuationUnavailable)?;
        if !reference.matches(&continuation) {
            return Err(ProviderContextError::ContinuationCorrupt);
        }
        Ok(continuation)
    }
}

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
