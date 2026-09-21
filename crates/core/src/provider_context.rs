use std::future::Future;

use sha2::{Digest, Sha256};

use crate::{
    Artifact, CONTEXT_MANIFEST_ATTACHMENT_ENCODING_VERSION, ChildActivityReference, ContentHash,
    ContextInstructionProvenance, ContextManifestEntry, ContextManifestId, MessageId, MessageRole,
    ProviderRequest, RunId, SessionId,
};

/// Resource ceilings supplied by the invoking adapter. These are byte budgets,
/// not token estimates or a claim about a model's context window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderContextLimits {
    pub max_text_bytes: u64,
    pub max_attachment_bytes: u64,
    pub max_total_attachment_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderContextError {
    LegacyManifest,
    InvalidLimits,
    TextLimitExceeded,
    AttachmentLimitExceeded,
    ArtifactNotFound,
    ArtifactUnavailable,
    ArtifactCorrupt,
    InvalidManifest,
}

/// Implementations must bound the read by the expected artifact size and the
/// caller's ceiling before allocating. Assembly independently checks size/hash.
pub trait ProviderContextArtifactReader: Send + Sync {
    fn read_context_artifact(
        &self,
        artifact: &Artifact,
        max_bytes: u64,
    ) -> impl Future<Output = Result<Vec<u8>, ProviderContextError>> + Send;
}

/// No Debug: context content and attachment bytes are model input, not diagnostics.
pub struct ProviderContextAttachment {
    artifact: Artifact,
    bytes: Vec<u8>,
}

impl ProviderContextAttachment {
    pub fn artifact(&self) -> &Artifact {
        &self.artifact
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

pub enum ProviderContextEntry {
    Instruction {
        provenance: ContextInstructionProvenance,
        content: String,
    },
    Message {
        message_id: MessageId,
        role: MessageRole,
        content: String,
        attachments: Vec<ProviderContextAttachment>,
    },
    ChildActivity {
        reaction_message_id: MessageId,
        reference: ChildActivityReference,
        content: String,
    },
}

/// Fully assembled input in the original manifest order. No tool is executed
/// during assembly, and no provider-specific upload/reference is manufactured.
pub struct ProviderContext {
    manifest_id: ContextManifestId,
    manifest_hash: ContentHash,
    session_id: SessionId,
    run_id: RunId,
    entries: Vec<ProviderContextEntry>,
}

impl ProviderContext {
    pub fn manifest_id(&self) -> &ContextManifestId {
        &self.manifest_id
    }
    pub fn manifest_hash(&self) -> &ContentHash {
        &self.manifest_hash
    }
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    pub fn entries(&self) -> &[ProviderContextEntry] {
        &self.entries
    }
}

impl ProviderRequest {
    /// Assemble only a complete versioned snapshot. Dropping this future stops
    /// further reads and discards partial assembly; it does not publish output.
    pub async fn assemble_context(
        &self,
        reader: &impl ProviderContextArtifactReader,
        limits: ProviderContextLimits,
    ) -> Result<ProviderContext, ProviderContextError> {
        let manifest = self.manifest();
        if manifest.encoding_version() != CONTEXT_MANIFEST_ATTACHMENT_ENCODING_VERSION {
            return Err(ProviderContextError::LegacyManifest);
        }
        if limits.max_text_bytes == 0
            || limits.max_attachment_bytes == 0
            || limits.max_total_attachment_bytes == 0
        {
            return Err(ProviderContextError::InvalidLimits);
        }
        // Preflight the entire snapshot before any artifact read. Do not trim
        // input, silently omit attachments, or return a partially assembled input.
        let mut text_bytes = 0_u64;
        for entry in manifest.entries() {
            text_bytes = text_bytes
                .checked_add(entry.content().len() as u64)
                .ok_or(ProviderContextError::TextLimitExceeded)?;
            if text_bytes > limits.max_text_bytes {
                return Err(ProviderContextError::TextLimitExceeded);
            }
        }
        let mut attachment_bytes = 0_u64;
        for attachment in manifest.attachments() {
            if attachment.artifact().size() > limits.max_attachment_bytes {
                return Err(ProviderContextError::AttachmentLimitExceeded);
            }
            attachment_bytes = attachment_bytes
                .checked_add(attachment.artifact().size())
                .ok_or(ProviderContextError::AttachmentLimitExceeded)?;
            if attachment_bytes > limits.max_total_attachment_bytes {
                return Err(ProviderContextError::AttachmentLimitExceeded);
            }
        }

        let mut attachments = manifest.attachments().iter().peekable();
        let mut entries = Vec::with_capacity(manifest.entries().len());
        for entry in manifest.entries() {
            entries.push(match entry {
                ContextManifestEntry::Instruction {
                    provenance,
                    content,
                } => ProviderContextEntry::Instruction {
                    provenance: provenance.clone(),
                    content: content.clone(),
                },
                ContextManifestEntry::MessageSnapshot {
                    message_id,
                    role,
                    content,
                } => {
                    let mut payloads = Vec::new();
                    while attachments
                        .peek()
                        .is_some_and(|attachment| attachment.message_id() == message_id)
                    {
                        let attachment = attachments
                            .next()
                            .ok_or(ProviderContextError::InvalidManifest)?;
                        let artifact = attachment.artifact();
                        let bytes = reader
                            .read_context_artifact(artifact, limits.max_attachment_bytes)
                            .await?;
                        if !artifact_bytes_match(artifact, &bytes) {
                            return Err(ProviderContextError::ArtifactCorrupt);
                        }
                        payloads.push(ProviderContextAttachment {
                            artifact: artifact.clone(),
                            bytes,
                        });
                    }
                    ProviderContextEntry::Message {
                        message_id: message_id.clone(),
                        role: *role,
                        content: content.clone(),
                        attachments: payloads,
                    }
                }
                ContextManifestEntry::ChildActivitySnapshot {
                    reaction_message_id,
                    reference,
                    content,
                } => ProviderContextEntry::ChildActivity {
                    reaction_message_id: reaction_message_id.clone(),
                    reference: reference.clone(),
                    content: content.clone(),
                },
            });
        }
        if attachments.next().is_some() {
            return Err(ProviderContextError::InvalidManifest);
        }
        Ok(ProviderContext {
            manifest_id: manifest.context_manifest_id().clone(),
            manifest_hash: manifest.content_hash().clone(),
            session_id: manifest.session_id().clone(),
            run_id: manifest.run_id().clone(),
            entries,
        })
    }
}

fn artifact_bytes_match(artifact: &Artifact, bytes: &[u8]) -> bool {
    if bytes.len() as u64 != artifact.size() {
        return false;
    }
    let expected = artifact.content_hash().as_str().as_bytes();
    let hex = b"0123456789abcdef";
    Sha256::digest(bytes)
        .iter()
        .enumerate()
        .all(|(index, byte)| {
            expected[index * 2] == hex[(byte >> 4) as usize]
                && expected[index * 2 + 1] == hex[(byte & 0x0f) as usize]
        })
}
