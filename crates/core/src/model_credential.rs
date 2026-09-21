use std::fmt;

use crate::{ModelInvocationId, SecretRef, WorkspaceId};

/// Private credential-version snapshot, separate from public invocation Events.
/// An opaque reference is not credential material or proof of principal continuity.
#[derive(Clone, PartialEq, Eq)]
pub struct ModelInvocationCredential {
    invocation_id: ModelInvocationId,
    workspace_id: WorkspaceId,
    secret_ref: SecretRef,
}

impl ModelInvocationCredential {
    pub fn new(
        invocation_id: ModelInvocationId,
        workspace_id: WorkspaceId,
        secret_ref: SecretRef,
    ) -> Self {
        Self {
            invocation_id,
            workspace_id,
            secret_ref,
        }
    }
    pub fn invocation_id(&self) -> &ModelInvocationId {
        &self.invocation_id
    }
    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.workspace_id
    }
    pub fn secret_ref(&self) -> &SecretRef {
        &self.secret_ref
    }
    pub fn same_version(&self, other: &Self) -> bool {
        self.workspace_id == other.workspace_id && self.secret_ref == other.secret_ref
    }
}

impl fmt::Debug for ModelInvocationCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModelInvocationCredential")
            .field("invocation_id", &self.invocation_id)
            .finish_non_exhaustive()
    }
}
