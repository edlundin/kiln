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

/// A vault value obtained through the expected-version/account/workspace guard.
/// It is non-cloneable and has no public constructor or serialization.
pub struct ResolvedModelCredential {
    binding: ModelInvocationCredential,
    account_id: crate::ProviderAccountId,
    provider: crate::ProviderType,
    secret: crate::SecretValue,
}

impl ResolvedModelCredential {
    pub(crate) fn new(
        request: &crate::ProviderRequest,
        binding: ModelInvocationCredential,
        secret: crate::SecretValue,
    ) -> Self {
        Self {
            binding,
            account_id: request.invocation().provider_account_id().clone(),
            provider: request.invocation().settings().provider().clone(),
            secret,
        }
    }
    pub fn matches(&self, request: &crate::ProviderRequest) -> bool {
        request.credential() == Some(&self.binding)
            && request.invocation().provider_account_id() == &self.account_id
            && request.invocation().settings().provider() == &self.provider
    }
    pub fn into_secret(self) -> crate::SecretValue {
        self.secret
    }
}

impl fmt::Debug for ResolvedModelCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedModelCredential(<redacted>)")
    }
}
