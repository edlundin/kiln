use std::sync::Arc;

use kiln_core::{
    ModelProvider, ProviderAccountApplication, ProviderAccountError, ProviderAccountIdGenerator,
    ProviderAccountStore, ProviderContextArtifactReader, ProviderContextContinuationReader,
    ProviderContextError, ProviderContextLimits, ProviderError, ProviderRequest, SecretStore,
    SecretStoreError,
};

use crate::{
    OPENAI_API_PROVIDER_TYPE, OpenAiApiOperation, OpenAiApiTransport, OpenAiApiTransportError,
    OpenAiApiTransportLimits,
};

/// Public API provider-port composition. Account lifecycle/vault instances must
/// be shared with account management so version checks use the same locks.
/// Construction and start perform no model HTTP request; the returned operation
/// sends only when polled. Registration, model selection and capabilities remain
/// explicit caller responsibilities, not provider-derived execution authority.
pub struct OpenAiApiModelProvider<S, I, V, C> {
    accounts: Arc<ProviderAccountApplication<S, I>>,
    vault: Arc<V>,
    context_reader: C,
    context_limits: ProviderContextLimits,
    transport: OpenAiApiTransport,
}

impl<S, I, V, C> OpenAiApiModelProvider<S, I, V, C> {
    pub fn new(
        accounts: Arc<ProviderAccountApplication<S, I>>,
        vault: Arc<V>,
        context_reader: C,
        context_limits: ProviderContextLimits,
        transport_limits: OpenAiApiTransportLimits,
    ) -> Result<Self, OpenAiApiTransportError> {
        if context_limits.max_text_bytes == 0
            || context_limits.max_attachment_bytes == 0
            || context_limits.max_total_attachment_bytes == 0
            || context_limits.max_continuation_bytes == 0
            || context_limits.max_total_continuation_bytes == 0
        {
            return Err(OpenAiApiTransportError::InvalidLimits);
        }
        Ok(Self {
            accounts,
            vault,
            context_reader,
            context_limits,
            transport: OpenAiApiTransport::new(transport_limits)?,
        })
    }
}

impl<S, I, V, C> ModelProvider for OpenAiApiModelProvider<S, I, V, C>
where
    S: ProviderAccountStore,
    I: ProviderAccountIdGenerator,
    V: SecretStore,
    C: ProviderContextArtifactReader + ProviderContextContinuationReader,
{
    type Operation = OpenAiApiOperation;

    async fn start(&self, request: ProviderRequest) -> Result<Self::Operation, ProviderError> {
        if request.invocation().settings().provider().as_str() != OPENAI_API_PROVIDER_TYPE {
            return Err(ProviderError::ModelUnavailable);
        }
        if request.credential().is_none() {
            return Err(ProviderError::ProviderAccountMismatch);
        }
        let context = request
            .assemble_context(&self.context_reader, self.context_limits)
            .await
            .map_err(|error| match error {
                ProviderContextError::ArtifactUnavailable
                | ProviderContextError::ContinuationUnavailable => {
                    ProviderError::ProviderUnavailable
                }
                _ => ProviderError::ProviderResponseInvalid,
            })?;
        // Validate all input/formats/settings and encode once before vault access.
        // The typed body binds to this immutable invocation and is moved into HTTP.
        let body = self
            .transport
            .encode(&request, &context)
            .map_err(transport_error)?;
        drop(context);
        let credential = self
            .accounts
            .read_model_credential(self.vault.as_ref(), &request)
            .await
            .map_err(account_error)?;
        self.transport
            .prepare_encoded(request, body, credential)
            .map_err(transport_error)
    }
}

fn account_error(error: ProviderAccountError) -> ProviderError {
    match error {
        ProviderAccountError::AccountNotFound
        | ProviderAccountError::AccountNotConnected
        | ProviderAccountError::SecretRefRequired
        | ProviderAccountError::CredentialStore(SecretStoreError::NotFound) => {
            ProviderError::AuthenticationRequired
        }
        ProviderAccountError::ProviderTypeMismatch
        | ProviderAccountError::WorkspaceAssociationMismatch
        | ProviderAccountError::CredentialVersionConflict
        | ProviderAccountError::CredentialStore(SecretStoreError::ProviderBindingMismatch) => {
            ProviderError::ProviderAccountMismatch
        }
        _ => ProviderError::ProviderUnavailable,
    }
}

fn transport_error(error: OpenAiApiTransportError) -> ProviderError {
    match error {
        OpenAiApiTransportError::CredentialMismatch => ProviderError::ProviderAccountMismatch,
        OpenAiApiTransportError::InvalidCredential => ProviderError::AuthenticationRequired,
        OpenAiApiTransportError::InvalidRequest => ProviderError::ProviderResponseInvalid,
        OpenAiApiTransportError::InvalidLimits | OpenAiApiTransportError::Unavailable => {
            ProviderError::ProviderUnavailable
        }
    }
}
