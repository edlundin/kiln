//! Direct model adapters. The first adapter is an explicit deterministic runtime.

use std::{collections::VecDeque, future::Future, pin::Pin};

use kiln_core::{
    ModelId, ModelInvocation, ModelInvocationOutcome, ModelOutputStream, ModelProvider,
    ModelProviderOperation, ProviderAccountId, ProviderError, ProviderRequest, ProviderType,
    ProviderUpdate, ProviderUsageMetadata, ProviderUsageUpdate, RecordModelOutput, UsageAccounting,
    UsageCompleteness, UsageFinality, UsageQuantity, UsageSource,
};

pub const DETERMINISTIC_PROVIDER_TYPE: &str = "kiln_deterministic";
pub const DETERMINISTIC_MODEL_ID: &str = "deterministic_text";
/// Fixture-only account identity; it never resolves credentials.
pub const DETERMINISTIC_PROVIDER_ACCOUNT_ID: &str = "pac_00000000000000000000000000";

type OperationFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
type ProviderStartFuture<'a> = OperationFuture<'a, Result<ProviderOperation, ProviderError>>;

trait ErasedProviderOperation: Send {
    fn next_update(&mut self)
    -> OperationFuture<'_, Result<Option<ProviderUpdate>, ProviderError>>;

    fn cancel(&mut self) -> OperationFuture<'_, Result<(), ProviderError>>;
}

impl<T> ErasedProviderOperation for T
where
    T: ModelProviderOperation + 'static,
{
    fn next_update(
        &mut self,
    ) -> OperationFuture<'_, Result<Option<ProviderUpdate>, ProviderError>> {
        Box::pin(ModelProviderOperation::next_update(self))
    }

    fn cancel(&mut self) -> OperationFuture<'_, Result<(), ProviderError>> {
        Box::pin(ModelProviderOperation::cancel(self))
    }
}

/// An operation whose concrete adapter type stays inside this crate.
pub struct ProviderOperation {
    operation: Box<dyn ErasedProviderOperation>,
}

impl ProviderOperation {
    fn new<T>(operation: T) -> Self
    where
        T: ModelProviderOperation + 'static,
    {
        Self {
            operation: Box::new(operation),
        }
    }
}

impl ModelProviderOperation for ProviderOperation {
    fn next_update(
        &mut self,
    ) -> impl Future<Output = Result<Option<ProviderUpdate>, ProviderError>> + Send {
        self.operation.next_update()
    }

    fn cancel(&mut self) -> impl Future<Output = Result<(), ProviderError>> + Send {
        self.operation.cancel()
    }
}

trait ProviderFactory: Send + Sync {
    fn provider_type(&self) -> &ProviderType;
    fn model_id(&self) -> &ModelId;
    fn account_id(&self) -> &ProviderAccountId;
    fn start(&self, request: ProviderRequest) -> ProviderStartFuture<'_>;
}

struct RegisteredProvider<P> {
    provider_type: ProviderType,
    model_id: ModelId,
    account_id: ProviderAccountId,
    provider: P,
}

impl<P> ProviderFactory for RegisteredProvider<P>
where
    P: ModelProvider + 'static,
{
    fn provider_type(&self) -> &ProviderType {
        &self.provider_type
    }

    fn model_id(&self) -> &ModelId {
        &self.model_id
    }

    fn account_id(&self) -> &ProviderAccountId {
        &self.account_id
    }

    fn start(&self, request: ProviderRequest) -> ProviderStartFuture<'_> {
        Box::pin(async move {
            self.provider
                .start(request)
                .await
                .map(ProviderOperation::new)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderRegistryError {
    DuplicateRegistration,
}

/// Resolves a durable provider selection to one registered adapter.
///
/// The registry stores only typed provider, model, and opaque account
/// identifiers. Credential lookup and transport remain adapter concerns.
pub struct ProviderRegistry {
    providers: Vec<Box<dyn ProviderFactory>>,
}

impl ProviderRegistry {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn register<P>(
        &mut self,
        provider_type: ProviderType,
        model_id: ModelId,
        account_id: ProviderAccountId,
        provider: P,
    ) -> Result<(), ProviderRegistryError>
    where
        P: ModelProvider + 'static,
    {
        if self.providers.iter().any(|registered| {
            registered.provider_type() == &provider_type
                && registered.model_id() == &model_id
                && registered.account_id() == &account_id
        }) {
            return Err(ProviderRegistryError::DuplicateRegistration);
        }
        self.providers.push(Box::new(RegisteredProvider {
            provider_type,
            model_id,
            account_id,
            provider,
        }));
        Ok(())
    }

    pub async fn start(
        &self,
        request: ProviderRequest,
    ) -> Result<ProviderOperation, ProviderError> {
        let invocation = request.invocation();
        let provider = invocation.settings().provider();
        let model = invocation.settings().model();
        let account = invocation.provider_account_id();
        let has_provider_model = self.providers.iter().any(|registered| {
            registered.provider_type() == provider && registered.model_id() == model
        });
        let Some(registered) = self.providers.iter().find(|registered| {
            registered.provider_type() == provider
                && registered.model_id() == model
                && registered.account_id() == account
        }) else {
            return Err(if has_provider_model {
                ProviderError::ProviderAccountMismatch
            } else {
                ProviderError::ModelUnavailable
            });
        };
        registered.start(request).await
    }
}

impl Default for ProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

pub struct DeterministicModelResponse {
    pub text: Vec<String>,
    pub quantities: Vec<UsageQuantity>,
    pub completeness: UsageCompleteness,
    pub outcome: ModelInvocationOutcome,
    pub observed_at_unix_ms: u64,
}

pub struct DeterministicModelProvider {
    response: DeterministicModelResponse,
}

impl DeterministicModelProvider {
    pub fn new(response: DeterministicModelResponse) -> Self {
        Self { response }
    }
}

pub struct DeterministicModelOperation {
    invocation: ModelInvocation,
    output: VecDeque<RecordModelOutput>,
    completion: Option<ProviderUpdate>,
    observed_at_unix_ms: u64,
}

impl ModelProvider for DeterministicModelProvider {
    type Operation = DeterministicModelOperation;

    async fn start(&self, request: ProviderRequest) -> Result<Self::Operation, ProviderError> {
        let invocation = request.invocation();
        if invocation.settings().provider().as_str() != DETERMINISTIC_PROVIDER_TYPE
            || invocation.settings().model().as_str() != DETERMINISTIC_MODEL_ID
        {
            return Err(ProviderError::ModelUnavailable);
        }
        if invocation.provider_account_id().as_str() != DETERMINISTIC_PROVIDER_ACCOUNT_ID {
            return Err(ProviderError::ProviderAccountMismatch);
        }
        let output = self
            .response
            .text
            .iter()
            .enumerate()
            .map(|(index, text)| {
                let position = u64::try_from(index)
                    .ok()
                    .and_then(|index| index.checked_add(1))
                    .ok_or(ProviderError::ProviderResponseInvalid)?;
                RecordModelOutput::new(
                    invocation.invocation_id().clone(),
                    format!(
                        "fixture:output:{}:{position}",
                        invocation.invocation_id().as_str()
                    ),
                    position,
                    ModelOutputStream::AssistantText,
                    text.clone(),
                )
                .map_err(|_| ProviderError::ProviderResponseInvalid)
            })
            .collect::<Result<VecDeque<_>, _>>()?;
        let completion = ProviderUpdate::Finished {
            outcome: self.response.outcome,
            usage: final_usage(
                invocation,
                self.response.observed_at_unix_ms,
                self.response.completeness,
                self.response.quantities.clone(),
            )?,
        };
        completion.validate_for(invocation)?;
        Ok(DeterministicModelOperation {
            invocation: invocation.clone(),
            output,
            completion: Some(completion),
            observed_at_unix_ms: self.response.observed_at_unix_ms,
        })
    }
}

impl ModelProviderOperation for DeterministicModelOperation {
    async fn next_update(&mut self) -> Result<Option<ProviderUpdate>, ProviderError> {
        if let Some(output) = self.output.pop_front() {
            return Ok(Some(ProviderUpdate::Output(output)));
        }
        Ok(self.completion.take())
    }

    async fn cancel(&mut self) -> Result<(), ProviderError> {
        if self.completion.is_none() {
            return Ok(());
        }
        let usage = final_usage(
            &self.invocation,
            self.observed_at_unix_ms,
            UsageCompleteness::Unknown,
            Vec::new(),
        )?;
        self.output.clear();
        self.completion = Some(ProviderUpdate::Finished {
            outcome: ModelInvocationOutcome::cancelled(),
            usage,
        });
        Ok(())
    }
}

fn final_usage(
    invocation: &ModelInvocation,
    observed_at_unix_ms: u64,
    completeness: UsageCompleteness,
    quantities: Vec<UsageQuantity>,
) -> Result<ProviderUsageUpdate, ProviderError> {
    ProviderUsageUpdate::new(
        ProviderUsageMetadata {
            update_id: format!("fixture:terminal:{}", invocation.invocation_id().as_str()),
            provider_account_id: invocation.provider_account_id().clone(),
            work_id: invocation.work_id().clone(),
            model_invocation_id: invocation.invocation_id().clone(),
            accounting: UsageAccounting::Cumulative,
            finality: UsageFinality::Final,
            completeness,
            observed_at_unix_ms,
            request_id: Some(format!("fixture:{}", invocation.invocation_id().as_str())),
            resolved_model: Some(invocation.settings().model().as_str().to_owned()),
            service_tier: None,
            source: UsageSource::NativeProvider,
        },
        quantities,
    )
    .map_err(|_| ProviderError::ProviderResponseInvalid)
}
