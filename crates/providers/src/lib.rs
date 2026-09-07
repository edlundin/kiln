//! Direct model adapters. The first adapter is an explicit deterministic runtime.

use std::collections::VecDeque;

use kiln_core::{
    ModelInvocation, ModelInvocationOutcome, ModelOutputStream, ModelProvider,
    ModelProviderOperation, ProviderError, ProviderRequest, ProviderUpdate, ProviderUsageMetadata,
    ProviderUsageUpdate, RecordModelOutput, UsageAccounting, UsageCompleteness, UsageFinality,
    UsageQuantity, UsageSource,
};

pub const DETERMINISTIC_PROVIDER_TYPE: &str = "kiln_deterministic";
pub const DETERMINISTIC_MODEL_ID: &str = "deterministic_text";

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
