use super::*;
use kiln_core::{
    McpElicitationUrl, McpElicitationUrlContext, McpElicitationUrlDecision,
    McpElicitationUrlLimits, McpElicitationUrlMutation, McpElicitationUrlPolicy,
};

/// Immutable host policy. Enabling this requires an authenticated consent
/// surface; it does not authorize automatic navigation or inspect browser data.
#[derive(Debug, Clone, Copy)]
pub struct McpUrlElicitationConfig {
    pub limits: McpElicitationUrlLimits,
    pub policy: McpElicitationUrlPolicy,
}

impl<S: McpInputStore + McpElicitationDecisionStore + McpElicitationUrlStore> RuntimeClient<S> {
    pub(crate) fn supports_url_elicitation(&self) -> bool {
        self.url_elicitation.is_some()
    }

    pub(crate) async fn url_elicitation(
        &self,
        message: String,
        url: String,
        context: McpElicitationUrlContext,
    ) -> Result<ElicitResult, ErrorData> {
        let unavailable = || ErrorData::internal_error("MCP elicitation unavailable", None);
        let active = self
            .active
            .lock()
            .map_err(|_| unavailable())?
            .clone()
            .ok_or_else(unavailable)?;
        let mut stopped = active.alive.subscribe();
        let live = || *active.alive.borrow() && tokio::time::Instant::now() < active.deadline;
        let work = async {
            // All roots/forms/URLs share this quota and serial interaction lock.
            // Rejected URL requests consume quota before policy/normalization.
            let mut budget = active.remaining.lock().await;
            if !live() || budget.0 == 0 {
                return Err(unavailable());
            }
            budget.0 -= 1;
            let config = self.url_elicitation.ok_or_else(unavailable)?;
            let request =
                McpElicitationUrl::new(message, url, context, config.limits, config.policy)
                    .map_err(|_| unavailable())?;
            budget.1 = budget
                .1
                .checked_add(1)
                .filter(|n| *n <= i64::MAX as u64)
                .ok_or_else(unavailable)?;
            let ordinal = NonZeroU64::new(budget.1).ok_or_else(unavailable)?;
            let mut changes = self.store.subscribe_mcp_input_changes();
            let McpElicitationUrlMutation::Applied(record) = self
                .store
                .require_mcp_elicitation_url(
                    &active.invocation,
                    ordinal,
                    &request,
                    config.limits,
                    config.policy,
                )
                .await
                .map_err(|_| unavailable())?
            else {
                return Err(unavailable());
            };
            loop {
                changes.borrow_and_update();
                if !live() {
                    return Err(unavailable());
                }
                if let Some(decision) = self
                    .store
                    .get_mcp_elicitation_url_decision(&record, config.limits, config.policy)
                    .await
                    .map_err(|_| unavailable())?
                {
                    let result = ElicitResult::new(match decision {
                        McpElicitationUrlDecision::Accept => ElicitationAction::Accept,
                        McpElicitationUrlDecision::Decline => ElicitationAction::Decline,
                        McpElicitationUrlDecision::Cancel => ElicitationAction::Cancel,
                    });
                    if serde_json::to_vec(&result)
                        .map_err(|_| unavailable())?
                        .len()
                        > active.max_result_bytes
                        || !live()
                    {
                        return Err(unavailable());
                    }
                    // Consent is not external completion or authority to replay.
                    // A fresh live resolution must precede this response.
                    let McpInputMutation::Applied(_) = self
                        .store
                        .resolve_mcp_input(&record.input)
                        .await
                        .map_err(|_| unavailable())?
                    else {
                        return Err(unavailable());
                    };
                    if !live() {
                        return Err(unavailable());
                    }
                    return Ok(result);
                }
                changes.changed().await.map_err(|_| unavailable())?;
            }
        };
        tokio::select! {
            biased;
            _ = stopped.changed() => Err(unavailable()),
            _ = tokio::time::sleep_until(active.deadline) => Err(unavailable()),
            result = work => result,
        }
    }
}
