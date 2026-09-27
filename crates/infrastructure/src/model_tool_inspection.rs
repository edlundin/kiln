use std::num::NonZeroUsize;

use kiln_core::{
    ModelInvocationId, NativeToolSource, RunId, ToolCallId, ToolCallInspection,
    ToolCallInspectionError as Error, ToolCallInspectionStore,
};
use sqlx::{Connection, Row};

use super::{
    SqliteStore, load_model_invocation, model_tool_catalog::load_catalog,
    model_tool_request::load_requests,
};

impl ToolCallInspectionStore for SqliteStore {
    async fn inspect_tool_call(
        &self,
        tool_call_id: &ToolCallId,
        max_source_bytes: NonZeroUsize,
    ) -> Result<ToolCallInspection, Error> {
        let budget = i64::try_from(max_source_bytes.get()).map_err(|_| Error::LimitExceeded)?;
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let row = sqlx::query("SELECT tool.run_id,
            CASE WHEN length(CAST(tool.capability AS BLOB)) <= ? THEN tool.capability END AS capability,
            adoption.model_invocation_id,
            CASE WHEN length(CAST(adoption.provider_call_id AS BLOB)) <= ? THEN adoption.provider_call_id END AS provider_call_id
            FROM tool_calls AS tool LEFT JOIN model_tool_adoptions AS adoption
              ON adoption.tool_call_id = tool.tool_call_id WHERE tool.tool_call_id = ?")
            .bind(budget).bind(budget).bind(tool_call_id.as_str()).fetch_optional(&mut *transaction).await
            .map_err(|_| Error::Unavailable)?.ok_or(Error::NotFound)?;
        let run_id = RunId::parse(
            row.try_get::<String, _>("run_id")
                .map_err(|_| Error::IntegrityViolation)?,
        )
        .map_err(|_| Error::IntegrityViolation)?;
        let capability: String = row
            .try_get::<Option<String>, _>("capability")
            .map_err(|_| Error::IntegrityViolation)?
            .ok_or(Error::LimitExceeded)?;
        let invocation_id: Option<String> = row
            .try_get("model_invocation_id")
            .map_err(|_| Error::IntegrityViolation)?;
        let provider_call_id: Option<String> = row
            .try_get("provider_call_id")
            .map_err(|_| Error::IntegrityViolation)?;
        let source = match (invocation_id, provider_call_id) {
            (None, None) => None,
            (Some(invocation_id), Some(provider_call_id)) => {
                let invocation_id = ModelInvocationId::parse(invocation_id)
                    .map_err(|_| Error::IntegrityViolation)?;
                // Preflight before reading text. Full batch/catalogue validation
                // is needed to verify the stored hashes; no unrelated invocation
                // is loaded, and no partial/truncated arguments are presented.
                let bytes: i64 = sqlx::query_scalar("SELECT
                    COALESCE((SELECT SUM(length(CAST(provider_call_id AS BLOB)) + length(CAST(name AS BLOB))
                        + length(CAST(arguments_json AS BLOB))) FROM model_tool_requests WHERE model_invocation_id = ?), 0)
                    + COALESCE((SELECT SUM(length(CAST(definition_json AS BLOB))) FROM model_tool_definitions
                        WHERE model_invocation_id = ?), 0)")
                    .bind(invocation_id.as_str()).bind(invocation_id.as_str()).fetch_one(&mut *transaction).await
                    .map_err(|_| Error::Unavailable)?;
                if bytes < 0
                    || usize::try_from(bytes)
                        .ok()
                        .and_then(|bytes| bytes.checked_add(capability.len()))
                        .is_none_or(|bytes| bytes > max_source_bytes.get())
                {
                    return Err(Error::LimitExceeded);
                }
                let invocation = load_model_invocation(&mut transaction, &invocation_id)
                    .await
                    .map_err(|_| Error::IntegrityViolation)?
                    .ok_or(Error::IntegrityViolation)?;
                if invocation.run_id() != &run_id {
                    return Err(Error::IntegrityViolation);
                }
                let requests = load_requests(&mut transaction, &invocation)
                    .await
                    .map_err(|_| Error::IntegrityViolation)?
                    .ok_or(Error::IntegrityViolation)?;
                let catalog = load_catalog(&mut transaction, &invocation)
                    .await
                    .map_err(|_| Error::IntegrityViolation)?
                    .ok_or(Error::IntegrityViolation)?;
                let request = requests
                    .requests()
                    .iter()
                    .find(|request| request.provider_call_id() == provider_call_id)
                    .ok_or(Error::IntegrityViolation)?;
                let definition = catalog
                    .find(request.name())
                    .ok_or(Error::IntegrityViolation)?;
                if definition.capability() != capability {
                    return Err(Error::IntegrityViolation);
                }
                Some(NativeToolSource {
                    invocation_id,
                    request: request.clone(),
                    definition: definition.clone(),
                })
            }
            (Some(_), None) => return Err(Error::LimitExceeded),
            (None, Some(_)) => return Err(Error::IntegrityViolation),
        };
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(ToolCallInspection {
            tool_call_id: tool_call_id.clone(),
            run_id,
            capability,
            source,
        })
    }
}
