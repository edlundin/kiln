use kiln_core::{
    ModelInvocationId, ModelToolExchange, ModelToolExchangeError as Error, ModelToolExchangeStore,
    RunId, ToolCallId,
};
use sqlx::{Connection, Row, Sqlite, Transaction};

use super::{
    SqliteStore, load_model_invocation_unchecked, load_run, model_tool_catalog::load_catalog,
    model_tool_request::load_requests, parse_tool_call,
};

pub(super) enum ExchangeContext {
    NewManifest,
    ExistingManifest(i64),
}

impl ModelToolExchangeStore for SqliteStore {
    async fn get_model_tool_exchange(
        &self,
        run_id: &RunId,
        tool_call_id: &ToolCallId,
    ) -> Result<ModelToolExchange, Error> {
        let mut connection = self.connection.lock().await;
        let mut transaction = connection.begin().await.map_err(|_| Error::Unavailable)?;
        let exchange = load_exchange(
            &mut transaction,
            run_id,
            tool_call_id,
            ExchangeContext::NewManifest,
        )
        .await?;
        // The standalone read validates the source manifest too. Manifest-entry
        // reads below use metadata/chronology checks and never recurse here.
        super::load_model_invocation(&mut transaction, exchange.invocation_id())
            .await
            .map_err(Error::Invocation)?
            .ok_or(Error::IntegrityViolation)?;
        transaction.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(exchange)
    }
}

pub(super) async fn load_exchange(
    transaction: &mut Transaction<'_, Sqlite>,
    run_id: &RunId,
    tool_call_id: &ToolCallId,
    context: ExchangeContext,
) -> Result<ModelToolExchange, Error> {
    let row = sqlx::query("SELECT adoption.model_invocation_id, adoption.provider_call_id,
        tool.tool_call_id, tool.run_id, tool.capability, tool.state,
        tool.requested_workspace_root_id, tool.requested_relative_directory,
        tool.effective_workspace_root_id, tool.effective_relative_directory,
        tool.stdout, tool.stderr, tool.exit_code,
        tool.stdout_artifact_hash, stdout_artifact.media_type AS stdout_artifact_media_type,
        stdout_artifact.size AS stdout_artifact_size,
        tool.stderr_artifact_hash, stderr_artifact.media_type AS stderr_artifact_media_type,
        stderr_artifact.size AS stderr_artifact_size
        FROM model_tool_adoptions AS adoption
        JOIN tool_calls AS tool ON tool.tool_call_id = adoption.tool_call_id
        LEFT JOIN artifacts AS stdout_artifact ON stdout_artifact.content_hash = tool.stdout_artifact_hash
        LEFT JOIN artifacts AS stderr_artifact ON stderr_artifact.content_hash = tool.stderr_artifact_hash
        WHERE adoption.tool_call_id = ?")
        .bind(tool_call_id.as_str()).fetch_optional(&mut **transaction).await
        .map_err(|_| Error::Unavailable)?.ok_or(Error::NotFound)?;
    let tool = parse_tool_call(&row).map_err(|_| Error::IntegrityViolation)?;
    if tool.run_id() != run_id {
        return Err(Error::RunMismatch);
    }
    if !tool.state().is_terminal() {
        return Err(Error::ToolNotComplete);
    }
    let invocation_id = ModelInvocationId::parse(
        row.try_get::<String, _>("model_invocation_id")
            .map_err(|_| Error::IntegrityViolation)?,
    )
    .map_err(|_| Error::IntegrityViolation)?;
    let provider_call_id: String = row
        .try_get("provider_call_id")
        .map_err(|_| Error::IntegrityViolation)?;
    let run = load_run(transaction, run_id)
        .await
        .map_err(Error::Store)?
        .ok_or(Error::IntegrityViolation)?;
    let (sequence, invocation) = load_model_invocation_unchecked(transaction, &invocation_id)
        .await
        .map_err(Error::Invocation)?
        .ok_or(Error::IntegrityViolation)?;
    match context {
        ExchangeContext::NewManifest => {
            validate_source_metadata(transaction, &run, sequence, &invocation, None).await?
        }
        ExchangeContext::ExistingManifest(destination_sequence) => {
            validate_source_metadata(
                transaction,
                &run,
                sequence,
                &invocation,
                Some(destination_sequence),
            )
            .await?
        }
    }
    let requests = load_requests(transaction, &invocation)
        .await
        .map_err(Error::Requests)?
        .ok_or(Error::IntegrityViolation)?;
    let position = requests
        .requests()
        .iter()
        .position(|request| request.provider_call_id() == provider_call_id)
        .ok_or(Error::IntegrityViolation)?;
    let catalog = load_catalog(transaction, &invocation)
        .await
        .map_err(Error::Catalog)?
        .ok_or(Error::IntegrityViolation)?;
    ModelToolExchange::new(&run, &invocation, &requests, &catalog, position, tool)
}

/// Validate source identity and chronology without recursively reloading every
/// prior manifest. Each destination snapshot independently binds full exchange
/// content; source manifest headers/retry metadata retain the provenance chain.
async fn validate_source_metadata(
    transaction: &mut Transaction<'_, Sqlite>,
    run: &kiln_core::Run,
    sequence: i64,
    invocation: &kiln_core::ModelInvocation,
    destination_sequence: Option<i64>,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT sequence, session_id, run_id, content_hash
        FROM context_manifests WHERE context_manifest_id = ?",
    )
    .bind(invocation.context_manifest_id().as_str())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| Error::Unavailable)?
    .ok_or(Error::IntegrityViolation)?;
    let source_sequence: i64 = row
        .try_get("sequence")
        .map_err(|_| Error::IntegrityViolation)?;
    if row
        .try_get::<String, _>("run_id")
        .map_err(|_| Error::IntegrityViolation)?
        != run.run_id().as_str()
        || row
            .try_get::<String, _>("session_id")
            .map_err(|_| Error::IntegrityViolation)?
            != run.session_id().as_str()
        || row
            .try_get::<String, _>("content_hash")
            .map_err(|_| Error::IntegrityViolation)?
            != invocation.context_manifest_hash().as_str()
        || destination_sequence.is_some_and(|destination| source_sequence >= destination)
    {
        return Err(Error::IntegrityViolation);
    }
    let mut current_sequence = sequence;
    let mut current = invocation.clone();
    while let Some(previous_id) = current.retry_of() {
        let (previous_sequence, previous) =
            load_model_invocation_unchecked(transaction, previous_id)
                .await
                .map_err(Error::Invocation)?
                .ok_or(Error::IntegrityViolation)?;
        if previous_sequence >= current_sequence
            || previous.work_id() != invocation.work_id()
            || !matches!(
                previous.state(),
                kiln_core::ModelInvocationState::Failed
                    | kiln_core::ModelInvocationState::Interrupted
            )
            || !super::model_invocation_request_fields_match(&previous, &current)
        {
            return Err(Error::IntegrityViolation);
        }
        current_sequence = previous_sequence;
        current = previous;
    }
    Ok(())
}
