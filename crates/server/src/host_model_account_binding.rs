use super::{AppState, PublicError};
use axum::{Json, extract::State, http::header, response::IntoResponse};
use kiln_core::{
    HOST_MODEL_ACCOUNT_BINDING_DEFAULT_PAGE_SIZE, HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES,
    HostModelAccountBindingApplication, HostModelAccountBindingError, HostModelAccountBindingStore,
    HostModelAccountBindingView, ProviderAccountId, ProviderAccountStore, ProviderType,
    SharedConfigurationKey,
};
use kiln_protocol::{
    GetModelAccountBindingRequest, ListModelAccountBindingsRequest,
    ListModelAccountBindingsResponse, MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE,
    MODEL_ACCOUNT_BINDING_RESPONSE_MAX_BYTES, ModelAccountBindingResponse,
    RemoveModelAccountBindingRequest, SetModelAccountBindingRequest,
};
use std::{future::Future, pin::Pin};

pub(super) trait HostModelAccountBindingOperations: Send + Sync {
    fn get(
        &self,
        key: SharedConfigurationKey,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    >;

    fn list(
        &self,
        after: Option<SharedConfigurationKey>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Vec<HostModelAccountBindingView>, HostModelAccountBindingError>,
                > + Send
                + '_,
        >,
    >;

    fn set(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
        expected_provider_type: ProviderType,
        provider_account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    >;

    fn remove(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    >;
}

pub(super) struct HostModelAccountBindingAdapter<T>(pub HostModelAccountBindingApplication<T>);

impl<T> HostModelAccountBindingOperations for HostModelAccountBindingAdapter<T>
where
    T: HostModelAccountBindingStore + ProviderAccountStore,
{
    fn get(
        &self,
        key: SharedConfigurationKey,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.get(key))
    }

    fn list(
        &self,
        after: Option<SharedConfigurationKey>,
        limit: usize,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<Vec<HostModelAccountBindingView>, HostModelAccountBindingError>,
                > + Send
                + '_,
        >,
    > {
        Box::pin(self.0.list(after, limit))
    }

    fn set(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
        expected_provider_type: ProviderType,
        provider_account_id: ProviderAccountId,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.set(
            key,
            expected_version,
            expected_provider_type,
            provider_account_id,
        ))
    }

    fn remove(
        &self,
        key: SharedConfigurationKey,
        expected_version: u64,
    ) -> Pin<
        Box<
            dyn Future<Output = Result<HostModelAccountBindingView, HostModelAccountBindingError>>
                + Send
                + '_,
        >,
    > {
        Box::pin(self.0.remove(key, expected_version))
    }
}

pub(super) async fn list<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<ListModelAccountBindingsRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: super::WorkspaceOperations + 'static,
    S: super::SessionOperations + 'static,
    R: super::RunOperations + 'static,
{
    let limit = request
        .limit
        .unwrap_or(HOST_MODEL_ACCOUNT_BINDING_DEFAULT_PAGE_SIZE);
    if !(1..=MODEL_ACCOUNT_BINDING_MAX_PAGE_SIZE).contains(&limit) {
        return Err(invalid_request());
    }
    let mut after = request.after.map(parse_binding_key).transpose()?;
    let operations = state
        .host_model_account_binding_operations
        .as_ref()
        .ok_or(unavailable())?;
    let mut bindings = Vec::with_capacity(limit);
    let mut encoded_items_bytes = 0usize;
    let mut has_more = false;

    // Read one row at a time. A portable key may approach the existing 2 MiB
    // snapshot budget, so count limits alone would permit an unbounded page.
    for _ in 0..=limit {
        let mut page = operations
            .list(after.clone(), 1)
            .await
            .map_err(PublicError::HostModelAccountBinding)?;
        let Some(view) = page.pop() else {
            break;
        };
        if bindings.len() == limit {
            has_more = true;
            break;
        }
        let binding = binding_response(&view);
        let item_bytes = serde_json::to_vec(&binding)
            .map_err(|_| {
                PublicError::HostModelAccountBinding(
                    HostModelAccountBindingError::IntegrityViolation,
                )
            })?
            .len();
        let candidate_items_bytes = encoded_items_bytes.checked_add(item_bytes).ok_or(
            PublicError::HostModelAccountBinding(HostModelAccountBindingError::IntegrityViolation),
        )?;
        let response_bytes = encoded_list_response_bytes(
            candidate_items_bytes,
            bindings.len() + 1,
            Some(&binding.binding_key),
        )?;
        if response_bytes > MODEL_ACCOUNT_BINDING_RESPONSE_MAX_BYTES {
            if bindings.is_empty() {
                return Err(PublicError::HostModelAccountBinding(
                    HostModelAccountBindingError::IntegrityViolation,
                ));
            }
            has_more = true;
            break;
        }
        encoded_items_bytes = candidate_items_bytes;
        after = Some(view.key);
        bindings.push(binding);
    }

    let next_cursor = has_more
        .then(|| after.as_ref().map(|key| key.as_str().to_owned()))
        .flatten();
    if encoded_list_response_bytes(encoded_items_bytes, bindings.len(), next_cursor.as_deref())?
        > MODEL_ACCOUNT_BINDING_RESPONSE_MAX_BYTES
    {
        return Err(PublicError::HostModelAccountBinding(
            HostModelAccountBindingError::IntegrityViolation,
        ));
    }
    let response = ListModelAccountBindingsResponse {
        bindings,
        next_cursor,
    };
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

pub(super) async fn get<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<GetModelAccountBindingRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: super::WorkspaceOperations + 'static,
    S: super::SessionOperations + 'static,
    R: super::RunOperations + 'static,
{
    let key = parse_binding_key(request.binding_key)?;
    let operations = state
        .host_model_account_binding_operations
        .as_ref()
        .ok_or(unavailable())?;
    let binding = operations
        .get(key)
        .await
        .map_err(PublicError::HostModelAccountBinding)?;
    let response = binding_response(&binding);
    validate_binding_response_size(&response)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

pub(super) async fn set<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<SetModelAccountBindingRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: super::WorkspaceOperations + 'static,
    S: super::SessionOperations + 'static,
    R: super::RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let key = parse_binding_key(request.binding_key)?;
    let provider_type =
        ProviderType::parse(request.expected_provider_type).map_err(|_| invalid_request())?;
    let account_id =
        ProviderAccountId::parse(request.provider_account_id).map_err(|_| invalid_request())?;
    let operations = state
        .host_model_account_binding_operations
        .clone()
        .ok_or(unavailable())?;
    let binding = tokio::spawn(async move {
        let _permit = permit;
        operations
            .set(key, request.expected_version, provider_type, account_id)
            .await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::HostModelAccountBinding)?;
    let response = binding_response(&binding);
    validate_binding_response_size(&response)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

pub(super) async fn remove<W, S, R>(
    State(state): State<AppState<W, S, R>>,
    super::StrictJson(request): super::StrictJson<RemoveModelAccountBindingRequest>,
) -> Result<impl IntoResponse, PublicError>
where
    W: super::WorkspaceOperations + 'static,
    S: super::SessionOperations + 'static,
    R: super::RunOperations + 'static,
{
    let permit = state.lifecycle.begin_command()?;
    let key = parse_binding_key(request.binding_key)?;
    let operations = state
        .host_model_account_binding_operations
        .clone()
        .ok_or(unavailable())?;
    let binding = tokio::spawn(async move {
        let _permit = permit;
        operations.remove(key, request.expected_version).await
    })
    .await
    .map_err(|_| PublicError::ConfigurationSyncUnavailable)?
    .map_err(PublicError::HostModelAccountBinding)?;
    let response = binding_response(&binding);
    validate_binding_response_size(&response)?;
    Ok(([(header::CACHE_CONTROL, "no-store")], Json(response)))
}

fn parse_binding_key(value: String) -> Result<SharedConfigurationKey, PublicError> {
    SharedConfigurationKey::parse(value, HOST_MODEL_ACCOUNT_BINDING_KEY_MAX_BYTES)
        .map_err(|_| invalid_request())
}

fn binding_response(binding: &HostModelAccountBindingView) -> ModelAccountBindingResponse {
    ModelAccountBindingResponse {
        binding_key: binding.key.as_str().to_owned(),
        version: binding.version,
        provider_account_id: binding
            .account
            .as_ref()
            .map(|account| account.id.as_str().to_owned()),
        provider_type: binding
            .account
            .as_ref()
            .map(|account| account.provider_type.as_str().to_owned()),
        provider_account_label: binding
            .account
            .as_ref()
            .map(|account| account.label.clone()),
        provider_account_state: binding
            .account
            .as_ref()
            .map(|account| account.state.as_str().to_owned()),
    }
}

fn validate_binding_response_size(
    response: &ModelAccountBindingResponse,
) -> Result<(), PublicError> {
    let encoded_len = serde_json::to_vec(response)
        .map_err(|_| {
            PublicError::HostModelAccountBinding(HostModelAccountBindingError::IntegrityViolation)
        })?
        .len();
    if encoded_len > MODEL_ACCOUNT_BINDING_RESPONSE_MAX_BYTES {
        return Err(PublicError::HostModelAccountBinding(
            HostModelAccountBindingError::IntegrityViolation,
        ));
    }
    Ok(())
}

fn encoded_list_response_bytes(
    item_bytes: usize,
    item_count: usize,
    cursor: Option<&str>,
) -> Result<usize, PublicError> {
    let cursor_bytes = match cursor {
        Some(cursor) => serde_json::to_vec(cursor).map(|bytes| bytes.len()),
        None => Ok(b"null".len()),
    }
    .map_err(|_| {
        PublicError::HostModelAccountBinding(HostModelAccountBindingError::IntegrityViolation)
    })?;
    b"{\"bindings\":["
        .len()
        .checked_add(item_bytes)
        .and_then(|bytes| bytes.checked_add(item_count.saturating_sub(1)))
        .and_then(|bytes| bytes.checked_add(b"],\"next_cursor\":".len()))
        .and_then(|bytes| bytes.checked_add(cursor_bytes))
        .and_then(|bytes| bytes.checked_add(1))
        .ok_or(PublicError::HostModelAccountBinding(
            HostModelAccountBindingError::IntegrityViolation,
        ))
}

fn invalid_request() -> PublicError {
    PublicError::HostModelAccountBinding(HostModelAccountBindingError::InvalidRequest)
}

fn unavailable() -> PublicError {
    PublicError::HostModelAccountBinding(HostModelAccountBindingError::Unavailable)
}
