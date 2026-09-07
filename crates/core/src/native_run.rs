use std::future::Future;

use crate::{
    EventId, ProviderType, RunError, RunId, RunIdGenerator, RunMutation, RunSnapshot, RunStoreError,
};

pub trait NativeRunStore: Send + Sync {
    fn list_recoverable_native_runs(
        &self,
        provider: &ProviderType,
    ) -> impl Future<Output = Result<Vec<RunSnapshot>, RunStoreError>> + Send;

    fn fail_native_run(
        &self,
        run_id: &RunId,
        event_id: EventId,
    ) -> impl Future<Output = Result<RunMutation<RunSnapshot>, RunStoreError>> + Send;
}

pub struct NativeRunApplication<S, I> {
    store: S,
    ids: I,
}

impl<S, I> NativeRunApplication<S, I> {
    pub fn new(store: S, ids: I) -> Self {
        Self { store, ids }
    }
}

impl<S: NativeRunStore, I: RunIdGenerator> NativeRunApplication<S, I> {
    pub async fn list_recoverable_native_runs(
        &self,
        provider: ProviderType,
    ) -> Result<Vec<RunSnapshot>, RunError> {
        self.store
            .list_recoverable_native_runs(&provider)
            .await
            .map_err(crate::map_run_store_error)
    }

    pub async fn fail_native_run(
        &self,
        run_id: RunId,
    ) -> Result<RunMutation<RunSnapshot>, RunError> {
        self.store
            .fail_native_run(&run_id, self.ids.event_id())
            .await
            .map_err(crate::map_run_store_error)
    }
}
