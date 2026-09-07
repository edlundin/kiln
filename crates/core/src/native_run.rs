use std::future::Future;

use crate::{EventId, RunError, RunId, RunIdGenerator, RunMutation, RunSnapshot, RunStoreError};

pub trait NativeRunStore: Send + Sync {
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
