//! Generation-local invalidation. Notifications never grant execution authority.

use kiln_core::McpCatalogKind;
use rmcp::{ClientHandler, RoleClient, service::NotificationContext};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Constant-size state: one counter per protocol list-change channel. Resources
/// and templates share a channel. A new generation starts with separate state.
#[derive(Clone, Default)]
pub(crate) struct CatalogEpochs(Arc<[AtomicU64; 3]>);

impl CatalogEpochs {
    fn counter(&self, kind: McpCatalogKind) -> &AtomicU64 {
        &self.0[match kind {
            McpCatalogKind::Tools => 0,
            McpCatalogKind::Prompts => 1,
            McpCatalogKind::Resources | McpCatalogKind::ResourceTemplates => 2,
        }]
    }
    pub(crate) fn version(&self, kind: McpCatalogKind) -> Option<u64> {
        let version = self.counter(kind).load(Ordering::SeqCst);
        // Saturation permanently invalidates this channel in this generation;
        // wrapping would make stale metadata appear current again.
        (version != u64::MAX).then_some(version)
    }
    pub(crate) fn unchanged(&self, kind: McpCatalogKind, expected: Option<u64>) -> bool {
        expected.is_some() && self.version(kind) == expected
    }
    pub(crate) fn invalidate(&self, kind: McpCatalogKind) {
        let _ = self
            .counter(kind)
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                Some(value.saturating_add(1))
            });
    }
}

impl ClientHandler for CatalogEpochs {
    fn on_tool_list_changed(
        &self,
        _: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.invalidate(McpCatalogKind::Tools);
        std::future::ready(())
    }
    fn on_prompt_list_changed(
        &self,
        _: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.invalidate(McpCatalogKind::Prompts);
        std::future::ready(())
    }
    fn on_resource_list_changed(
        &self,
        _: NotificationContext<RoleClient>,
    ) -> impl Future<Output = ()> + Send + '_ {
        self.invalidate(McpCatalogKind::Resources);
        std::future::ready(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn channels_are_generation_local_and_saturation_never_revives_a_snapshot() {
        use McpCatalogKind::*;
        let epochs = CatalogEpochs::default();
        let reader = epochs.clone();
        epochs.invalidate(Tools);
        assert!(!reader.unchanged(Tools, Some(0)));
        assert!(reader.unchanged(Prompts, Some(0)));
        epochs.invalidate(Prompts);
        assert!(!reader.unchanged(Prompts, Some(0)));
        epochs.invalidate(Resources);
        assert!(!reader.unchanged(ResourceTemplates, Some(0)));
        assert!(CatalogEpochs::default().unchanged(Tools, Some(0)));
        epochs.counter(Tools).store(u64::MAX - 1, Ordering::SeqCst);
        epochs.invalidate(Tools);
        epochs.invalidate(Tools);
        assert_eq!(epochs.version(Tools), None);
        assert!(!epochs.unchanged(Tools, None));
    }
}
