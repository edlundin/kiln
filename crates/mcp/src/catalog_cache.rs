//! Bounded historical discovery snapshots, never invocation authorization.

use crate::{
    McpCatalogError, McpCatalogLimits, catalog_state::CatalogEpochs, discovery::CollectedCatalog,
};
use kiln_core::{McpCatalogKind, McpGenerationId};
use std::collections::VecDeque;

pub(crate) struct CatalogSnapshot {
    pub token: String,
    pub kind: McpCatalogKind,
    pub epoch: u64,
    pub catalog: CollectedCatalog,
}

/// One retained snapshot per protocol catalogue kind, bounded collectively by
/// max_bytes. Oldest refreshed kinds are evicted first, with explicit stale-token
/// failures. Encoded metadata accounting is not a total heap-memory guarantee.
#[derive(Default)]
pub(crate) struct CatalogCache {
    snapshots: VecDeque<CatalogSnapshot>,
    bytes: usize,
    sequence: u64,
}

impl CatalogCache {
    pub fn prune(&mut self, epochs: &CatalogEpochs, limits: McpCatalogLimits) {
        self.snapshots.retain(|snapshot| {
            let keep = epochs.unchanged(snapshot.kind, Some(snapshot.epoch))
                && snapshot.catalog.usage.fits(limits);
            if !keep {
                self.bytes -= snapshot.catalog.usage.bytes;
            }
            keep
        });
        while self.bytes > limits.max_bytes.get() {
            self.evict_oldest();
        }
    }
    fn evict_oldest(&mut self) {
        if let Some(snapshot) = self.snapshots.pop_front() {
            self.bytes -= snapshot.catalog.usage.bytes;
        }
    }
    pub fn get(
        &self,
        kind: McpCatalogKind,
        token: &str,
    ) -> Result<&CatalogSnapshot, McpCatalogError> {
        self.snapshots
            .iter()
            .find(|snapshot| snapshot.kind == kind && snapshot.token == token)
            .ok_or(McpCatalogError::SnapshotUnavailable)
    }
    pub fn insert(
        &mut self,
        generation: &McpGenerationId,
        kind: McpCatalogKind,
        epoch: u64,
        catalog: CollectedCatalog,
        limits: McpCatalogLimits,
    ) -> Result<&CatalogSnapshot, McpCatalogError> {
        if !catalog.usage.fits(limits) {
            return Err(McpCatalogError::LimitExceeded);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(McpCatalogError::SnapshotUnavailable)?;
        self.snapshots.retain(|snapshot| {
            let keep = snapshot.kind != kind;
            if !keep {
                self.bytes -= snapshot.catalog.usage.bytes;
            }
            keep
        });
        while self.bytes > limits.max_bytes.get()
            || catalog.usage.bytes > limits.max_bytes.get() - self.bytes
        {
            self.evict_oldest();
        }
        self.bytes += catalog.usage.bytes;
        self.snapshots.push_back(CatalogSnapshot {
            token: format!(
                "{}:{}:{}",
                generation.as_str(),
                kind.as_str(),
                self.sequence
            ),
            kind,
            epoch,
            catalog,
        });
        Ok(self.snapshots.back().expect("just inserted snapshot"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{catalog::CatalogUsage, discovery::McpCatalogEntries};
    use std::num::NonZeroUsize;
    fn limits(bytes: usize) -> McpCatalogLimits {
        McpCatalogLimits {
            max_pages: NonZeroUsize::new(2).unwrap(),
            max_entries: NonZeroUsize::new(2).unwrap(),
            max_bytes: NonZeroUsize::new(bytes).unwrap(),
            max_regex_bytes: NonZeroUsize::new(1).unwrap(),
            max_regex_backtracks: NonZeroUsize::new(1).unwrap(),
        }
    }
    fn catalog(bytes: usize) -> CollectedCatalog {
        CollectedCatalog {
            entries: McpCatalogEntries::Tools(Vec::new()),
            usage: CatalogUsage {
                pages: 2,
                entries: 2,
                bytes,
            },
        }
    }
    #[test]
    fn retained_snapshots_obey_budgets_replacement_invalidation_and_generation_identity() {
        use McpCatalogKind::*;
        let generation = McpGenerationId::from_ulid(ulid::Ulid::generate());
        let other = McpGenerationId::from_ulid(ulid::Ulid::generate());
        let mut cache = CatalogCache::default();
        let epochs = CatalogEpochs::default();
        let first = cache
            .insert(&generation, Tools, 0, catalog(6), limits(10))
            .unwrap()
            .token
            .clone();
        assert!(cache.get(Tools, &first).is_ok());
        assert!(cache.get(Prompts, &first).is_err());
        assert!(
            cache
                .get(Tools, &first.replace(generation.as_str(), other.as_str()))
                .is_err()
        );
        let second = cache
            .insert(&generation, Tools, 0, catalog(6), limits(10))
            .unwrap()
            .token
            .clone();
        assert_ne!(first, second);
        assert!(cache.get(Tools, &first).is_err());
        let prompt = cache
            .insert(&generation, Prompts, 0, catalog(4), limits(10))
            .unwrap()
            .token
            .clone();
        assert_eq!(cache.bytes, 10);
        cache
            .insert(&generation, Resources, 0, catalog(5), limits(10))
            .unwrap();
        assert!(cache.get(Tools, &second).is_err());
        assert!(cache.get(Prompts, &prompt).is_ok());
        epochs.invalidate(Prompts);
        cache.prune(&epochs, limits(10));
        assert!(cache.get(Prompts, &prompt).is_err());
        assert_eq!(cache.bytes, 5);
        let template = cache
            .insert(&generation, ResourceTemplates, 0, catalog(5), limits(10))
            .unwrap()
            .token
            .clone();
        epochs.invalidate(Resources);
        cache.prune(&epochs, limits(10));
        assert!(cache.get(ResourceTemplates, &template).is_err());
        assert_eq!(cache.bytes, 0);
        cache
            .insert(&generation, Tools, 0, catalog(6), limits(10))
            .unwrap();
        let mut smaller = limits(10);
        smaller.max_entries = NonZeroUsize::new(1).unwrap();
        cache.prune(&epochs, smaller);
        assert_eq!(cache.bytes, 0);
        cache
            .insert(&generation, Tools, 0, catalog(6), limits(10))
            .unwrap();
        cache.prune(&epochs, limits(5));
        assert_eq!(cache.bytes, 0);
        cache.sequence = u64::MAX;
        assert_eq!(
            cache
                .insert(&generation, Tools, 0, catalog(1), limits(10))
                .err(),
            Some(McpCatalogError::SnapshotUnavailable)
        );
    }
}
