use std::{fs, num::NonZeroUsize};

use kiln_core::Artifact;
use kiln_infrastructure::{ArtifactPageError, ArtifactStoreError, FileArtifactStore};

fn page_size(size: usize) -> NonZeroUsize {
    NonZeroUsize::new(size).unwrap()
}

#[test]
fn pages_preserve_exact_bytes_across_scan_boundaries_and_eof() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileArtifactStore::open(directory.path()).unwrap();
    let bytes: Vec<u8> = (0..150_000).map(|n| (n % 251) as u8).collect();
    let artifact = store.store(&bytes, "application/octet-stream").unwrap();
    for (offset, limit) in [(0, 13), (65_530, 19), (149_995, 100), (150_000, 1)] {
        let page = store
            .read_page(&artifact, offset, page_size(limit), artifact.size())
            .unwrap()
            .unwrap();
        let start = offset as usize;
        assert_eq!(page, bytes[start..(start + limit).min(bytes.len())]);
    }
    assert!(matches!(
        store.read_page(&artifact, 150_001, page_size(1), artifact.size()),
        Err(ArtifactPageError::InvalidOffset)
    ));
    let empty = store.store(&[], "application/octet-stream").unwrap();
    assert_eq!(
        store.read_page(&empty, 0, page_size(1), 0).unwrap(),
        Some(vec![])
    );
}

#[test]
fn pages_reject_corruption_outside_the_requested_range_and_wrong_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileArtifactStore::open(directory.path()).unwrap();
    let mut bytes = vec![b'a'; 150_000];
    let artifact = store.store(&bytes, "text/plain").unwrap();
    let hash = artifact.content_hash().as_str();
    let path = directory
        .path()
        .join("artifacts")
        .join(&hash[..2])
        .join(hash);
    bytes[149_999] = b'b';
    fs::write(&path, &bytes).unwrap();
    assert!(matches!(
        store.read_page(&artifact, 0, page_size(1), artifact.size()),
        Err(ArtifactPageError::Store(ArtifactStoreError::Corrupt))
    ));
    bytes[149_999] = b'a';
    fs::write(&path, &bytes).unwrap();
    let wrong_size = Artifact::new(artifact.content_hash().clone(), "text/plain", 149_999).unwrap();
    assert!(matches!(
        store.read_page(&wrong_size, 0, page_size(1), artifact.size()),
        Err(ArtifactPageError::Store(ArtifactStoreError::Corrupt))
    ));
    fs::write(&path, &bytes[..149_999]).unwrap();
    assert!(matches!(
        store.read_page(&artifact, 0, page_size(1), artifact.size()),
        Err(ArtifactPageError::Store(ArtifactStoreError::Corrupt))
    ));
}

#[test]
fn scan_budget_is_checked_before_filesystem_access_and_missing_files_are_distinct() {
    let directory = tempfile::tempdir().unwrap();
    let store = FileArtifactStore::open(directory.path()).unwrap();
    let artifact = store.store(b"result", "text/plain").unwrap();
    let hash = artifact.content_hash().as_str();
    fs::remove_file(
        directory
            .path()
            .join("artifacts")
            .join(&hash[..2])
            .join(hash),
    )
    .unwrap();
    assert!(matches!(
        store.read_page(&artifact, 0, page_size(1), 5),
        Err(ArtifactPageError::LimitExceeded)
    ));
    assert!(
        store
            .read_page(&artifact, 0, page_size(1), 6)
            .unwrap()
            .is_none()
    );
}
