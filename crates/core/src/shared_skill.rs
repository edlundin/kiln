//! Portable global skill content. No filesystem traversal, extraction, execution,
//! dependency installation or secret provisioning is performed here.

use crate::ContentHash;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy)]
pub struct SharedSkillLimits {
    pub max_identifier_bytes: usize,
    pub max_version_bytes: usize,
    pub max_dependencies: usize,
    pub max_files: usize,
    pub max_path_bytes: usize,
    pub max_file_bytes: usize,
    pub max_total_file_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedSkillError {
    InvalidLimits,
    InvalidIdentifier,
    InvalidVersion,
    InvalidDependency,
    InvalidPath,
    PathCollision,
    MissingSkillEntry,
    InvalidSkillEntry,
    LimitExceeded,
    HashMismatch,
}

/// Portable IDs are case-sensitive ASCII slugs. They do not identify a local
/// filesystem location, executable, credential, or execution grant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GlobalSkillId(String);
impl GlobalSkillId {
    pub fn parse(value: impl Into<String>, max_bytes: usize) -> Result<Self, SharedSkillError> {
        let value = value.into();
        if max_bytes == 0
            || value.is_empty()
            || value.len() > max_bytes
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'_'
            })
        {
            return Err(SharedSkillError::InvalidIdentifier);
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

pub struct SharedSkillFileInput {
    pub path: String,
    pub content: Vec<u8>,
    pub expected_hash: ContentHash,
}

pub struct SharedSkillPackageInput {
    pub id: GlobalSkillId,
    pub version: String,
    pub enabled: bool,
    pub dependencies: Vec<GlobalSkillId>,
    /// Only regular-file bytes are representable. Filesystem importers must
    /// refuse symlinks and other special entries before reading bytes.
    pub files: Vec<SharedSkillFileInput>,
}

#[derive(Clone)]
pub struct SharedSkillFile {
    path: String,
    content: Vec<u8>,
    content_hash: ContentHash,
}
impl SharedSkillFile {
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn content(&self) -> &[u8] {
        &self.content
    }
    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
}
impl std::fmt::Debug for SharedSkillFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSkillFile")
            .field("bytes", &self.content.len())
            .finish_non_exhaustive()
    }
}

/// A completely validated package. Enabled state is content metadata only;
/// neither it nor a declared dependency grants authority to execute anything.
#[derive(Clone)]
pub struct SharedSkillPackage {
    id: GlobalSkillId,
    version: String,
    enabled: bool,
    dependencies: Vec<GlobalSkillId>,
    files: Vec<SharedSkillFile>,
    content_hash: ContentHash,
    total_file_bytes: usize,
}
impl std::fmt::Debug for SharedSkillPackage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedSkillPackage")
            .field("files", &self.files.len())
            .field("total_file_bytes", &self.total_file_bytes)
            .finish_non_exhaustive()
    }
}
impl SharedSkillPackage {
    pub fn validate(
        input: SharedSkillPackageInput,
        limits: SharedSkillLimits,
    ) -> Result<Self, SharedSkillError> {
        use SharedSkillError as Error;
        limits.validate()?;
        GlobalSkillId::parse(input.id.as_str(), limits.max_identifier_bytes)?;
        if input.version.is_empty()
            || input.version.len() > limits.max_version_bytes
            || !input.version.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::InvalidVersion);
        }
        if input.files.len() > limits.max_files
            || input.dependencies.len() > limits.max_dependencies
        {
            return Err(Error::LimitExceeded);
        }
        let mut dependencies = BTreeSet::new();
        for dependency in input.dependencies {
            GlobalSkillId::parse(dependency.as_str(), limits.max_identifier_bytes)?;
            if dependency == input.id || !dependencies.insert(dependency) {
                return Err(Error::InvalidDependency);
            }
        }
        let mut files = BTreeMap::new();
        let mut folded_paths = BTreeSet::new();
        let mut directory_spellings = BTreeMap::new();
        let mut total_file_bytes = 0usize;
        for file in input.files {
            validate_path(&file.path, limits.max_path_bytes)?;
            for (index, _) in file.path.match_indices('/') {
                let directory = &file.path[..index];
                if directory_spellings
                    .insert(directory.to_ascii_lowercase(), directory.to_owned())
                    .is_some_and(|previous| previous != directory)
                {
                    return Err(Error::PathCollision);
                }
            }
            if !folded_paths.insert(file.path.to_ascii_lowercase()) {
                return Err(Error::PathCollision);
            }
            if file.content.len() > limits.max_file_bytes {
                return Err(Error::LimitExceeded);
            }
            total_file_bytes = total_file_bytes
                .checked_add(file.content.len())
                .ok_or(Error::LimitExceeded)?;
            if total_file_bytes > limits.max_total_file_bytes {
                return Err(Error::LimitExceeded);
            }
            let hash = hash(&file.content);
            if hash != file.expected_hash {
                return Err(Error::HashMismatch);
            }
            files.insert(
                file.path.clone(),
                SharedSkillFile {
                    path: file.path,
                    content: file.content,
                    content_hash: hash,
                },
            );
        }
        // Case-insensitive file/directory conflicts are invalid on every host,
        // independent of the receiver filesystem's case sensitivity.
        for path in &folded_paths {
            for (index, _) in path.match_indices('/') {
                if folded_paths.contains(&path[..index]) {
                    return Err(Error::PathCollision);
                }
            }
        }
        let entry = files.get("SKILL.md").ok_or(Error::MissingSkillEntry)?;
        if std::str::from_utf8(&entry.content)
            .map_err(|_| Error::InvalidSkillEntry)?
            .trim()
            .is_empty()
        {
            return Err(Error::InvalidSkillEntry);
        }
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        let files = files.into_values().collect::<Vec<_>>();
        let mut digest = Sha256::new();
        digest.update(b"kiln:shared-skill:v1\0");
        field(&mut digest, input.id.as_str().as_bytes());
        field(&mut digest, input.version.as_bytes());
        digest.update([u8::from(input.enabled)]);
        digest.update((dependencies.len() as u64).to_be_bytes());
        for dependency in &dependencies {
            field(&mut digest, dependency.as_str().as_bytes());
        }
        digest.update((files.len() as u64).to_be_bytes());
        for file in &files {
            field(&mut digest, file.path.as_bytes());
            digest.update((file.content.len() as u64).to_be_bytes());
            field(&mut digest, file.content_hash.as_str().as_bytes());
        }
        let content_hash = digest_hash(digest);
        Ok(Self {
            id: input.id,
            version: input.version,
            enabled: input.enabled,
            dependencies,
            files,
            content_hash,
            total_file_bytes,
        })
    }
    pub fn id(&self) -> &GlobalSkillId {
        &self.id
    }
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn enabled(&self) -> bool {
        self.enabled
    }
    pub fn dependencies(&self) -> &[GlobalSkillId] {
        &self.dependencies
    }
    pub fn files(&self) -> &[SharedSkillFile] {
        &self.files
    }
    pub fn content_hash(&self) -> &ContentHash {
        &self.content_hash
    }
    pub fn total_file_bytes(&self) -> usize {
        self.total_file_bytes
    }
}

fn validate_path(path: &str, max_bytes: usize) -> Result<(), SharedSkillError> {
    if path.is_empty() || path.len() > max_bytes {
        return Err(SharedSkillError::InvalidPath);
    }
    for segment in path.split('/') {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.ends_with('.')
            || !segment
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(SharedSkillError::InvalidPath);
        }
        let stem = segment
            .split('.')
            .next()
            .unwrap_or_default()
            .to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || (stem.len() == 4
                && (stem.starts_with("COM") || stem.starts_with("LPT"))
                && matches!(stem.as_bytes()[3], b'1'..=b'9'))
        {
            return Err(SharedSkillError::InvalidPath);
        }
    }
    Ok(())
}
fn field(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}
fn hash(bytes: &[u8]) -> ContentHash {
    digest_hash(Sha256::new_with_prefix(bytes))
}
fn digest_hash(digest: Sha256) -> ContentHash {
    // SHA-256's lower-hex representation always satisfies ContentHash's grammar.
    let mut encoded = String::with_capacity(64);
    for byte in digest.finalize() {
        encoded.push(b"0123456789abcdef"[(byte >> 4) as usize] as char);
        encoded.push(b"0123456789abcdef"[(byte & 0x0f) as usize] as char);
    }
    ContentHash::parse(encoded).expect("SHA-256 is a valid content hash")
}

impl SharedSkillLimits {
    pub fn validate(self) -> Result<(), SharedSkillError> {
        if self.max_identifier_bytes == 0
            || self.max_version_bytes == 0
            || self.max_dependencies == 0
            || self.max_files == 0
            || self.max_path_bytes == 0
            || self.max_file_bytes == 0
            || self.max_total_file_bytes == 0
        {
            return Err(SharedSkillError::InvalidLimits);
        }
        Ok(())
    }
}
