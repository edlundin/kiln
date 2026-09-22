//! Explicit user-selected bundle I/O. No directory traversal or skill extraction.

use kiln_protocol::{CONFIGURATION_PUBLICATION_MAX_BYTES, SharedConfigurationBundle};
use std::{fs::File, io::Read, path::Path};

pub struct BundleSummary {
    pub mcp_servers: usize,
    pub skills: usize,
    pub files: usize,
    pub model_defaults: bool,
}

pub fn read_bundle(path: &Path) -> Result<(SharedConfigurationBundle, BundleSummary), String> {
    let selected =
        std::fs::symlink_metadata(path).map_err(|_| "Could not inspect the selected bundle.")?;
    if !selected.is_file() || selected.file_type().is_symlink() {
        return Err("Choose a regular JSON bundle file.".into());
    }
    #[cfg(unix)]
    let file = File::from(
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| "Could not open the selected bundle as a regular file.")?,
    );
    #[cfg(not(unix))]
    let file = File::open(path).map_err(|_| "Could not open the selected bundle.")?;
    let metadata = file
        .metadata()
        .map_err(|_| "Could not inspect the selected bundle.")?;
    if !metadata.is_file() {
        return Err("Choose a regular JSON bundle file.".into());
    }
    let cap = CONFIGURATION_PUBLICATION_MAX_BYTES;
    if metadata.len() > cap as u64 {
        return Err("The bundle exceeds the 2 MiB transfer limit.".into());
    }
    // The extra byte detects growth beyond the bound without unbounded buffering.
    let mut bytes = Vec::new();
    file.take(cap as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "Could not read the selected bundle.")?;
    if bytes.len() > cap {
        return Err("The bundle exceeds the 2 MiB transfer limit.".into());
    }
    let bundle: SharedConfigurationBundle =
        serde_json::from_slice(&bytes).map_err(|_| "The file is not a configuration bundle.")?;
    let metadata: serde_json::Value = serde_json::from_str(&bundle.metadata_json)
        .map_err(|_| "The bundle metadata is not valid JSON.")?;
    if metadata
        .get("schema_version")
        .and_then(|value| value.as_u64())
        != Some(1)
    {
        return Err("The bundle uses an unsupported configuration schema.".into());
    }
    let mcp_servers = metadata
        .get("mcp_servers")
        .and_then(|value| value.as_array())
        .ok_or("The bundle has no MCP catalog.")?
        .len();
    let model = metadata
        .get("settings")
        .and_then(|value| value.get("model_defaults"))
        .ok_or("The bundle has no shared model settings.")?;
    if !model.is_null() && !model.is_object() {
        return Err("The bundle has invalid model settings.".into());
    }
    let summary = BundleSummary {
        mcp_servers,
        skills: bundle.skills.len(),
        files: bundle.skills.iter().map(|skill| skill.files.len()).sum(),
        model_defaults: !model.is_null(),
    };
    // This is a preview only. The daemon performs full canonical/hash validation.
    Ok((bundle, summary))
}

pub fn save_bundle(path: &Path, bundle: &SharedConfigurationBundle) -> Result<(), String> {
    let parent = path.parent().ok_or("Choose a destination folder.")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| "Could not create the export in the selected folder.")?;
    serde_json::to_writer(temporary.as_file_mut(), bundle)
        .map_err(|_| "Could not write the configuration bundle.")?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|_| "Could not finish writing the bundle.")?;
    temporary.persist_noclobber(path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            "A file already exists there. Choose a new export filename."
        } else {
            "Could not save the configuration bundle."
        }
    })?;
    Ok(())
}
