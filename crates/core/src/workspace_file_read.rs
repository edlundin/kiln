use serde_json::{Map, Value, json};

use crate::{
    ModelToolArgumentError, ModelToolArgumentResolver, ModelToolCatalog, ModelToolCatalogError,
    ModelToolCatalogLimits, ModelToolDefinition, ModelToolDefinitionInput,
};

pub const WORKSPACE_FILE_READ_CAPABILITY: &str = "kiln.workspace.read_file";
const REVISION: &str = "1";

#[derive(Debug, Clone, Copy)]
pub struct WorkspaceFileReadLimits {
    pub max_path_bytes: usize,
    pub max_file_bytes: usize,
}

/// Parsed data only. Construction is owned by the registered local parser.
pub struct WorkspaceFileReadCommand {
    relative_path: String,
    max_file_bytes: usize,
}

impl WorkspaceFileReadCommand {
    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }
    pub fn max_file_bytes(&self) -> usize {
        self.max_file_bytes
    }
}

pub struct WorkspaceFileReadTool {
    catalog: ModelToolCatalog,
    limits: WorkspaceFileReadLimits,
}

impl WorkspaceFileReadTool {
    pub fn new(
        limits: WorkspaceFileReadLimits,
        catalog_limits: ModelToolCatalogLimits,
    ) -> Result<Self, ModelToolCatalogError> {
        if limits.max_path_bytes == 0
            || limits.max_file_bytes == 0
            || limits.max_file_bytes.checked_add(1).is_none()
        {
            return Err(ModelToolCatalogError::InvalidLimits);
        }
        let schema = json!({
            "type": "object",
            "properties": { "path": { "type": "string", "minLength": 1 } },
            "required": ["path"],
            "additionalProperties": false,
        });
        let Value::Object(input_schema) = schema else {
            unreachable!()
        };
        // Limits are part of the frozen definition, so changing host limits
        // cannot silently reinterpret an already offered tool revision.
        let catalog = ModelToolCatalog::new(
            vec![ModelToolDefinitionInput {
                name: "read_file".into(),
                description: format!(
                    "Read a UTF-8 regular file relative to the approved Workspace directory. \
                 Use a relative path with slash separators and no empty, dot, or parent components. \
                 Symlinks in the file path are not followed. The path must fit in {} UTF-8 bytes \
                 and the file in {} bytes. Oversized or non-UTF-8 files fail without partial content.",
                    limits.max_path_bytes, limits.max_file_bytes
                ),
                capability: WORKSPACE_FILE_READ_CAPABILITY.into(),
                revision: REVISION.into(),
                input_schema,
            }],
            catalog_limits,
        )?;
        Ok(Self { catalog, limits })
    }

    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
}

impl ModelToolArgumentResolver for WorkspaceFileReadTool {
    type Command = WorkspaceFileReadCommand;

    fn definition(&self, capability: &str, revision: &str) -> Option<&ModelToolDefinition> {
        (capability == WORKSPACE_FILE_READ_CAPABILITY && revision == REVISION)
            .then(|| &self.catalog.definitions()[0])
    }

    fn parse_arguments(
        &self,
        definition: &ModelToolDefinition,
        arguments_json: &str,
    ) -> Result<Self::Command, ModelToolArgumentError> {
        if self.catalog.definitions().first() != Some(definition) {
            return Err(ModelToolArgumentError::UnsupportedSchema);
        }
        let mut object: Map<String, Value> = serde_json::from_str(arguments_json)
            .map_err(|_| ModelToolArgumentError::InvalidArguments)?;
        let path = match object.remove("path") {
            Some(Value::String(path)) => path,
            _ => return Err(ModelToolArgumentError::InvalidArguments),
        };
        if !object.is_empty()
            || path.len() > self.limits.max_path_bytes
            || path.contains('\\')
            || path.chars().any(char::is_control)
            || path
                .split('/')
                .any(|component| matches!(component, "" | "." | ".."))
        {
            return Err(ModelToolArgumentError::InvalidArguments);
        }
        Ok(WorkspaceFileReadCommand {
            relative_path: path,
            max_file_bytes: self.limits.max_file_bytes,
        })
    }
}
