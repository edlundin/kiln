//! Composition of Kiln-owned native parsers. No executor or authority factory.

use crate::*;

pub enum NativeToolCommand {
    FileRead(WorkspaceFileReadCommand),
    Mcp(McpCommand),
    ToolOutputPage(ToolOutputPageCommand),
}

pub enum NativeToolExecutionRequest {
    FileRead(ModelToolExecutionRequest<WorkspaceFileReadCommand>),
    Mcp(ModelToolExecutionRequest<McpCommand>),
    ToolOutputPage(ModelToolExecutionRequest<ToolOutputPageCommand>),
}

pub struct NativeTools<'a> {
    file_read: Option<&'a WorkspaceFileReadTool>,
    mcp: Option<&'a McpTools>,
    output_page: Option<&'a ToolOutputPageTool>,
    catalog: ModelToolCatalog,
}

impl<'a> NativeTools<'a> {
    pub fn new(
        file_read: Option<&'a WorkspaceFileReadTool>,
        mcp: Option<&'a McpTools>,
        output_page: Option<&'a ToolOutputPageTool>,
        limits: ModelToolCatalogLimits,
    ) -> Result<Self, ModelToolCatalogError> {
        let catalogs = file_read
            .map(WorkspaceFileReadTool::catalog)
            .into_iter()
            .chain(mcp.map(McpTools::catalog))
            .chain(output_page.map(ToolOutputPageTool::catalog));
        let catalog = ModelToolCatalog::combine(catalogs, limits)?;
        Ok(Self {
            file_read,
            mcp,
            output_page,
            catalog,
        })
    }
    pub fn catalog(&self) -> &ModelToolCatalog {
        &self.catalog
    }
}

impl ModelToolArgumentResolver for NativeTools<'_> {
    type Command = NativeToolCommand;
    fn definition(&self, capability: &str, revision: &str) -> Option<&ModelToolDefinition> {
        self.file_read
            .and_then(|tool| tool.definition(capability, revision))
            .or_else(|| {
                self.mcp
                    .and_then(|tool| tool.definition(capability, revision))
            })
            .or_else(|| {
                self.output_page
                    .and_then(|tool| tool.definition(capability, revision))
            })
    }
    fn parse_arguments(
        &self,
        definition: &ModelToolDefinition,
        arguments_json: &str,
    ) -> Result<Self::Command, ModelToolArgumentError> {
        if definition.capability() == WORKSPACE_FILE_READ_CAPABILITY {
            self.file_read
                .ok_or(ModelToolArgumentError::UnsupportedSchema)?
                .parse_arguments(definition, arguments_json)
                .map(NativeToolCommand::FileRead)
        } else if definition.capability() == TOOL_OUTPUT_PAGE_CAPABILITY {
            self.output_page
                .ok_or(ModelToolArgumentError::UnsupportedSchema)?
                .parse_arguments(definition, arguments_json)
                .map(NativeToolCommand::ToolOutputPage)
        } else {
            self.mcp
                .ok_or(ModelToolArgumentError::UnsupportedSchema)?
                .parse_arguments(definition, arguments_json)
                .map(NativeToolCommand::Mcp)
        }
    }
}
