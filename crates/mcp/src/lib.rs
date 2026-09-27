//! MCP SDK boundary. Startup does not confer permission to execute operations.

#[cfg(unix)]
mod bindings;

#[cfg(unix)]
mod broker;

#[cfg(unix)]
mod catalog;

#[cfg(unix)]
pub use catalog::{McpCatalogError, McpCatalogLimits};

#[cfg(unix)]
pub use broker::{StdioBrokerError, StdioBrokerLimits, execute_stdio_call};

#[cfg(unix)]
mod discovery;
#[cfg(unix)]
mod dispatch;
#[cfg(unix)]
pub use discovery::{McpCatalogEntries, McpCatalogKind, discover_catalog};
#[cfg(unix)]
mod generation;
mod negotiation;
#[cfg(unix)]
mod output;
#[cfg(unix)]
mod process;
#[cfg(unix)]
mod registry;
mod stdio;

#[cfg(unix)]
pub use bindings::{
    ResolvedStdioLaunch, StdioBindingError, StdioHostBindingReferences, StdioHostBindings,
    StdioLaunchResources, resolve_persisted_stdio_launch, resolve_stdio_launch,
    resolve_stdio_launch_from_vault,
};
#[cfg(unix)]
pub use dispatch::{StdioCallError, StdioCallLimits, StdioCallResult};
#[cfg(unix)]
pub use generation::{StdioGeneration, StdioGenerationError, StdioGenerationLaunch};
pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
#[cfg(unix)]
pub use process::{ManagedStdioProcess, StdioProcess, StdioProcessCleanup, StdioProcessConfig};
#[cfg(unix)]
pub use registry::{StdioRegistry, StdioRegistryError};
pub use stdio::StdioTransport;
