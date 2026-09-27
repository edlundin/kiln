//! MCP SDK boundary. Startup does not confer permission to execute operations.

#[cfg(unix)]
mod bindings;

#[cfg(unix)]
mod broker;

#[cfg(unix)]
mod catalog;
#[cfg(unix)]
mod catalog_cache;
#[cfg(unix)]
mod catalog_state;

#[cfg(unix)]
pub use catalog::{McpCatalogError, McpCatalogLimits};

#[cfg(unix)]
pub use broker::{
    HttpBrokerLimits, McpBrokerError, McpBrokerLimits, StdioBrokerError, StdioBrokerLimits,
    execute_mcp_call, execute_stdio_call,
};

#[cfg(unix)]
mod continuation;
#[cfg(unix)]
mod discovery;
#[cfg(unix)]
mod dispatch;
#[cfg(unix)]
pub use discovery::{McpCatalogEntries, McpCatalogKind, discover_catalog};
mod elicitation;
#[cfg(unix)]
mod generation;
mod http_bindings;
mod http_client;
mod http_generation;
mod http_negotiation;
mod http_sse;
mod mediation;
pub use mediation::url::McpUrlElicitationConfig;
mod negotiation;
mod schema;
pub use elicitation::{
    McpElicitationDecisionError, McpElicitationError, McpElicitationValidationLimits,
    McpElicitationValidator, decide_elicitation_form,
};
pub use http_bindings::{
    HttpBindingError, HttpLaunchAuthorization, HttpLaunchResources, ResolvedHttpLaunch,
    resolve_persisted_http_launch,
};
pub use http_client::{BoundedHttpClient, McpHttpError, McpHttpLimits};
pub use http_generation::{McpHttpGenerationConfig, McpHttpTransport, http_generation_transport};
pub use http_negotiation::{
    McpHttpClientCleanup, McpHttpStartError, start_http_client, start_managed_http_client,
};
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
pub use generation::{
    McpGeneration, McpGenerationError, StdioGeneration, StdioGenerationError, StdioGenerationLaunch,
};
pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
#[cfg(unix)]
pub use process::{ManagedStdioProcess, StdioProcess, StdioProcessCleanup, StdioProcessConfig};
#[cfg(unix)]
pub use registry::{McpRegistry, McpRegistryError, StdioRegistry, StdioRegistryError};
pub use stdio::StdioTransport;
