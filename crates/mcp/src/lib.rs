//! MCP SDK boundary. Startup does not confer permission to execute operations.

#[cfg(unix)]
mod bindings;

#[cfg(unix)]
mod generation;
mod negotiation;
#[cfg(unix)]
mod process;
#[cfg(unix)]
mod registry;
mod stdio;

#[cfg(unix)]
pub use bindings::{
    ResolvedStdioLaunch, StdioBindingError, StdioHostBindings, StdioLaunchResources,
    resolve_stdio_launch,
};
#[cfg(unix)]
pub use generation::{StdioGeneration, StdioGenerationError, StdioGenerationLaunch};
pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
#[cfg(unix)]
pub use process::{ManagedStdioProcess, StdioProcess, StdioProcessCleanup, StdioProcessConfig};
#[cfg(unix)]
pub use registry::{StdioRegistry, StdioRegistryError};
pub use stdio::StdioTransport;
