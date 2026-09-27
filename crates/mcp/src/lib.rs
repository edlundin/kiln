//! MCP SDK boundary. Startup does not confer permission to execute operations.

#[cfg(unix)]
mod generation;
mod negotiation;
#[cfg(unix)]
mod process;
mod stdio;

#[cfg(unix)]
pub use generation::{StdioGeneration, StdioGenerationError, StdioGenerationLaunch};
pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
#[cfg(unix)]
pub use process::{ManagedStdioProcess, StdioProcess, StdioProcessCleanup, StdioProcessConfig};
pub use stdio::StdioTransport;
