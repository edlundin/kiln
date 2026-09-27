//! MCP SDK boundary. Startup does not confer permission to execute operations.

mod negotiation;
#[cfg(unix)]
mod process;
mod stdio;

pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
#[cfg(unix)]
pub use process::{StdioProcess, StdioProcessConfig};
pub use stdio::StdioTransport;
