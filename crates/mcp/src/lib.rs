//! MCP SDK boundary. Startup does not confer permission to execute operations.

mod negotiation;
mod stdio;

pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
pub use stdio::StdioTransport;
