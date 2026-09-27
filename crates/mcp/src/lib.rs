//! MCP SDK boundary. Startup does not confer permission to execute operations.

mod negotiation;

pub use negotiation::{ProtocolPolicy, ProtocolVersion, start_stdio_client};
