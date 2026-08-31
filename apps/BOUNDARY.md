# Applications

Applications are protocol clients. They do not own domain state, persistence,
permissions, scheduling, or process execution.

- `desktop` is the first graphical client. It is a native Rust application
  built with GPUI and `gpui-component`.
- `cli` is the official Rust protocol client.

Both official clients use the shared Rust `client` crate and the public
HTTP/WebSocket protocol. Neither client imports the core domain.
