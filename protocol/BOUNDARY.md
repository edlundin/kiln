# Protocol sources

The Rust types in `crates/protocol` are the canonical executable wire contract.
`protocol/generated` contains the complete generated contract bundle.
`docs/protocol/openapi.yaml` is the accepted OpenAPI copy for documentation
consumers.

Run `cargo run -p kiln-protocol --bin generate-contract` after a protocol
change. The generator writes both OpenAPI copies. Do not edit either copy by
hand.
