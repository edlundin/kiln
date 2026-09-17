# Protocol sources

The Rust types in `crates/protocol` are the canonical executable wire contract.
`protocol/generated` contains the complete generated contract bundle.
`docs/protocol/openapi.yaml` is the accepted OpenAPI copy for documentation
consumers.

Run `cargo run -p kiln-protocol --bin generate-contract` after a protocol
change. The generator writes both OpenAPI copies. Do not edit either copy by
hand.

The generated bundle and the accepted OpenAPI copy are intentionally ignored
by Git. The generator source and canonical Rust DTOs remain tracked in
`crates/protocol`; regenerate the outputs whenever a fresh clone needs the
protocol artifacts, before running protocol tests or consumers that read
those files.

For a fresh clone, run:

```sh
cargo run -p kiln-protocol --bin generate-contract
```
