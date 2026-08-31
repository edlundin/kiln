# Rust workspace

Rust crates are added in dependency order. Do not create an empty crate only to
reserve a name.

The planned order is `protocol`, `core`, `adapters`, `providers`, `mcp`,
`plugin-host`, `server`, and `daemon`. The dependency rules are defined in
`docs/architecture/target-monorepo.yaml`.

