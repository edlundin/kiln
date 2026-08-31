use std::{fs, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let generated_root = manifest_dir.join("../../protocol/generated");
    kiln_protocol::write_generated_artifacts(generated_root)
        .expect("generated contract is writable");

    let artifacts = kiln_protocol::artifact_files();
    let openapi = artifacts.get("openapi.yaml").expect("OpenAPI artifact");
    fs::write(
        manifest_dir.join("../../docs/protocol/openapi.yaml"),
        openapi,
    )
    .expect("accepted OpenAPI contract is writable");
}
