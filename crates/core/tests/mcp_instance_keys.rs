use kiln_core::*;
use ulid::Ulid;

#[test]
fn scoped_keys_round_trip_without_granting_definition_or_owner_authority() {
    let workspace = WorkspaceId::from_ulid(Ulid::generate());
    let root = WorkspaceRootId::from_ulid(Ulid::generate());
    let checkout = WorkspaceCheckout::from_resolved_paths(
        workspace.clone(),
        root,
        "src",
        "/fixture",
        "/fixture/.git",
        FilesystemIdentity::new("fixture-identity").unwrap(),
    )
    .unwrap();
    let limits = McpDefinitionLimits {
        max_key_bytes: 64,
        max_metadata_bytes: 2048,
        max_arguments: 1,
        max_argument_bytes: 64,
        max_environment: 1,
        max_endpoint_bytes: 128,
    };
    for owner in [
        McpInstanceOwner::Core,
        McpInstanceOwner::Session(SessionId::from_ulid(Ulid::generate())),
        McpInstanceOwner::Workspace(workspace),
        McpInstanceOwner::WorkspaceCheckout(checkout),
    ] {
        let definition = McpServerDefinition::new(
            SharedMcpServerInput {
                id: SharedConfigurationKey::parse("fixture", 64).unwrap(),
                enabled: false,
                transport: SharedMcpTransport::Https {
                    endpoint: "https://example.test/mcp".into(),
                    credential_binding: None,
                },
            },
            McpProtocolPolicy::Auto,
            owner.scope(),
            Some(SharedConfigurationKey::parse("profile", 64).unwrap()),
            limits,
        )
        .unwrap();
        let key = McpInstanceKey::new(&definition, owner, 2048).unwrap();
        let bytes = key.canonical_json().as_bytes();
        let parsed = McpInstanceKey::from_canonical_json(bytes, bytes.len()).unwrap();
        assert_eq!(parsed.canonical_json(), key.canonical_json());
        assert!(parsed.owner() == key.owner());
        assert!(McpInstanceKey::from_canonical_json(bytes, bytes.len() - 1).is_err());
        for altered in [
            key.canonical_json().replacen('{', "{\"extra\":true,", 1),
            key.canonical_json()
                .replacen('{', "{\"definition_id\":\"fixture\",", 1),
            format!(" {}", key.canonical_json()),
        ] {
            assert!(McpInstanceKey::from_canonical_json(altered.as_bytes(), 2048).is_err());
        }
        if matches!(key.owner(), McpInstanceOwner::WorkspaceCheckout(_)) {
            let traversal = key.canonical_json().replace(
                "\"relative_directory\":\"src\"",
                "\"relative_directory\":\"../escape\"",
            );
            assert!(McpInstanceKey::from_canonical_json(traversal.as_bytes(), 2048).is_err());
        }
    }
}
