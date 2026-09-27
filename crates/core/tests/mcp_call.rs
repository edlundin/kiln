use kiln_core::*;
use serde_json::json;
use std::num::NonZeroUsize;

fn canonical(value: &serde_json::Value) -> String {
    let mut value = value.clone();
    value.sort_all_objects();
    value.to_string()
}

fn tool(bytes: usize) -> McpTools {
    McpTools::new(
        NonZeroUsize::new(bytes).unwrap(),
        ModelToolCatalogLimits {
            max_tools: 3,
            max_definition_bytes: usize::MAX,
            max_total_definition_bytes: usize::MAX,
        },
    )
    .unwrap()
}

#[test]
fn compact_call_preserves_operations_and_rejects_ambiguous_or_oversized_input() {
    for operation in [
        json!({"kind":"tool","name":"write","arguments":{"text":"private", "nested":[1,true,null]}}),
        json!({"kind":"resource","uri":"fixture://document"}),
        json!({"kind":"prompt","name":"review","arguments":{"subject":"private"}}),
    ] {
        let source =
            canonical(&json!({"server_id":"fixture","definition_version":2,"operation":operation}));
        let resolver = tool(source.len());
        let definition = &resolver.catalog().definitions()[0];
        let command = resolver.parse_arguments(definition, &source).unwrap();
        assert_eq!(command.server_id().as_str(), "fixture");
        assert_eq!(command.definition_version(), 2);
        assert_eq!(command.canonical_json(), source);
        match command.operation() {
            McpOperation::Tool { name, arguments } => {
                assert_eq!(name, "write");
                assert_eq!(arguments["nested"], json!([1, true, null]));
            }
            McpOperation::Resource { uri } => assert_eq!(uri, "fixture://document"),
            McpOperation::Prompt { name, arguments } => {
                assert_eq!(name, "review");
                assert_eq!(arguments["subject"], "private");
            }
            _ => panic!("expected call operation"),
        }
        let smaller = tool(source.len() - 1);
        assert!(matches!(
            smaller.parse_arguments(&smaller.catalog().definitions()[0], &source),
            Err(ModelToolArgumentError::InvalidArguments)
        ));
        assert!(matches!(
            smaller.parse_arguments(definition, &source),
            Err(ModelToolArgumentError::UnsupportedSchema)
        ));
        let duplicate = source.replacen(
            "\"server_id\":",
            "\"server_id\":\"other\",\"server_id\":",
            1,
        );
        let larger = tool(duplicate.len());
        assert!(
            larger
                .parse_arguments(&larger.catalog().definitions()[0], &duplicate)
                .is_err()
        );
    }
}

#[test]
fn compact_call_rejects_unrecognized_authority_and_wrong_operation_shapes() {
    let resolver = tool(4096); // Fixture envelope budget, not a product default.
    let definition = &resolver.catalog().definitions()[0];
    for operation in [
        json!({"kind":"tool","name":"write","arguments":{},"parallel_safe":true}),
        json!({"kind":"tool","name":"write","arguments":[]}),
        json!({"kind":"resource","uri":"fixture://document","arguments":{}}),
        json!({"kind":"prompt","name":"review","arguments":{"subject":1}}),
        json!({"kind":"tool","name":"bad\nname","arguments":{}}),
        json!({"kind":"sampling","name":"provider","arguments":{}}),
    ] {
        let source =
            canonical(&json!({"server_id":"fixture","definition_version":1,"operation":operation}));
        assert!(matches!(
            resolver.parse_arguments(definition, &source),
            Err(ModelToolArgumentError::InvalidArguments)
        ));
    }
    for version in [json!(0), json!(-1), json!(u64::MAX), json!(1.5), json!("1")] {
        let source = canonical(
            &json!({"server_id":"fixture","definition_version":version,"operation":{"kind":"resource","uri":"fixture://document"}}),
        );
        assert!(resolver.parse_arguments(definition, &source).is_err());
    }
    let source = canonical(
        &json!({"server_id":"fixture","definition_version":1,"scope":"core","operation":{"kind":"resource","uri":"fixture://document"}}),
    );
    assert!(resolver.parse_arguments(definition, &source).is_err());
}

#[test]
fn discovery_has_distinct_capabilities_and_strict_canonical_shapes() {
    let resolver = tool(4096);
    for (name, capability, arguments) in [
        (
            "mcp_search",
            MCP_SEARCH_CAPABILITY,
            json!({"server_id":"fixture","definition_version":1,"kind":"tool","query":"","offset":0,"limit":1}),
        ),
        (
            "mcp_describe",
            MCP_DESCRIBE_CAPABILITY,
            json!({"server_id":"fixture","definition_version":1,"kind":"resource_template","identifier":"notes:///{id}"}),
        ),
    ] {
        let definition = resolver.catalog().find(name).unwrap();
        let command = resolver
            .parse_arguments(definition, &canonical(&arguments))
            .unwrap();
        assert_eq!(command.capability(), capability);
        assert_eq!(command.tool_name(), name);
        assert_eq!(command.canonical_json(), canonical(&arguments));
        assert!(
            resolver
                .parse_arguments(
                    resolver.catalog().find("mcp_call").unwrap(),
                    &canonical(&arguments)
                )
                .is_err()
        );
        for (key, value) in [
            ("kind", json!("sampling")),
            ("scope", json!("core")),
            ("definition_version", json!(0)),
        ] {
            let mut invalid = arguments.clone();
            invalid[key] = value;
            assert!(
                resolver
                    .parse_arguments(definition, &canonical(&invalid))
                    .is_err()
            );
        }
    }
    let definition = resolver.catalog().find("mcp_search").unwrap();
    for (key, value) in [
        ("limit", json!(0)),
        ("offset", json!(-1)),
        ("limit", json!(u64::MAX)),
        ("query", json!("a\nb")),
        ("query", json!(null)),
    ] {
        let mut args = json!({"server_id":"fixture","definition_version":1,"kind":"tool","query":"","offset":0,"limit":1});
        args[key] = value;
        assert!(
            resolver
                .parse_arguments(definition, &canonical(&args))
                .is_err()
        );
    }
}

#[test]
fn discovery_continuations_require_a_snapshot_and_a_new_revision() {
    let resolver = tool(4096);
    let definition = resolver.catalog().find("mcp_search").unwrap();
    assert_eq!(definition.revision(), "2");
    assert!(resolver.definition(MCP_SEARCH_CAPABILITY, "1").is_none());
    assert_eq!(resolver.catalog().find("mcp_call").unwrap().revision(), "1");
    let mut args = json!({"server_id":"fixture","definition_version":1,"kind":"tool","query":"","offset":1,"limit":1});
    assert!(
        resolver
            .parse_arguments(definition, &canonical(&args))
            .is_err()
    );
    args["snapshot"] = "opaque-snapshot".into();
    assert!(
        resolver
            .parse_arguments(definition, &canonical(&args))
            .is_ok()
    );
    for snapshot in [json!(null), json!(""), json!("bad\ntoken"), json!(42)] {
        args["snapshot"] = snapshot;
        assert!(
            resolver
                .parse_arguments(definition, &canonical(&args))
                .is_err()
        );
    }
}
