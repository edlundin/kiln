use kiln_core::*;
use serde_json::json;
use std::num::NonZeroUsize;

fn tool(bytes: usize) -> McpCallTool {
    McpCallTool::new(
        NonZeroUsize::new(bytes).unwrap(),
        ModelToolCatalogLimits {
            max_tools: 1,
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
            json!({"server_id":"fixture","definition_version":2,"operation":operation}).to_string();
        let resolver = tool(source.len());
        let definition = &resolver.catalog().definitions()[0];
        let command = resolver.parse_arguments(definition, &source).unwrap();
        assert_eq!(command.server_id().as_str(), "fixture");
        assert_eq!(command.definition_version(), 2);
        assert_eq!(command.canonical_json(), source);
        match command.operation() {
            McpCallOperation::Tool { name, arguments } => {
                assert_eq!(name, "write");
                assert_eq!(arguments["nested"], json!([1, true, null]));
            }
            McpCallOperation::Resource { uri } => assert_eq!(uri, "fixture://document"),
            McpCallOperation::Prompt { name, arguments } => {
                assert_eq!(name, "review");
                assert_eq!(arguments["subject"], "private");
            }
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
            json!({"server_id":"fixture","definition_version":1,"operation":operation}).to_string();
        assert!(matches!(
            resolver.parse_arguments(definition, &source),
            Err(ModelToolArgumentError::InvalidArguments)
        ));
    }
    for version in [json!(0), json!(-1), json!(u64::MAX), json!(1.5), json!("1")] {
        let source = json!({"server_id":"fixture","definition_version":version,"operation":{"kind":"resource","uri":"fixture://document"}}).to_string();
        assert!(resolver.parse_arguments(definition, &source).is_err());
    }
    let source = json!({"server_id":"fixture","definition_version":1,"scope":"core","operation":{"kind":"resource","uri":"fixture://document"}}).to_string();
    assert!(resolver.parse_arguments(definition, &source).is_err());
}
