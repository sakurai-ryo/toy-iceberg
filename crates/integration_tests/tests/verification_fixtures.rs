//! Runs the official `apache/iceberg-verification` fixtures. Cases that use types outside this
//! implementation's scope are counted as skipped, and the counts are pinned so that a fixture
//! update or a regression cannot silently shrink coverage.

use serde_json::{Value, json};
use toy_iceberg_core::Error;
use toy_iceberg_core::types::{Schema, Type};

fn cases(path: &str) -> Vec<Value> {
    let file = format!(
        "{}/testdata/iceberg-verification/{path}",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(file).unwrap();
    let doc: Value = serde_json::from_str(&text).unwrap();
    doc["cases"].as_array().unwrap().clone()
}

#[test]
fn primitive_types() {
    let (mut run, mut skipped) = (0, 0);
    for case in cases("types/primitive/cases.json") {
        let id = case["id"].as_str().unwrap();
        let parsed = case["input"].as_str().unwrap().parse::<Type>();
        if case["valid"] == json!(true) {
            match parsed {
                Ok(ty) => {
                    assert_eq!(json!(ty.to_string()), case["canonical"], "{id}");
                    run += 1;
                }
                Err(Error::Unsupported(_)) => skipped += 1,
                Err(e) => panic!("{id}: {e}"),
            }
        } else {
            assert!(parsed.is_err(), "{id} must be rejected");
            run += 1;
        }
    }
    println!("primitive types: {run} run, {skipped} skipped");
    assert_eq!((run, skipped), (19, 11));
}

fn is_flat(schema: &Value) -> bool {
    schema["fields"].as_array().is_some_and(|fields| {
        fields.iter().all(|f| {
            f["type"]
                .as_str()
                .is_some_and(|t| t.parse::<Type>().is_ok())
        })
    })
}

fn project(s: &Schema) -> Value {
    let fields: Vec<Value> = s
        .fields
        .iter()
        .map(|f| {
            json!({
                "id": f.id, "name": f.name, "parent": null, "required": f.required,
                "type": f.ty.to_string(), "doc": f.doc,
            })
        })
        .collect();
    json!({
        "schema-id": s.schema_id,
        "identifier-field-ids": s.identifier_field_ids,
        "fields": fields,
    })
}

#[test]
fn schemas() {
    let (mut run, mut skipped) = (0, 0);
    for case in cases("schema/core/cases.json") {
        let id = case["id"].as_str().unwrap();
        if !is_flat(&case["input"]) {
            skipped += 1;
            continue;
        }
        let parsed = serde_json::from_value::<Schema>(case["input"].clone());
        if case["valid"] == json!(true) {
            let schema = parsed.unwrap_or_else(|e| panic!("{id}: {e}"));
            assert_eq!(project(&schema), case["decoded"], "{id}");
        } else {
            assert!(parsed.is_err(), "{id} must be rejected");
        }
        run += 1;
    }
    println!("schemas: {run} run, {skipped} skipped");
    assert_eq!((run, skipped), (8, 13));
}
