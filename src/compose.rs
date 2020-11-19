use std::io::{self, Read};
use std::path::Path;
use serde_json::{self, Value, Map};
use std::fs::File;
use std::collections::HashMap;


/// Makes a hashmap indexing the Avro schema jsons by schema name
fn raw_schema_jsons(schemas_dir: &Path) -> io::Result<HashMap<String, Map<String, Value>>> {
    let mut raw_schema_jsons: HashMap<String, Map<String, Value>> = HashMap::new();
    for entry in std::fs::read_dir(schemas_dir)? {
        let path = entry?.path();
        if !path.is_dir() {
            let mut raw_schema = String::new();
            File::open(&path)?.read_to_string(&mut raw_schema)?;
            if let Value::Object(schema_json) = serde_json::from_str(&raw_schema)? {
                raw_schema_jsons.insert(schema_json.get("name")
                                            .expect("Found Avro schema json without a \"name\" field.")
                                            .to_string(),
                                        schema_json);
            };

        }
    }
    Ok(raw_schema_jsons)
}


/// For an given Avro schema json, determines which other Avro schemas its definition relies on
///
/// # Examples:
/// ```rust
/// if let serde_json::Value::Object(map) = serde_json::from_str(&r#"
///         {
///         	"name": "Thing",
///         	"type": "map",
///         	"values": "UUID"
///         }
///         "#).unwrap() {
///     let dependencies = determine_depencies(&map); // dependencies = vec!(serde_json::Value::String("UUID".to_string()))
/// }
///
/// if let serde_json::Value::Object(array) = serde_json::from_str(&r#"
///         {
///         	"name": "Thing",
///         	"type": "array",
///         	"values": "long"
///         }
///         "#).unwrap() {
///     let dependencies = determine_depencies(&array); // dependencies = vec!(serde_json::Value::String("long".to_string()))
/// }
///
/// if let serde_json::Value::Object(record) = serde_json::from_str(&r#"
///     {
///         "name": "Thing",
///         "type", "record",
///         "field": [
///             {"name": "id", "type": "UUID"},
///             {"name": "other", "type": "float"}
///         ]
///     }
///     "#).unwrap() {
///     let dependencies = determine_dependencies(&record); // dependencies = vec!(serde_json::Value::String("UUID".to_string()),
///                                                         //                     serde_json::Value::string("float".to_string()))
/// }
/// ```
///
/// All other input types produce an empty vector of dependencies.
fn determine_dependencies(raw_schema: &Map<String, Value>) -> Vec<&Value> {
    let mut dependencies: Vec<&Value> = Vec::new();
    match raw_schema.get("type")
        .expect("Found Avro schema json without a \"type\" field.")
        .as_str()
        .expect("Failed to parse \"type\" field of Avro schema json."){

        "array" => dependencies.push(raw_schema.get("items")
            .expect("Failed to fetch \"items\" field from an Avro array schema.")),

        "map" => dependencies.push(raw_schema.get("values")
            .expect("Failed to fetch \"values\" field from an Avro map schema.")),

        "record" => {
            if let Value::Array(inner) = raw_schema.get("fields")
                .expect("Failed to fetch \"fields\" field from Avro record schema."){
                for field in inner {
                    dependencies.push(&field.get("type")
                        .expect("Failed to fetch \"type\" from a field of an Avro record schema."));
                }
            }

        }
        _ => {}
    }
    dependencies

}

#[cfg(test)]
mod compose_tests {
    use super::*;

    #[test]
    fn test_raw_jsons() {
        let raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");

        if let Value::Object(expected_uuid) = serde_json::from_str(&r#"
        {
        	"name": "UUID",
        	"type": "record",
        	"fields": [
        		{"name": "bytes", "type": "bytes"}
        	]
        }
        "#).expect("Test failed."){
            assert_eq!(raw_schema_jsons.get("\"UUID\"").expect("Test failed."), &expected_uuid);
        } else {
            panic!("Test failed.");
        }

        if let Value::Object(expected_thing) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": "UUID"},
        		{"name": "other", "type": "float"}
        	]
        }
        "#).expect("Test failed."){
            assert_eq!(raw_schema_jsons.get("\"Thing\"").expect("Test failed."), &expected_thing);
        } else {
            panic!("Test failed.");
        }

        assert_eq!(raw_schema_jsons.len(), 2);
    }

    #[test]
    fn test_deps_from_record() {
        if let Value::Object(record) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": "UUID"},
        		{"name": "other", "type": "float"}
        	]
        }
        "#).expect("Test failed."){
            let dependencies = determine_dependencies(&record);
            assert_eq!(vec!(&Value::String("UUID".to_string()),
                            &Value::String("float".to_string())),
                            dependencies)
        } else {
            panic!("Test failed.")
        }
    }

    #[test]
    fn test_union_dependency() {
        if let Value::Object(record) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": "UUID"},
        		{"name": "other", "type": ["null", "Unknown"]}
        	]
        }
        "#).expect("Test failed"){
            let dependencies = determine_dependencies(&record);
            assert_eq!(vec!(&Value::String("UUID".to_string()),
                            &Value::Array(vec!(Value::String("null".to_string()),
                                               Value::String("Unknown".to_string())))),
                       dependencies);
        } else {
            panic!("Test failed.");
        }
    }


    #[test]
    fn test_array_dependency() {
        if let Value::Object(array) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "array",
        	"items": "UUID"
        }
        "#).expect("Test failed."){
            let dependencies = determine_dependencies(&array);
            assert_eq!(vec!(&Value::String("UUID".to_string())), dependencies)
        } else {
            panic!("Test failed.");
        }

    }

    #[test]
    fn test_map_dependency() {
        if let Value::Object(map) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "map",
        	"values": "UUID"
        }
        "#).expect("Test failed."){
            let dependencies = determine_dependencies(&map);
            assert_eq!(vec!(&Value::String("UUID".to_string())), dependencies)
        } else {
            panic!("Test failed.");
        }
    }

}
