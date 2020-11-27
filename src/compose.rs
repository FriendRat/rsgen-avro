use std::io::{self, Read};
use std::path::Path;
use serde_json::{self, Value, Map};
use std::fs::File;
use std::collections::{HashMap, HashSet};


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

/// The Avro primitive types from which more complex types are built
fn is_primitive(ty: &Value) -> bool {
    if let Value::String(type_name) = ty {
        match type_name.as_str() {
            "null"
            | "boolean"
            | "int"
            | "long"
            | "bytes"
            | "string"
            | "float"
            | "double"
            | "enum"
            | "fixed"=> true,
            _ => false
        }
    } else {
        false
    }
}

/// For an given Avro schema json, determines which other Avro schemas its definition relies on
///
/// This function only extracts dependencies on non-primitive Avro types, i.e. complex types built
/// up from primitives. See the `is_primitive` function above for what is defined as a primitive
/// type.
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
///         	"items": "Type"
///         }
///         "#).unwrap() {
///     let dependencies = determine_depencies(&array); // dependencies = vec!(serde_json::Value::String("Type".to_string()))
/// }
///
/// if let serde_json::Value::Object(record) = serde_json::from_str(&r#"
///     {
///         "name": "Thing",
///         "type", "record",
///         "fields": [
///             {"name": "id", "type": "UUID"},
///             {"name": "other", "type": "float"}
///         ]
///     }
///     "#).unwrap() {
///     let dependencies = determine_dependencies(&record); // dependencies = vec!(serde_json::Value::String("UUID".to_string()))
/// }
/// ```
///
/// All other input types produce an empty vector of dependencies.
fn determine_dependencies(raw_schema: &Map<String, Value>) -> HashSet<String> {
    // TODO: Replace vec with set, currently repeated dependencies can appear in output
    let mut dependencies: HashSet<String> = HashSet::new();
    match raw_schema.get("type")
        .expect("Found Avro schema json without a \"type\" field.")
        .as_str()
        .expect("Failed to parse \"type\" field of Avro schema json."){

        "array" => {
            let ty = raw_schema.get("items")
                .expect("Failed to fetch \"items\" field from an Avro array schema.");
            if !is_primitive(ty) {
                dependencies.insert(ty.to_string());
            }
        },

        "map" => {
            let ty = raw_schema.get("values")
            .expect("Failed to fetch \"values\" field from an Avro map schema.");
            if !is_primitive(ty) {
                dependencies.insert(ty.to_string());
            }
        },

        "record" => {
            if let Value::Array(inner) = raw_schema.get("fields")
                .expect("Failed to fetch \"fields\" field from Avro record schema."){
                for field in inner {
                    let ty = &field.get("type")
                        .expect("Failed to fetch \"type\" from a field of an Avro record schema.");
                    if !is_primitive(ty) {
                        dependencies.insert(ty.to_string());
                    }
                }
            }

        }
        _ => {}
    }
    dependencies

}

/// Given a target Avro schema, looks ups each dependency by name and replaces its name in the
/// schema with the avro json defintion.
///
/// The list of json schemas is provided to this method, as well as the name of one of the schemas
/// whose dependencies are to be resolved. A set of these depedencies is also given. For each
/// dependency in set, its json definition is looked up from the set of raw_schemas and copied in
/// place of the name in the schema definition of target_type.
///
/// Furthermore, for each dependency that gets resolved in the target type, that dependencies name
/// is removed from the list of dependencies that was input.
///
///# Example
/// Consider the following Avro schemas
/// ```json
/// // Cotained in some_directory/top.avsc
/// { "name": "Top",
///   "type": "array",
///   "items": "Middle"
/// }
///```
/// ```json
/// // Contained in some_directory/middle.avsc
/// { "name": "Bottom",
///   "type": "map",
///   "value": "float"
/// }
///```
///
///```rust
/// let mut raw_schemas = raw_schema_jsons(std::path::Path::new("some_directory"));
/// let mut  dependencies = determine_dependencies(raw_schemas.get("\"Top\"").unwrap()); // Only dependency is "Bottom"
/// compose("\"Top\"", &mut raw_schemas, &mut dependencies);
///
/// if let serde_json::Value::Object(schema) = serde_json::from_str(r#"
///    { "name": "Top",
///     "type": "array",
///     "items": {
///        "name": "Bottom",
///        "type": "map",
///        "value": "float"
///        }
///    }
/// "#).unwrap() {
///     assert_eq!(&schema, raw_schemas.get("\"Top\"").unwrap());
///     assert!(dependencies.is_empty());
/// }
///```
fn compose(target_type: &String,
           raw_schemas: &mut HashMap<String, Map<String, Value>>,
           dependencies: &mut HashSet<String> ) {
    // Copy the schema definitions necessary
    let mut deps : HashMap<&String, Map<String, Value>> = HashMap::new();
    for dep in dependencies.iter() {
        deps.insert(dep,
                    raw_schemas.get(dep)
                        .expect("Failed to fetch dependency of target type")
                        .to_owned());
    }

    let target_schema = raw_schemas.get_mut(target_type).unwrap();
    // determine the type of the target schema
    let avro_type = target_schema.get("type")
        .expect("Found Avro schema json without a \"type\" field.")
        .as_str()
        .expect("Failed to parse \"type\" field of Avro schema json.");

    match avro_type {
        "array" => {
            let dep_name = target_schema.get("items").unwrap().to_string();
            target_schema.insert("items".to_string(),
                                 Value::Object(deps.remove(&dep_name).unwrap()));
            dependencies.remove(&dep_name);
        },
        "map" => {
            let dep_name = target_schema.get("values").unwrap().to_string();
            target_schema.insert("values".to_string(),
                                 Value::Object(deps.remove(&dep_name).unwrap()));
            dependencies.remove(&dep_name);
        },
        "record" => {
            if let Value::Array(inner) = target_schema.get("fields").unwrap().to_owned() {
                let mut resolved : HashSet<String> = HashSet::new();
                for (ix, field) in inner.iter().enumerate() {
                    let dep_name = field.get("type")
                        .unwrap()
                        .to_string();
                    if dependencies.contains(&dep_name) {
                        *target_schema.get_mut("fields").unwrap()[ix].get_mut("type").unwrap() =
                            Value::Object(deps.get(&dep_name).unwrap().to_owned());
                        resolved.insert(dep_name.to_owned());
                    }
                }
                *dependencies = dependencies.difference(&resolved).cloned().collect();
            }

        },
        _ => {}
    }


}

/// For each schema name, a set of types it depends on and types that depend on it are computed
///
/// The dependencies of the the types defined by the schemas should for a directed acyclic graph
/// (circular dependencies are not supported at this time). The sinks of this graph are those types
/// with no dependencies.
///
/// This graph may be specified by saying for each type TYPE, which types require TYPE in their
/// definition, and which types TYPE requires in its definition. The former types are called
/// ancestors and the later descendants.
///
/// This function returns two HashMaps: ancestors is a HashSet of ancestors for a specified
/// type name and descendants is a HashSet of descendats for a specified type name.
///
///  # Example:
/// Consider the following Avro schemas
/// ```json
/// { "name": "Top",
///   "type": "array",
///   "items": "Middle"
/// }
///
/// { "name": "Middle",
///   "type": "map",
///   "value": "Bottom"
/// }
///
/// { "name": "Bottom",
///   "type": "record",
///   "fields" : [
///     {"name": "id", "type": "fixed" }
///   ]
/// }
/// ```
/// This function returns the following two HashMaps
///
/// ```json
/// // Descendants
/// {
///     "Top": {"Middle"},
///     "Middle": {"Bottom"},
///     "Bottom": {}
/// }
///
/// // Ancestors
/// {
///     "Top": {},
///     "Middle": {"Top"},
///     "Bottom": {"Middle"}
/// }
/// ```
#[allow(unused_must_use)]
fn build_dependency_tree(raw_schemas: &HashMap<String, Map<String, Value>>) -> [HashMap<&String, HashSet<String>>; 2] {

    // Value is all types who need the key as part of their definition
    let mut ancestors: HashMap<&String, HashSet<String>> = HashMap::new();
    // Value is all of the types needed for definition of key
    let mut descendants: HashMap<&String, HashSet<String>> = HashMap::new();

    for (name, schema_) in raw_schemas.iter() {
        descendants.insert(name, determine_dependencies(schema_));
        let dependencies = descendants.get(name).unwrap();
        if !ancestors.contains_key(name){
            ancestors.insert(&name, HashSet::new());
        }

        dependencies.iter().map(|dep| ancestors.get_mut(name)
            .unwrap()
            .insert(dep.to_owned()));
    }
    [ancestors, descendants]
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

        if let Value::Object(expected_other) = serde_json::from_str(&r#"
        {
        	"name": "Other",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": "UUID"},
        		{"name": "other", "type": "UUID"}
        	]
        }
        "#).expect("Test failed."){
            assert_eq!(raw_schema_jsons.get("\"Other\"").expect("Test failed."), &expected_other);
        } else {
            panic!("Test failed.");
        }

        assert_eq!(raw_schema_jsons.len(), 3);
    }

    #[test]
    fn test_deps_from_record() {
        if let Value::Object(record) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": "UUID"},
        		{"name": "other", "type": "float"},
        		{"name": "yet_another", "type": "Unknown"}
        	]
        }
        "#).expect("Test failed."){
            let dependencies = determine_dependencies(&record);
            let expected : HashSet<String> = ["\"UUID\"".to_string(),
                "\"Unknown\"".to_string()].iter().cloned().collect();
            assert_eq!(expected, dependencies)
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
        		{"name": "other", "type": ["null", "float"]}
        	]
        }
        "#).expect("Test failed"){
            let dependencies = determine_dependencies(&record);
            let expected : HashSet<String> = ["\"UUID\"".to_string(),
                "[\"null\",\"float\"]".to_string()].iter().cloned().collect();
            assert_eq!(expected, dependencies);
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
            let expected : HashSet<String> = ["\"UUID\"".to_string()].iter().cloned().collect();
            assert_eq!(expected, dependencies)
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
            let expected : HashSet<String> = ["\"UUID\"".to_string()].iter().cloned().collect();
            assert_eq!(expected, dependencies)
        } else {
            panic!("Test failed.");
        }
    }

    #[test]
    fn test_no_dependency() {
        if let Value::Object(map) = serde_json::from_str(&r#"
        {
        	"name": "Thing",
        	"type": "enum",
        	"symbols": ["One", "Two"]
        }
        "#).expect("Test failed."){
            let dependencies = determine_dependencies(&map);
            let expected: HashSet<String> = HashSet::new();
            assert_eq!(expected, dependencies)
        } else {
            panic!("Test failed.");
        }
    }

    #[test]
    fn test_dependency_graph() {
        let raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");
        let [ancestors, descendants] = build_dependency_tree(&raw_schema_jsons);
        let names = ["\"UUID\"".to_string(), "\"Thing\"".to_string(), "\"Other\"".to_string()];
        let expected_ancestors: HashMap<&String, HashSet<String>> = [(&names[0], HashSet::new()),
            (&names[1], HashSet::new()), (&names[2], HashSet::new())]
            .iter()
            .cloned()
            .collect();
        assert_eq!(ancestors, expected_ancestors);

        let expected_descendants: HashMap<&String, HashSet<String>> = [(&names[0], HashSet::new()),
            (&names[1], ["\"UUID\"".to_string()].iter().cloned().collect()),
            (&names[2], ["\"UUID\"".to_string()].iter().cloned().collect())]
            .iter()
            .cloned()
            .collect();
        assert_eq!(descendants, expected_descendants);
    }

    #[test]
    fn test_compose_record() {
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");
        let mut dependencies = determine_dependencies(raw_schema_jsons.get("\"Other\"")
            .expect("Test failed"));
        compose(&"\"Other\"".to_string(), &mut raw_schema_jsons, &mut dependencies);

        let schema = raw_schema_jsons.get("\"Other\"").expect("Test failed");
        if let Value::Object(expected) = serde_json::from_str(r#"
        {
        	"name": "Other",
        	"type": "record",
        	"fields": [
        		{"name": "id", "type": {"name": "UUID",
                                        "type": "record",
                                        "fields": [
                                        	{"name": "bytes", "type": "bytes"}
                                        ]}},
        		{"name": "other", "type": {"name": "UUID",
                                        "type": "record",
                                        "fields": [
                                        	{"name": "bytes", "type": "bytes"}
                                        ]}}
        	]
        }
        "#).expect("Test failed") {
            assert_eq!(schema, &expected);
            assert!(dependencies.is_empty());
        } else {
            panic!("Test failed")
        };
    }

    #[test]
    fn test_array_compose() {
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");

        if let Value::Object(raw_schema) = serde_json::from_str(r#"
        {
            "name": "Array",
            "type": "array",
            "items": "UUID"
        }
        "#).expect("Test failed") {
            let mut dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Array\"".to_string(),raw_schema);

            compose(&"\"Array\"".to_string(), &mut raw_schema_jsons, &mut dependencies);

            let schema = raw_schema_jsons.get("\"Array\"").expect("Test failed");
            if let Value::Object(expected) = serde_json::from_str(r#"
            {
        	"name": "Array",
        	"type": "array",
        	"items": {"name": "UUID",
                      "type": "record",
                      "fields": [
                      	{"name": "bytes", "type": "bytes"}
                      ]}
            }
            "#).expect("Test failed") {
                assert_eq!(schema, &expected);
                assert!(dependencies.is_empty());
            } else {
                panic!("Test failed")
            };

        } else {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_map_compose() {
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");

        if let Value::Object(raw_schema) = serde_json::from_str(r#"
        {
            "name": "Map",
            "type": "map",
            "values": "UUID"
        }
        "#).expect("Test failed") {
            let mut dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Map\"".to_string(),raw_schema);

            compose(&"\"Map\"".to_string(), &mut raw_schema_jsons, &mut dependencies);

            let schema = raw_schema_jsons.get("\"Map\"").expect("Test failed");
            if let Value::Object(expected) = serde_json::from_str(r#"
            {
        	"name": "Map",
        	"type": "map",
        	"values": {"name": "UUID",
                      "type": "record",
                      "fields": [
                      	{"name": "bytes", "type": "bytes"}
                      ]}
            }
            "#).expect("Test failed") {
                assert_eq!(schema, &expected);
                assert!(dependencies.is_empty());
            } else {
                panic!("Test failed")
            };

        } else {
            panic!("Test failed");
        }
    }
}
