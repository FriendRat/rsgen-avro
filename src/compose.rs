use std::io::{self, Read};
use std::path::Path;
use serde_json::{self, Value, Map};
use std::fs::File;
use std::collections::{HashMap, HashSet, VecDeque};
use std::{fmt, error::Error};


/// Custom errors.
#[derive(Debug)]
pub struct DependencyResolutionError {
    failed_type: String,
    failed_dependency: Option<String>,
    msg: String
}

impl DependencyResolutionError {

    /// Makes the appropriate error message for when the definition for a dependency is not found
    pub fn missing_dependency(failed_type: &str, failed_dependency: &str) -> DependencyResolutionError {
        DependencyResolutionError{
            failed_type: failed_type.to_string(),
            failed_dependency: Some(failed_dependency.to_string()),
            msg: format!("Failed to find the definition of type <{}> which is required for <{}>",
            failed_dependency, failed_type)
        }
    }

    /// Makes the appropriate error message for when a cycle of dependencies is found
    pub fn cyclic_dependency(failed_type: &str) -> DependencyResolutionError {
        DependencyResolutionError{
            failed_type: failed_type.to_string(),
            failed_dependency: None,
            msg: format!("The type definition <{}> appears to be part of a cycle of  dependencies;\
             this is not currently supported.",  failed_type)
        }
    }
}
impl Error for DependencyResolutionError { }

impl fmt::Display for DependencyResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DependencyResolutionError :: {}", self.msg)
    }
}

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
fn build_dependency_tree(raw_schemas: &HashMap<String, Map<String, Value>>) -> [HashMap<String, HashSet<String>>; 2] {

    // Value is all types who need the key as part of their definition
    let mut ancestors: HashMap<String, HashSet<String>> = HashMap::new();
    // Value is all of the types needed for definition of key
    let mut descendants: HashMap<String, HashSet<String>> = HashMap::new();

    for (name, schema_) in raw_schemas.iter() {
        descendants.insert(name.to_owned(), determine_dependencies(schema_));
        let dependencies = descendants.get(name).unwrap();
        if !ancestors.contains_key(name){
            ancestors.insert(name.to_owned(), HashSet::new());
        }

        dependencies.iter().map(|dep| {
            match ancestors.get_mut(dep){
                Some(ancs) => ancs.insert(name.to_owned()),
                None => ancestors.insert(dep.to_owned(), [name.to_owned()].iter().cloned().collect()).is_some()
            };
        }).collect::<()>();
    }

    [ancestors, descendants]
}

/// Given a target Avro schema, looks ups each dependency by name and replaces its name in the
/// schema with the avro json defintion.
///
/// The list of json schemas is provided to this method, as well as the name of one of the schemas
/// whose dependencies are to be resolved. A set of these depedencies is also given. For each
/// dependency in set, its json definition is looked up from the set of raw_schemas and copied in
/// place of the name in the schema definition of target_type.
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
/// let dependencies = determine_dependencies(raw_schemas.get("\"Top\"").unwrap()); // Only dependency is "Bottom"
/// compose("\"Top\"", &mut raw_schemas, &dependencies);
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
/// }
///```
fn compose(target_type: &String,
           raw_schemas: &mut HashMap<String, Map<String, Value>>,
           dependencies: &HashSet<String> ) -> Result<(), DependencyResolutionError>{
    // Copy the schema definitions necessary
    let mut deps : HashMap<&String, Map<String, Value>> = HashMap::new();
    for dep in dependencies.iter() {
        deps.insert(dep,
                    raw_schemas.get(dep)
                        .ok_or(DependencyResolutionError::missing_dependency(target_type, dep))?
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
        },
        "map" => {
            let dep_name = target_schema.get("values").unwrap().to_string();
            target_schema.insert("values".to_string(),
                                 Value::Object(deps.remove(&dep_name).unwrap()));
        },
        "record" => {
            if let Value::Array(inner) = target_schema.get("fields").unwrap().to_owned() {
                for (ix, field) in inner.iter().enumerate() {
                    let dep_name = field.get("type")
                        .unwrap()
                        .to_string();
                    if dependencies.contains(&dep_name) {
                        *target_schema.get_mut("fields").unwrap()[ix].get_mut("type").unwrap() =
                            Value::Object(deps.get(&dep_name).unwrap().to_owned());
                    }
                }
            }

        },
        _ => {}
    }
    Ok(())

}

/// This function composes all the Avro schema jsons found in a given directory so that every type
/// is fully specified.
///
/// First every type is determined along with its ancestors and descendants. Starting with those
/// types that have no descendants (leaves in the dependency grap), it is attempted to resolve all
/// dependencies of the ancestors of the leaves. Then in the next iteration, this is again done with
/// the new set of leaves.
///
/// At each stage, it is checked if any new leaves are created. If none are, the loop is terminated.
/// If not every type is resolved, it implies that a cycle of dependencies exists and an error is
/// raised. Similarly, if a type definition is required, but not found, this error is propagated
/// from the compose function (which is where such an error will be detected).
///
/// However, if every type is resolved, the resulting json schemas will be returned as a HashMap.
pub fn resolve_cross_dependencies(schemas_dir: &Path) -> Result<HashMap<String, Map<String, Value>>, DependencyResolutionError> {
    let mut raw_schema_jsons = raw_schema_jsons(schemas_dir)
        .expect("Failed to read schemas from given directory");

    let [ancestors, descendants] = build_dependency_tree(&raw_schema_jsons);
    // A queue of types whose dependencies will be tried to resolved
    let mut to_be_resolved: VecDeque<&String> = VecDeque::new();

    // We keep a set of types whose dependencies are resolved
    let mut resolved: HashSet<String> = descendants
        .iter()
        .filter_map(|(name, deps)| if deps.is_empty() {Some(name.to_owned())} else {None}).collect();
    // We add the ancestors of the above types to the queue
    for leaf in resolved.iter() {
        let mut next_types: VecDeque<&String> = ancestors.get(leaf)
            .unwrap()
            .iter()
            .filter(|next| !to_be_resolved.contains(next))
            .collect();
        to_be_resolved.append(&mut next_types);
    }

    loop {
        // We perform this loop in iterations. If after any iteration, no progress has been made,
        // We break out of the loop and check that all dependencies have been resolved.
        let mut next_iteration_of_resolution: VecDeque<&String> = VecDeque::new();
        let mut any_resolved = false;
        while let Some(to_resolve) = to_be_resolved.pop_front() {
            let deps = descendants.get(to_resolve).unwrap();
            if deps.is_subset(&resolved) {
                // We can resolve this type as long as all definition are present.
                compose(to_resolve, &mut raw_schema_jsons, &deps)?;
                resolved.insert(to_resolve.to_owned());
                any_resolved = true;
                // Add ancestors of this resolved type to the queue.
                for ancestor in ancestors.get(to_resolve).unwrap() {
                    next_iteration_of_resolution.push_back(ancestor);
                }
            } else {
                // We can't resolve this type yet, add to back of the queue
                next_iteration_of_resolution.push_back(to_resolve);
            }
        }
        if !any_resolved {
            break;
        } else {
            to_be_resolved.append(&mut next_iteration_of_resolution);
        }
    }


    if to_be_resolved.len() > 0 {
        Err(DependencyResolutionError::cyclic_dependency(to_be_resolved.pop_front().unwrap()))
    } else {
        Ok(raw_schema_jsons)
    }

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
        let expected_ancestors: HashMap<String, HashSet<String>> = [("\"UUID\"".to_string(),
                                                                     ["\"Thing\"".to_string(),
                                                                         "\"Other\"".to_string()]
                                                                         .iter().cloned().collect()),
            ("\"Thing\"".to_string(), HashSet::new()), ("\"Other\"".to_string(), HashSet::new())]
            .iter()
            .cloned()
            .collect();
        assert_eq!(ancestors, expected_ancestors);

        let expected_descendants: HashMap<String, HashSet<String>> = [("\"UUID\"".to_string(), HashSet::new()),
            ("\"Thing\"".to_string(), ["\"UUID\"".to_string()].iter().cloned().collect()),
            ("\"Other\"".to_string(), ["\"UUID\"".to_string()].iter().cloned().collect())]
            .iter()
            .cloned()
            .collect();
        assert_eq!(descendants, expected_descendants);
    }

    #[test]
    fn test_compose_record() {
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");
        let dependencies = determine_dependencies(raw_schema_jsons.get("\"Other\"")
            .expect("Test failed"));
        compose(&"\"Other\"".to_string(), &mut raw_schema_jsons, &dependencies)
            .expect("Test failed");

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
            let dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Array\"".to_string(),raw_schema);

            compose(&"\"Array\"".to_string(), &mut raw_schema_jsons, &dependencies)
                .expect("Test failed");

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
            let dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Map\"".to_string(),raw_schema);

            compose(&"\"Map\"".to_string(), &mut raw_schema_jsons, &dependencies)
                .expect("Test failed");

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
            } else {
                panic!("Test failed")
            };

        } else {
            panic!("Test failed");
        }
    }

    #[test]
    fn test_compose_err() {
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_data"))
            .expect("Failed to read json schemas from directory.");

        if let Value::Object(raw_schema) = serde_json::from_str(r#"
        {
            "name": "Map",
            "type": "map",
            "values": "Unknown"
        }
        "#).expect("Test failed") {
            let dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Map\"".to_string(), raw_schema);

            match compose(&"\"Map\"".to_string(),
                                 &mut raw_schema_jsons, &dependencies){
                Ok(_) => panic!("Expected error; test failed"),
                Err(error) => {
                    assert_eq!(error.failed_type, "\"Map\"".to_string());
                    assert_eq!(error.failed_dependency.unwrap(), "\"Unknown\"".to_string());
                }
            };
        } else {
            panic!("Test failed.")
        }
    }

    #[test]
    fn test_resolve_cross_dependencies() -> Result<(), Box<dyn Error>> {
        let raw_schema_jsons = resolve_cross_dependencies(Path::new("test_data"))?;
        for schema in raw_schema_jsons.values() {
            println!("{:?}", schema);
        }
        Ok(())
    }
}
