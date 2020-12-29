use std::io::{self, Read};
use std::path::Path;
use serde_json::{self, Value, Map};
use std::fs::File;
use std::collections::{HashMap, HashSet, VecDeque};
use std::{fmt, error::Error};
use thiserror::Error;


/// Custom errors.
#[derive(Debug, Error)]
pub struct DependencyResolutionError {
    failed_type: String,
    failed_dependency: Option<String>,
    pub(crate) msg: String
}

#[derive(Eq, PartialEq, Debug)]
enum ResolutionErrorCause {
    Cyclic,
    Missing(String),
}

type SchemaMap = HashMap<String, Map<String, Value>>;
pub type SchemaGraph = HashMap<String, HashSet<String>>;

/// Wrapper around a nullable HashSet pointer that provides a default value that
/// always "contains" Avro primitive types
///
/// The null case is used when no cross dependencies are present and thus this set should "contain"
/// all  types. Only if working with cross-dependencies do we place restrictions on those
/// dependencies of a type which are eligible for code generation (so that they do not get defined
/// more than once if they are a dependency of multiple types).
#[derive(Default)]
pub struct Dependencies<'a> {
    deps: Option<&'a HashSet<String>>
}

impl<'a> Dependencies<'a> {
    fn new(deps: &HashSet<String>) -> Dependencies {
        Dependencies{deps: Some(deps)}
    }

    pub fn contains(&self, value: &String) -> bool {
        match value.as_str() {
            "null"
            | "boolean"
            | "int"
            | "long"
            | "bytes"
            | "string"
            | "float"
            | "double"
            | "enum"
            | "fixed" => true,
            _ => {
                if let Some(inner) = self.deps {
                    inner.contains(value)
                } else {
                    true
                }
            }
        }
    }
}


/// A wrapper around a HashSet that implements a default dict like interface with a non-standard
/// default we wish to use (see the Dependencies type).
#[derive(Default, Debug)]
pub struct DependenciesMap {
    deps: Option<SchemaGraph>
}

impl DependenciesMap {
    pub fn new(deps: SchemaGraph) -> DependenciesMap {
        DependenciesMap{deps: Some(deps)}
    }

    pub fn get(&self, key: &String) -> Option<Dependencies> {
        if let Some(inner) = &self.deps {
            inner.get(key).map(|values| Dependencies::new(values))
        } else {
            None
        }
    }
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
    pub fn cyclic_dependency(failed_type: String) -> DependencyResolutionError {
        DependencyResolutionError{
            failed_type: failed_type.clone(),
            failed_dependency: None,
            msg: format!("The type definition <{}> appears to be part of a cycle of dependencies; \
             this is not currently supported.",  &failed_type)
        }
    }
}

impl fmt::Display for DependencyResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DependencyResolutionError :: {}", self.msg)
    }
}


/// Makes a hashmap indexing the Avro schema jsons by schema name
fn raw_schema_jsons(schemas_dir: &Path) -> io::Result<SchemaMap> {
    let mut raw_schema_jsons: SchemaMap = HashMap::new();
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
    if raw_schema_jsons.is_empty() {
        panic!("Could not find any files in this directory containing AVRO schemas");
    }
    Ok(raw_schema_jsons)
}

/// The Avro primitive types from which more complex types are built
pub fn is_primitive(ty: &Value) -> bool {
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
                    if let Value::Array(inner_types) = ty {
                        for inner_ty in inner_types {
                            if !is_primitive(inner_ty) {
                                dependencies.insert(inner_ty.to_string());
                            }
                        }
                    } else {
                        if !is_primitive(ty) {
                            dependencies.insert(ty.to_string());
                        }
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
/// ```text
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
/// ```text
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
fn build_dependency_tree(raw_schemas: &SchemaMap) -> [SchemaGraph; 2] {

    // Value is all types who need the key as part of their definition
    let mut ancestors: SchemaGraph = HashMap::new();
    // Value is all of the types needed for definition of key
    let mut descendants: SchemaGraph = HashMap::new();

    for (name, schema_) in raw_schemas.iter() {
        descendants.insert(name.to_owned(), determine_dependencies(schema_));
        let dependencies = descendants.get(name).unwrap();
        if !ancestors.contains_key(name){
            ancestors.insert(name.to_owned(), HashSet::new());
        }

        dependencies.iter().map(|dep| {
            match ancestors.get_mut(dep){
                Some(ancs) => ancs.insert(name.to_owned()),
                None => ancestors.insert(dep.to_owned(), [name.to_owned()]
                    .iter()
                    .cloned()
                    .collect())
                    .is_some()
            };
        }).collect::<()>();
    }

    [ancestors, descendants]
}

/// Computes a directed subgraph of the dependency tree that is
///  a) acyclic as a directed graph
///  b) a spanning forest of the graph when viewed as undirected
///
/// It satisfies the property that each tree in the forest has a source from which all other nodes
///  in said tree are reachable.
///
/// Each type in the graph may have multiple sources as ancestors. This makes a choice of
/// designated source (among the possible ancestors) for each type. This prevents types from being
/// defined more than once later on.
#[allow(unused_must_use)]
fn designated_source_per_type(ancestors: &SchemaGraph, descendants: &SchemaGraph)
                              -> Result<SchemaGraph, ResolutionErrorCause> {

    let sources: HashSet<String> = ancestors
        .iter()
        .filter_map(|(name, ancs)| if ancs.len() == 0 { Some(name) } else {None} )
        .cloned()
        .collect();

    if sources.len() == 0 {
        return Err(ResolutionErrorCause::Cyclic);
    }
    // Creates the subgraph using breadth-first search
    let source_deps = bfs(&sources, descendants);
    Ok(source_deps)

}

/// Performs directed breadth-first search from each source of the graph and returns edges visited.
fn bfs(sources: &HashSet<String>, descendants: &SchemaGraph) -> SchemaGraph {
    let mut queue: VecDeque<&String> = VecDeque::new();
    let mut visited: HashSet<&String> = HashSet::new();
    let mut source_deps: SchemaGraph = HashMap::new();

    for source in sources {
        let mut deps = HashSet::new();

        queue.push_back(source);
        visited.insert(source);
        deps.insert(source.to_owned());
        while let Some(node) = queue.pop_front() {
            for descendant in descendants.get(node)
                .expect("Unexpected missing dependencies! \
                (Note: using namespaces can cause this error)"){
                if !visited.contains(descendant) {
                    visited.insert(descendant);
                    deps.insert(descendant.to_owned());
                    queue.push_back(descendant);
                }
            }
        }
        source_deps.insert(source.to_owned(), deps);
    }
    source_deps
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
/// ```text
/// // Cotained in some_directory/top.avsc
/// { "name": "Top",
///   "type": "array",
///   "items": "Middle"
/// }
///```
/// ```text
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
           raw_schemas: &mut SchemaMap,
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
                    let dep_name = field.get("type").unwrap();
                    if let Value::Array(inner_types ) = dep_name {
                        let mut resolved_types: Vec<Value> = Vec::new();
                        for inner_ty in inner_types {
                            if dependencies.contains(&inner_ty.to_string()) {
                                resolved_types.push(Value::Object(deps.get(&inner_ty.to_string())
                                    .unwrap()
                                    .to_owned()));
                            } else if is_primitive(&inner_ty) {
                                resolved_types.push(inner_ty.clone());
                            }
                        }
                        *target_schema.get_mut("fields").unwrap()[ix].get_mut("type").unwrap() =
                            Value::Array(resolved_types);

                    } else{
                        if dependencies.contains(&dep_name.to_string()) {
                            *target_schema.get_mut("fields").unwrap()[ix].get_mut("type").unwrap() =
                                Value::Object(deps.get(&dep_name.to_string()).unwrap().to_owned());
                        }
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
/// If not every type is resolved, it implies that a cycle of dependencies exists or that a type
/// definition is required, but not found. If so, an error is raised saying the type the failed and
/// whether is a) is part of a cycle of dependencies or b) needs a type definition that is missing
/// and the name of the missing type
///
/// However, if every type is resolved, the resulting json schemas will be returned as a HashMap.
pub fn resolve_cross_dependencies(schemas_dir: &Path) -> Result<(SchemaMap, DependenciesMap), DependencyResolutionError> {
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
        to_be_resolved.append(&mut next_iteration_of_resolution);
        if !any_resolved {
            break;
        }
    }

    if let Some(first) = to_be_resolved.pop_front() {
        match resolution_failure_reason(&raw_schema_jsons, [&ancestors, &descendants]) {
            ResolutionErrorCause::Missing(type_name) => {
                Err(DependencyResolutionError::missing_dependency(
                    ancestors.get(&type_name)
                        .unwrap()
                        .iter()
                        .next()
                        .unwrap()
                        .as_str(),
                    type_name.as_str()))
            },
            ResolutionErrorCause::Cyclic => {
                Err(DependencyResolutionError::cyclic_dependency(first.to_string()))
            }
        }
    } else {
        let mut source_deps = designated_source_per_type(&ancestors, &descendants)
            .expect("Unexpected cyclic dependency found!");
        scrub_names(&mut source_deps);
        raw_schema_jsons.retain(|name, _| source_deps.contains_key(name));
        Ok((raw_schema_jsons, DependenciesMap::new(source_deps)))
    }
}

/// Determines the reason the resolution of dependencies fails. Can be either from cycle of
/// dependencies or because a type definition was missing.
///
/// All required type definitions are checked and if any are missing, this is returned as the reason
/// along with the name of the missing type. Otherwise, the reason given is the presence of a cycle
/// of dependencies. It is the job of the calling function to determine a type in this cycle and
/// communicate its name in the error message.
fn resolution_failure_reason(raw_schema_jsons: &SchemaMap, graph: [&SchemaGraph; 2]) -> ResolutionErrorCause {
    let defined_types: HashSet<&String> = raw_schema_jsons.keys().collect();

    for direction in &graph {
        for neighbors in direction.values() {
            let nghbrs: HashSet<&String> = neighbors.iter().collect();
            if let Some(missing) = nghbrs.difference(&defined_types).next() {
                return ResolutionErrorCause::Missing((*missing).to_owned());
            }
        }
    }
    ResolutionErrorCause::Cyclic
}

/// Removes the \" character from the names.
fn scrub_names(schema_graph: &mut SchemaGraph) {
    for schema_list in schema_graph.values_mut() {
        *schema_list = schema_list.iter().map(|name| name.replace("\"", "")).collect();
    }

}


#[cfg(test)]
mod compose_tests {
    use super::*;
    use std::io::Write;

    fn setup(path: &Path) -> Result<(), Box<dyn Error>> {
        std::fs::create_dir(path)?;
        let mut file_uuid = File::create(path.join("UUID.avsc"))?;
        file_uuid.write_all(br#"{"name": "UUID","type": "record",
        "fields": [{"name": "bytes", "type": "bytes"}]}"#)?;
        let mut file_thing = File::create(path.join("Thing.avsc"))?;
        file_thing.write_all(br#"{
	"name": "Thing",
	"type": "record",
	"fields": [{"name": "id", "type": "UUID"},{"name": "other", "type": "float"}]}"#)?;
        let mut file_other = File::create(path.join("Other.avsc"))?;
        file_other.write_all(br#"{
	"name": "Other",
	"type": "record",
	"fields": [
		{"name": "id", "type": "UUID"},
		{"name": "other", "type": "UUID"}]}"#)?;
        Ok(())
    }

    fn teardown(path : &Path) -> Result<(), Box<dyn Error>> {
        std::fs::remove_dir_all(path)?;
        Ok(())
    }

    #[test]
    fn test_raw_jsons()-> Result<(), Box<dyn Error>> {
        setup(Path::new("test_raw_jsons"))?;
        let raw_schema_jsons = raw_schema_jsons(Path::new("test_raw_jsons"))
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
            assert_eq!(raw_schema_jsons.get("\"UUID\"").expect("Test failed."),
                       &expected_uuid);
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
            assert_eq!(raw_schema_jsons.get("\"Thing\"").expect("Test failed."),
                       &expected_thing);
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
            assert_eq!(raw_schema_jsons.get("\"Other\"").expect("Test failed."),
                       &expected_other);
        } else {
            panic!("Test failed.");
        }

        assert_eq!(raw_schema_jsons.len(), 3);
        teardown(Path::new("test_raw_jsons"))
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
    fn test_union_dependency(){
        if let Value::Object(union) = serde_json::from_str(&r#"
        {
            "name": "Record",
            "type": "record",
            "fields": [
            {"name": "union", "type": ["A", "B"]}
            ]
        }
        "#).expect("Test failed") {
            let dependencies = determine_dependencies(&union);
            let expected: HashSet<String> = ["\"A\"".to_string(), "\"B\"".to_string()].iter().cloned().collect();
            assert_eq!(expected, dependencies);
        }  else {
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
    fn test_dependency_graph() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_dependency_graph"))?;
        let raw_schema_jsons = raw_schema_jsons(Path::new("test_dependency_graph"))
            .expect("Failed to read json schemas from directory.");
        let [ancestors, descendants] = build_dependency_tree(&raw_schema_jsons);
        let expected_ancestors: SchemaGraph =
            [("\"UUID\"".to_string(), ["\"Thing\"".to_string(), "\"Other\"".to_string()]
                .iter().cloned().collect()),
                ("\"Thing\"".to_string(), HashSet::new()), ("\"Other\"".to_string(), HashSet::new())]
                .iter().cloned().collect();
        assert_eq!(ancestors, expected_ancestors);

        let expected_descendants: HashMap<String, HashSet<String>> =
            [("\"UUID\"".to_string(), HashSet::new()),
                ("\"Thing\"".to_string(), ["\"UUID\"".to_string()].iter().cloned().collect()),
                ("\"Other\"".to_string(), ["\"UUID\"".to_string()].iter().cloned().collect())]
                .iter().cloned().collect();
        assert_eq!(descendants, expected_descendants);
        teardown(Path::new("test_dependency_graph"))
    }

    #[test]
    fn test_compose_record() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_compose_record"))?;
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_compose_record"))
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
        teardown(Path::new("test_compose_record"))
    }

    #[test]
    fn test_array_compose() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_array_compose"))?;
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_array_compose"))
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
        teardown(Path::new("test_array_compose"))
    }

    #[test]
    fn test_map_compose() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_map_compose"))?;
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_map_compose"))
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
        teardown(Path::new("test_map_compose"))
    }

    #[test]
    fn test_union_compose() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_union_compose"))?;
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_union_compose"))
            .expect("Failed to read json schemas from directory.");

        if let Value::Object(raw_schema) = serde_json::from_str(r#"
        {
            "name": "Union",
            "type": "record",
            "fields": [
            {"name": "nullable", "type": ["null", "UUID"]}
            ]
        }
        "#).expect("Test failed") {
            let dependencies = determine_dependencies(&raw_schema);
            raw_schema_jsons.insert("\"Union\"".to_string(),raw_schema);

            compose(&"\"Union\"".to_string(), &mut raw_schema_jsons, &dependencies)
                .expect("Test failed");

            let schema = raw_schema_jsons.get("\"Union\"").expect("Test failed");
            if let Value::Object(expected) = serde_json::from_str(r#"
            {
        	"name": "Union",
        	"type": "record",
        	"fields":[
        	    { "name": "nullable",
        	      "type": [ "null",
        	        {"name": "UUID",
                     "type": "record",
                     "fields": [
                      	{"name": "bytes", "type": "bytes"}
                      ]
                    }
                  ]
                }
             ]
            }
            "#).expect("Test failed") {
                assert_eq!(schema, &expected);
            } else {
                panic!("Test failed")
            };

        } else {
            panic!("Test failed");
        }
        teardown(Path::new("test_union_compose"))
    }

    #[test]
    fn test_compose_err() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_compose_err"))?;
        let mut raw_schema_jsons = raw_schema_jsons(Path::new("test_compose_err"))
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
        teardown(Path::new("test_compose_err"))
    }

    #[test]
    fn test_resolve_cross_dependencies() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_resolve_cross_dependencies"))?;
        let (raw_schema_jsons, _) = resolve_cross_dependencies(Path::new("test_resolve_cross_dependencies"))?;
        if let Value::Object(thing_expected) = serde_json::from_str(r#"
            {
              "name": "Thing",
              "type": "record",
              "fields": [
                {
                  "name": "id",
                  "type": {
                  	"name": "UUID",
                    "type": "record",
                    "fields": [{"name": "bytes", "type": "bytes"}]
                  }
                },
                {"name": "other","type": "float"}
              ]
            }
        "#).expect("Test failed"){
            assert_eq!(&thing_expected, raw_schema_jsons.get("\"Thing\"").unwrap());
        } else {
            panic!("Test failed");
        }

        if let Value::Object(other_expected) = serde_json::from_str(r#"
        {
          "name": "Other",
          "type": "record",
          "fields": [
            {
              "name": "id",
              "type": {
              	"name": "UUID",
                "type": "record",
                "fields": [{"name": "bytes","type": "bytes"}]
              }
            },
            {
              "name": "other",
              "type": {
              	"name": "UUID",
                "type": "record",
                "fields": [{"name": "bytes", "type": "bytes"}]
              }
            }
          ]
        }
        "#).expect("Test failed") {
            assert_eq!(&other_expected, raw_schema_jsons.get("\"Other\"").unwrap());
        }

        assert_eq!(raw_schema_jsons.len(), 2);
        teardown(Path::new("test_resolve_cross_dependencies"))
    }

    #[test]
    fn test_missing_dependency() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_missing_dependency"))?;
        let mut incomplete = File::create(Path::new("test_missing_dependency/incomplete.avsc"))?;
        incomplete.write_all(br#"{"name": "incomplete",
         "type": "record",
         "fields": [{"name": "thing", "type": "Thing"}, {"name": "unknown", "type": "Unknown"}]
         }
         "#)?;

        let err = resolve_cross_dependencies(Path::new("test_missing_dependency"))
            .expect_err("Test failed");

        assert_eq!(err.failed_type, "\"incomplete\"");
        assert_eq!(err.failed_dependency, Some("\"Unknown\"".to_string()));
        teardown(Path::new("test_missing_dependency"))
    }

    #[test]
    fn test_missing_dependency_reason() -> Result<(), Box<dyn Error>> {
        setup(Path::new("test_missing_dependency_reason"))?;
        let mut incomplete = File::create(Path::new("test_missing_dependency_reason/incomplete.avsc"))?;
        incomplete.write_all(br#"{"name": "incomplete",
         "type": "record",
         "fields": [{"name": "thing", "type": "Thing"}, {"name": "unknown", "type": "Unknown"}]
         }
         "#)?;

        let raw_schema_jsons = raw_schema_jsons(Path::new("test_missing_dependency_reason"))
            .expect("Test failed");
        let graph = build_dependency_tree(&raw_schema_jsons);

        assert_eq!(resolution_failure_reason(&raw_schema_jsons, [&graph[0], &graph[1]]),
                   ResolutionErrorCause::Missing("\"Unknown\"".to_string()));

        teardown(Path::new("test_missing_dependency_reason"))
    }

    #[test]
    fn test_cyclic_dependency() -> Result<(), Box<dyn Error>> {
        let path = Path::new("test_cyclic_dependency");
        std::fs::create_dir(path)?;
        let mut file_uuid = File::create(path.join("UUID.avsc"))?;
        file_uuid.write_all(br#"{"name": "UUID","type": "record",
        "fields": [{"name": "bytes", "type": "bytes"}]}"#)?;
        let mut file_thing = File::create(path.join("Thing.avsc"))?;
        file_thing.write_all(br#"{
	"name": "Thing",
	"type": "record",
	"fields": [{"name": "id", "type": "UUID"},{"name": "other", "type": "Other"}]}"#)?;
        let mut file_other = File::create(path.join("Other.avsc"))?;
        file_other.write_all(br#"{
	"name": "Other",
	"type": "record",
	"fields": [
		{"name": "thing", "type": "Thing"}]}"#)?;

        let err = resolve_cross_dependencies(path).expect_err("Test failed");
        assert_eq!(err.failed_type, "\"Thing\"");
        assert_eq!(err.failed_dependency, None);
        teardown(path)

    }

    #[test]
    fn test_cyclic_dependency_reason() -> Result<(), Box<dyn Error>>{
        let path = Path::new("test_cyclic_dependency_reason");
        std::fs::create_dir(path)?;
        let mut file_uuid = File::create(path.join("UUID.avsc"))?;
        file_uuid.write_all(br#"{"name": "UUID","type": "record",
        "fields": [{"name": "bytes", "type": "bytes"}]}"#)?;
        let mut file_thing = File::create(path.join("Thing.avsc"))?;
        file_thing.write_all(br#"{
	"name": "Thing",
	"type": "record",
	"fields": [{"name": "id", "type": "UUID"},{"name": "other", "type": "Other"}]}"#)?;
        let mut file_other = File::create(path.join("Other.avsc"))?;
        file_other.write_all(br#"{
	"name": "Other",
	"type": "record",
	"fields": [
		{"name": "id", "type": "UUID"},
		{"name": "thing", "type": "Thing"}]}"#)?;

        let raw_json_schemas = raw_schema_jsons(path).expect("Test failed");
        let graph = build_dependency_tree(&raw_json_schemas);
        assert_eq!(resolution_failure_reason(&raw_json_schemas, [&graph[0], &graph[1]]),
                   ResolutionErrorCause::Cyclic);
        teardown(path)
    }

    #[test]
    #[allow(unused_must_use)]
    fn test_remove_two_cycle() {
        let mut ancestors: SchemaGraph =
            [("Thing".to_string(),[ "Other".to_string()]
                .iter().cloned().collect()),
                ("Other".to_string(), [ "Thing".to_string()]
                    .iter().cloned().collect())]
                .iter().cloned().collect();

        let mut descendants: SchemaGraph = ancestors.clone();
        designated_source_per_type(&mut ancestors, &mut descendants).expect_err("Test failed");
    }

    #[test]
    fn test_designated_source_per_type() {
        let descendants: SchemaGraph =
            [("A".to_string(),[ "B".to_string(), "C".to_string()]
                .iter().cloned().collect()),
                ("B".to_string(), [ "D".to_string()]
                    .iter().cloned().collect()),
                ("C".to_string(), [ "D".to_string()]
                    .iter().cloned().collect()),
                ("D".to_string(), HashSet::new())]
                .iter().cloned().collect();

        let ancestors: SchemaGraph =
            [("D".to_string(),[ "B".to_string(), "C".to_string()]
                .iter().cloned().collect()),
                ("B".to_string(), [ "A".to_string()]
                    .iter().cloned().collect()),
                ("C".to_string(), [ "A".to_string()]
                    .iter().cloned().collect()),
                ("A".to_string(), HashSet::new())]
                .iter().cloned().collect();

        let source_deps = designated_source_per_type(&ancestors, &descendants)
            .expect("Test failed");
        let expected: SchemaGraph = [("A".to_string(),
                                      ["A".to_string(), "B".to_string(), "C".to_string(), "D".to_string()]
                                          .iter().cloned().collect())]
            .iter().cloned().collect();
        assert_eq!(expected, source_deps);

    }

    #[test]
    fn test_good_dependency_order() {
        let descendants: SchemaGraph =
            [("A".to_string(),[ "C".to_string(), "D".to_string()]
                .iter().cloned().collect()),
                ("B".to_string(), [ "D".to_string(), "E".to_string()]
                    .iter().cloned().collect()),
                ("C".to_string(), HashSet::new()),
                ("D".to_string(), HashSet::new()),
                ("E".to_string(), HashSet::new())]
                .iter().cloned().collect();

        let ancestors: SchemaGraph =
            [("D".to_string(),[ "A".to_string(), "B".to_string()]
                .iter().cloned().collect()),
                ("E".to_string(), [ "B".to_string()]
                    .iter().cloned().collect()),
                ("C".to_string(), [ "A".to_string()]
                    .iter().cloned().collect()),
                ("A".to_string(), HashSet::new()),
                ("B".to_string(), HashSet::new())]
                .iter().cloned().collect();
        let source_deps = designated_source_per_type(&ancestors, &descendants)
            .expect("Test failed");
        let expected_1: SchemaGraph =
            [("A".to_string(), ["A".to_string(), "C".to_string(), "D".to_string()]
                .iter().cloned().collect()),
                ("B".to_string(), ["B".to_string(), "E".to_string()]
                    .iter().cloned().collect())]
                .iter().cloned().collect();
        let expected_2: SchemaGraph =
            [("A".to_string(), ["A".to_string(), "C".to_string()]
                .iter().cloned().collect()),
                ("B".to_string(), ["B".to_string(), "D".to_string(), "E".to_string()]
                    .iter().cloned().collect())]
                .iter().cloned().collect();
        assert!(source_deps == expected_1 || source_deps == expected_2)
    }
}
