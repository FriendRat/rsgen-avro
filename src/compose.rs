use std::io;
use std::path::Path;
use serde_json;

fn raw_schema_jsons(schemas_dir: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(schemas_dir)? {
        let path = entry?.path();
        if !path.is_dir() {
            
        }
    }
    Ok(())
}