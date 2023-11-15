// build.rs

use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(&["src/items.proto", "src/value.proto"], &["src/"])?;
    Ok(())
}
