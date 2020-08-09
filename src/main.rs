use std::path::PathBuf;
use tantivy::schema::*;
const DIR_PREFIX: &str = "/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc";

fn main() {
    let schema = build_covid_schema();
}

fn build_covid_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);

    todo!()
}
