use failure::Error;
use std::path::PathBuf;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::IndexWriter;
use tantivy::Searcher;
const DIR_PREFIX: &str = "/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc";

mod json_parser;

fn main() {
    let schema = build_covid_schema();
    let index = build_corpus(schema).expect("Failed to build corpus");
    let searcher = commit_corpus(
        PathBuf::from(format!("{}/covid-19-dataset", DIR_PREFIX)),
        &index,
    );
}

fn commit_corpus(path: PathBuf, index: &IndexWriter) -> Result<Searcher, Error> {
    todo!()
}

fn build_corpus(scheme: Schema) -> Result<IndexWriter, Error> {
    let index_path = PathBuf::from(format!("{}/ordc_corpus", DIR_PREFIX));
    let index = Index::create_in_dir(index_path, scheme.clone())?;
    let writer = index.writer(100_000_000)?;
    Ok(writer)
}

fn build_covid_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("author", TEXT);
    schema_builder.add_text_field("paper_id", TEXT | STORED);
    schema_builder.add_text_field("text", TEXT);
    schema_builder.build()
}
