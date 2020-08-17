use failure::Error;
use std::path::PathBuf;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::IndexWriter;
use tantivy::ReloadPolicy;
const DIR_PREFIX: &str = "/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc";

mod json_parser;

fn main() {
    let schema = build_covid_schema();
    let index = build_and_commit_corpus(&schema).expect("Failed to build corpus");
}

fn commit_corpus(
    path: PathBuf,
    index: &mut IndexWriter,
    schema: &Schema,
    main_index: &Index,
) -> Result<IndexReader, Error> {
    let deserializer = json_parser::get_stream_deserializer(&path)?;
    for paper in deserializer {
        let documents = paper?.convert_to_doc(schema);
        for doc in documents {
            index.add_document(doc);
        }
    }
    index.commit()?;
    Ok(main_index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?)
}

fn build_and_commit_corpus(scheme: &Schema) -> Result<IndexReader, Error> {
    let index_path = PathBuf::from(format!("{}/ordc_corpus", DIR_PREFIX));
    let index = Index::create_in_dir(&index_path, scheme.clone())?;
    let mut writer = index.writer(100_000_000)?;
    let reader = commit_corpus(index_path, &mut writer, scheme, &index)?;
    Ok(reader)
}

fn build_covid_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("author", TEXT);
    schema_builder.add_text_field("paper_id", TEXT | STORED);
    schema_builder.add_text_field("text", TEXT);
    schema_builder.build()
}
