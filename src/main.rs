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
    let target_files = vec![PathBuf::from("/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc/covid-19-dataset/document_parses/pdf_json/7fefce09c13ca598e5fb20e02c9f19d840bd2a7a.json")];
    let index_reader =
        build_and_commit_corpus(&schema, target_files.into_iter()).expect("Failed to build corpus");
}

fn write_article(
    target_path: &PathBuf,
    schema: &Schema,
    index: &mut IndexWriter,
) -> Result<(), Error> {
    let deserializer = json_parser::get_stream_deserializer(&target_path)?;
    for paper in deserializer {
        println!("{:?}", paper);
        let documents = paper?.convert_to_doc(schema);
        for doc in documents {
            index.add_document(doc);
        }
    }
    index.commit()?;
    Ok(())
}

fn build_and_commit_corpus<K: Iterator<Item = PathBuf>>(
    scheme: &Schema,
    target_files: K,
) -> Result<IndexReader, Error> {
    let index_path = PathBuf::from(format!("{}/ordc_corpus", DIR_PREFIX));
    let index = Index::open_in_dir(&index_path)?;
    let mut writer = index.writer(100_000_000)?;
    for file in target_files {
        write_article(&file, &scheme, &mut writer)?;
    }

    Ok(index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?)
}

fn build_covid_schema() -> Schema {
    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("title", TEXT | STORED);
    schema_builder.add_text_field("author", TEXT);
    schema_builder.add_text_field("paper_id", TEXT | STORED);
    schema_builder.add_text_field("text", TEXT);
    schema_builder.build()
}
