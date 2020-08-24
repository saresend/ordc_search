use failure::Error;
use std::path::PathBuf;
use tantivy::schema::*;
use tantivy::Index;
use tantivy::IndexReader;
use tantivy::Searcher;
use tantivy::collector::TopDocs;
use tantivy::IndexWriter;
use warp::Filter;
use warp::filters::query;
use tantivy::ReloadPolicy;
use tantivy::query::PhraseQuery;
use serde::Serialize;
use warp::reply::Reply;

const DIR_PREFIX: &str = "/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc";

mod json_parser;

#[tokio::main]
async fn main() {
    let schema = build_covid_schema();
    let target_files = vec![PathBuf::from("/media/saresend/81e4a1e1-4190-4fbf-bc60-f287f3d935cc/covid-19-dataset/document_parses/pdf_json/7fefce09c13ca598e5fb20e02c9f19d840bd2a7a.json")];
    let index_reader =
        build_and_commit_corpus(&schema, target_files.into_iter()).expect("Failed to build corpus");

    run_server(index_reader, schema).await;
}

fn build_query(schema: &Schema, query: String) -> PhraseQuery {
    let field = schema.get_field("text").unwrap();
    let mut token_vec = vec![];
    for token in query.split(' ') {
        let new_term = tantivy::Term::from_field_text(field, token);
        token_vec.push(new_term);
    }
    PhraseQuery::new(token_vec)
}

fn serialize_documents(schema: &Schema, searcher: &Searcher, docs: &Vec<(f32, tantivy::DocAddress)>) -> Result<impl Serialize, Error> {
    let mut results = vec![];
    for (score, doc_address) in docs {
        let retrieved_doc = searcher.doc(*doc_address)?;
        results.push(schema.to_json(&retrieved_doc));
    }
    Ok(results)
}


async fn run_server(index_reader: IndexReader, schema: Schema) {
    let path = warp::path("/query").and(query::raw()).map(move |query | {
        let searcher = index_reader.searcher();
        let query = build_query(&schema, query);
        let top_docs = searcher.search(&query, &TopDocs::with_limit(20)); 
        match top_docs {
            Ok(top_docs) => {
                let serializable_docs = serialize_documents(&schema, &searcher, &top_docs).unwrap();
                warp::reply::json(&serializable_docs).into_response()
            },
            Err(top_docs) => { warp::reply::with_status(warp::reply(), warp::http::StatusCode::from_u16(500).unwrap()).into_response() },
        }

    });
    warp::serve(path).run(([127,0,0,1], 8080)).await;
}

fn write_article(
    target_path: &PathBuf,
    schema: &Schema,
    index: &mut IndexWriter,
) -> Result<(), Error> {
    let deserializer = json_parser::get_stream_deserializer(&target_path)?;
    for paper in deserializer {
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
