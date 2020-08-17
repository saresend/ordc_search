// This is the structure for a given paragraph provided by
// the thing
use failure::Error;
use serde::Deserialize;
use serde_json::Deserializer;
use std::io::BufReader;
use std::path::PathBuf;
use tantivy::doc;
use tantivy::schema::Schema;
use tantivy::Document;

#[derive(Deserialize, Debug)]
pub struct PaperStruct {
    paper_id: String,
    metadata: Metadata,
    #[serde(rename = "abstract")]
    paper_abstract: Option<Vec<Paragraph>>,
    body_text: Vec<Paragraph>,
}

#[derive(Deserialize, Debug)]
pub struct Metadata {
    title: String,
    authors: Vec<Author>,
    // TODO: Investigate whether adding biblio metadata could be valuable here, potentially for
    // ranking?
}

#[derive(Deserialize, Debug)]
pub struct Author {
    first: String,
    middle: Vec<String>,
    last: String,
}

#[derive(Deserialize, Debug)]
pub struct Paragraph {
    text: String,
    section: String,
}

use std::mem;

impl PaperStruct {
    // Probably quite a hot path
    pub fn convert_to_doc(mut self, schema: &Schema) -> Vec<Document> {
        // This should be safe, but consider a more consistent way to read from the schema
        let title = schema.get_field("title").unwrap();
        let author = schema.get_field("author").unwrap();
        let paper_id = schema.get_field("paper_id").unwrap();
        let text = schema.get_field("text").unwrap();
        let body_text = mem::replace(&mut self.body_text, vec![]);
        body_text
            .iter()
            .map(move |pgraph| {
                doc!(
                title => self.metadata.title.clone(),
                author => self.metadata.authors[0].first.clone(),
                paper_id => self.paper_id.clone(),
                text => pgraph.text.clone(),
                )
            })
            .collect()
    }
}

pub fn get_stream_deserializer(
    path: &PathBuf,
) -> Result<impl Iterator<Item = Result<PaperStruct, serde_json::error::Error>>, Error> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    Ok(Deserializer::from_reader(reader).into_iter::<PaperStruct>())
}
