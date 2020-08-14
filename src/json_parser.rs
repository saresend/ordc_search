// This is the structure for a given paragraph provided by
// the thing
use serde::Deserialize;
#[derive(Deserialize)]
pub struct PaperStruct {
    paper_id: String,
    metadata: Metadata,
}

#[derive(Deserialize)]
pub struct Metadata {
    title: String,
    authors: Vec<Author>,
    #[serde(rename = "abstract")]
    paper_abstract: Vec<Paragraph>,
    body_text: Vec<Paragraph>,
    // TODO: Investigate whether adding biblio metadata could be valuable here, potentially for
    // ranking?
}

#[derive(Deserialize)]
pub struct Author {
    first: String,
    middle: Vec<String>,
    last: String,
}

#[derive(Deserialize)]
pub struct Paragraph {
    text: String,
    section: String,
}
