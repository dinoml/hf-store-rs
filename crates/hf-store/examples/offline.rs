//! Reopens a filtered request without constructing a network transport.

use std::error::Error;
use std::path::PathBuf;

use hf_store::{CacheMode, FetchRequest, OfflineStore, RepositoryId, RepositorySpec, Revision};

fn main() -> Result<(), Box<dyn Error>> {
    let cache_root = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: offline <huggingface-hub-cache>")?;
    let request = FetchRequest::new(
        RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
        Revision::parse("main")?,
    )
    .allow_patterns(["config.json", "tokenizer.json"]);
    let store = OfflineStore::new(cache_root).cache_mode(CacheMode::Compatible);
    let snapshot = store.open_request(&request)?;

    println!("{}", snapshot.directory().display());
    Ok(())
}
