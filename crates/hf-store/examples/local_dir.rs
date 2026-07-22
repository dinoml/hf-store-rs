//! Downloads or reuses files and copies them into a caller-owned directory.

use std::error::Error;
use std::path::PathBuf;

use hf_store::{
    CacheMode, FetchOptions, FetchRequest, HubStore, RepositoryId, RepositorySpec, Revision,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = std::env::args_os().skip(1).map(PathBuf::from);
    let cache_root = arguments
        .next()
        .ok_or("usage: local_dir <huggingface-hub-cache> <destination>")?;
    let destination = arguments
        .next()
        .ok_or("usage: local_dir <huggingface-hub-cache> <destination>")?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(materialize(cache_root, destination))
}

async fn materialize(cache_root: PathBuf, destination: PathBuf) -> Result<(), Box<dyn Error>> {
    let request = FetchRequest::new(
        RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
        Revision::parse("main")?,
    )
    .allow_patterns(["config.json", "tokenizer.json"]);
    let store = HubStore::builder()
        .cache_mode(CacheMode::Compatible)
        .cache_root(cache_root)
        .build();

    let local = store
        .fetch_to_local_dir(request, FetchOptions::default(), destination, false)
        .await?;
    println!("{}", local.root().display());
    Ok(())
}
