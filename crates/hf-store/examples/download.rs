//! Downloads or reuses a filtered model snapshot in the compatible cache.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use hf_store::{
    CacheMode, FetchOptions, FetchRequest, HubStore, ProgressEvent, ProgressObserver, RepoPath,
    RepositoryId, RepositorySpec, Revision,
};

#[derive(Debug)]
struct Progress;

impl ProgressObserver for Progress {
    fn observe(&self, event: &ProgressEvent) {
        let path = event.path().map_or("-", RepoPath::as_str);
        eprintln!(
            "{:?} {path}: {}/{} bytes ({:?})",
            event.phase(),
            event.transferred_bytes(),
            event.total_bytes().unwrap_or_default(),
            event.reuse()
        );
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let cache_root = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: download <huggingface-hub-cache>")?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(download(cache_root))
}

async fn download(cache_root: PathBuf) -> Result<(), Box<dyn Error>> {
    let request = FetchRequest::new(
        RepositorySpec::model(RepositoryId::parse("openai-community/gpt2")?),
        Revision::parse("main")?,
    )
    .allow_patterns(["config.json", "tokenizer.json"]);
    let store = HubStore::builder()
        .cache_mode(CacheMode::Compatible)
        .cache_root(cache_root)
        .max_concurrent_downloads(4)
        .build();
    let options = FetchOptions::default().progress(Arc::new(Progress));
    let snapshot = store.fetch(request, options).await?;

    println!("commit: {}", snapshot.commit());
    println!("reused complete snapshot: {}", snapshot.was_reused());
    for file in snapshot.files() {
        println!("{} -> {}", file.path(), file.local_path().display());
    }

    // Keep `snapshot` alive for as long as downstream code uses these paths.
    Ok(())
}
