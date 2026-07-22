//! Compile-and-run contract for a cache-only downstream library consumer.

use std::error::Error;

use hf_store::{
    CacheMode, Endpoint, InspectionState, OfflineStore, RepoPath, RepositoryId, RepositorySpec,
    Revision,
};

#[test]
fn cache_only_consumer_gets_typed_reports_without_a_runtime() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::TempDir::new()?;
    let repository = RepositorySpec::model(RepositoryId::parse("owner/model")?);
    let revision = Revision::parse("main")?;
    let path = RepoPath::parse("model.safetensors")?;
    let store = OfflineStore::new(directory.path())
        .endpoint(Endpoint::hugging_face())
        .cache_mode(CacheMode::Owned);

    let inspection = store.inspect(&repository, &revision, std::slice::from_ref(&path));
    assert_eq!(inspection.state(), InspectionState::Incomplete);
    assert!(!store.verify(&repository, &revision, &[path]).is_valid());
    let json = serde_json::to_string(&inspection)?;
    assert!(json.contains("\"schema\":\"hf-store.inspection\""));
    assert!(!json.contains(&directory.path().display().to_string()));
    assert_send(inspection);
    Ok(())
}

fn assert_send<T: Send>(_value: T) {}
