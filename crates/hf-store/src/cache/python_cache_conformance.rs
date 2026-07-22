use std::collections::BTreeSet;
use std::env;
use std::error::Error;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use serde::Deserialize;

use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositoryKind, RepositorySpec, Revision};

use super::compatible_cache::{
    CompatibleCacheOffline, CompatibleSnapshot, CompatibleSnapshotImporter, ExactSelection,
};
use super::hub_cache::HubSnapshotFileForm;
use super::hub_layout::HubCacheLayout;
use super::hub_metadata::decode_tree;
use super::key::BlobDigest;
use super::publication::{
    Effects, NoPublicationFaults, OsFileSystem, RandomOperationIds, SystemClock,
};
use super::standard_cache::StandardCacheWriter;

const INVENTORY_ENV: &str = "HF_STORE_PYTHON_CONFORMANCE_INVENTORY";
const INVENTORY_FORMAT_VERSION: u32 = 1;
const EXPECTED_HUB_COMMIT: &str = "36fd32c84d630f455a23b9a3bc4dc7b76d19cdde";
const EXPECTED_HUB_TAG: &str = "v1.24.0";
const EXPECTED_HUB_VERSION: &str = "1.24.0";

#[derive(Debug, Deserialize)]
struct Provenance {
    format_version: u32,
    git_commit: String,
    git_tag: String,
    package: String,
    package_version: String,
}

#[derive(Debug, Deserialize)]
struct Inventory {
    format_version: u32,
    cache_root: String,
    runtime_symlinks_materialized: bool,
    repositories: Vec<RepositoryFixture>,
}

#[derive(Debug, Deserialize)]
struct RepositoryFixture {
    repo_type: String,
    repo_id: String,
    commit: String,
    refs: Vec<RefFixture>,
    files: Vec<FileFixture>,
}

#[derive(Debug, Deserialize)]
struct RefFixture {
    revision: String,
}

#[derive(Debug, Deserialize)]
struct FileFixture {
    path: String,
    etag: String,
    blob_id: String,
    lfs_sha256: Option<String>,
    lfs_size: Option<u64>,
    size: u64,
    content_sha256: String,
    snapshot_form: String,
}

#[derive(Clone, Debug)]
struct ExpectedFile {
    path: RepoPath,
    hub_blob_key: String,
    size: u64,
    digest: BlobDigest,
    form: HubSnapshotFileForm,
}

struct ExpectedRepository {
    repo_type: &'static str,
    repo_id: &'static str,
    commit: &'static str,
    refs: &'static [&'static str],
    path: &'static str,
    etag: &'static str,
    blob_id: &'static str,
    lfs_sha256: Option<&'static str>,
    size: u64,
    content_sha256: &'static str,
    snapshot_form: &'static str,
}

const EXPECTED_REPOSITORIES: [ExpectedRepository; 3] = [
    ExpectedRepository {
        repo_type: "model",
        repo_id: "fixture-model",
        commit: "1111111111111111111111111111111111111111",
        refs: &["main"],
        path: "config.json",
        etag: "2a90aae99746563212682db129aa431699537cb2",
        blob_id: "2a90aae99746563212682db129aa431699537cb2",
        lfs_sha256: None,
        size: 40,
        content_sha256: "da2dcf17b64bf30e3ac0d1353b6a7fcdbb75a3255c953c8c9c38cb7f4bc92dcc",
        snapshot_form: "snapshot_only_regular",
    },
    ExpectedRepository {
        repo_type: "dataset",
        repo_id: "fixture-org/fixture-dataset",
        commit: "2222222222222222222222222222222222222222",
        refs: &["main", "refs/pr/7"],
        path: "data/train.jsonl",
        etag: "c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844",
        blob_id: "da60683925f47d433804d08c7128d3fd1bd850f1",
        lfs_sha256: Some("c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844"),
        size: 35,
        content_sha256: "c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844",
        snapshot_form: "copied_regular_with_blob",
    },
    ExpectedRepository {
        repo_type: "space",
        repo_id: "fixture-org/fixture-space",
        commit: "3333333333333333333333333333333333333333",
        refs: &["main"],
        path: "src/app.py",
        etag: "8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05",
        blob_id: "8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05",
        lfs_sha256: None,
        size: 49,
        content_sha256: "329ab5ce0c3179d1dfb17f3fddc1c420ec9f04d2ad1e6a3bea07e09b278b806e",
        snapshot_form: "relative_symlink_runtime",
    },
];

#[test]
#[ignore = "invoked only after the pinned Python fixture generator has materialized runtime symlinks"]
fn python_written_cache_reuses_every_repository_without_source() -> Result<(), Box<dyn Error>> {
    let inventory_path = env::var_os(INVENTORY_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| invalid_data(format!("{INVENTORY_ENV} is required")))?;
    let inventory: Inventory = serde_json::from_slice(&fs::read(&inventory_path)?)?;

    let inventory_directory = inventory_path
        .parent()
        .ok_or_else(|| invalid_data("inventory path has no parent directory"))?;
    let provenance: Provenance =
        serde_json::from_slice(&fs::read(inventory_directory.join("provenance.json"))?)?;
    validate_provenance(&provenance)?;
    validate_inventory(&inventory)?;
    let cache_root = resolve_relative(inventory_directory, &inventory.cache_root)?;
    let slash_case = inventory
        .repositories
        .iter()
        .position(|repository| slash_revision(repository).is_some())
        .ok_or_else(|| invalid_data("inventory has no slash-containing revision"))?;

    let mut kinds = BTreeSet::new();
    let order = std::iter::once(slash_case)
        .chain((0..inventory.repositories.len()).filter(|index| *index != slash_case));
    for index in order {
        let repository = inventory
            .repositories
            .get(index)
            .ok_or_else(|| invalid_data("repository order escaped the inventory"))?;
        let kind = exercise_repository(&cache_root, repository)?;
        kinds.insert(kind);
    }

    assert_eq!(
        kinds,
        BTreeSet::from([
            RepositoryKind::Model,
            RepositoryKind::Dataset,
            RepositoryKind::Space,
        ])
    );
    Ok(())
}

fn validate_inventory(inventory: &Inventory) -> Result<(), io::Error> {
    if inventory.format_version != INVENTORY_FORMAT_VERSION {
        return Err(invalid_data(format!(
            "unsupported Python conformance inventory version {}",
            inventory.format_version
        )));
    }
    if !inventory.runtime_symlinks_materialized {
        return Err(invalid_data(
            "Python conformance inventory has no materialized runtime symlinks",
        ));
    }
    if inventory.repositories.len() != 3 {
        return Err(invalid_data(format!(
            "expected three Python cache repositories, found {}",
            inventory.repositories.len()
        )));
    }
    if inventory.cache_root != "cache" {
        return Err(invalid_data("Python conformance cache root changed"));
    }
    for expected in &EXPECTED_REPOSITORIES {
        validate_expected_repository(inventory, expected)?;
    }
    Ok(())
}

fn validate_provenance(provenance: &Provenance) -> Result<(), io::Error> {
    if provenance.format_version != 1
        || provenance.git_commit != EXPECTED_HUB_COMMIT
        || provenance.git_tag != EXPECTED_HUB_TAG
        || provenance.package != "huggingface_hub"
        || provenance.package_version != EXPECTED_HUB_VERSION
    {
        return Err(invalid_data(
            "Python conformance provenance does not match the pinned Hub writer",
        ));
    }
    Ok(())
}

fn validate_expected_repository(
    inventory: &Inventory,
    expected: &ExpectedRepository,
) -> Result<(), io::Error> {
    let repository = inventory
        .repositories
        .iter()
        .find(|repository| {
            repository.repo_type == expected.repo_type && repository.repo_id == expected.repo_id
        })
        .ok_or_else(|| invalid_data("Python conformance repository identity changed"))?;
    let revisions = repository
        .refs
        .iter()
        .map(|reference| reference.revision.as_str())
        .collect::<Vec<_>>();
    if repository.commit != expected.commit || revisions != expected.refs {
        return Err(invalid_data(
            "Python conformance repository revision matrix changed",
        ));
    }
    let [file] = repository.files.as_slice() else {
        return Err(invalid_data(
            "Python conformance repository file matrix changed",
        ));
    };
    let actual_identity = (
        file.path.as_str(),
        file.etag.as_str(),
        file.blob_id.as_str(),
        file.lfs_sha256.as_deref(),
        file.lfs_size,
        file.size,
        file.content_sha256.as_str(),
        file.snapshot_form.as_str(),
    );
    let expected_identity = (
        expected.path,
        expected.etag,
        expected.blob_id,
        expected.lfs_sha256,
        expected.lfs_sha256.map(|_sha256| expected.size),
        expected.size,
        expected.content_sha256,
        expected.snapshot_form,
    );
    if actual_identity != expected_identity {
        return Err(invalid_data(
            "Python conformance repository file identity changed",
        ));
    }
    Ok(())
}

fn exercise_repository(
    cache_root: &Path,
    fixture: &RepositoryFixture,
) -> Result<RepositoryKind, Box<dyn Error>> {
    let kind = parse_repository_kind(&fixture.repo_type)?;
    let spec = RepositorySpec::new(kind, RepositoryId::parse(&fixture.repo_id)?);
    let endpoint = Endpoint::hugging_face();
    let commit = CommitId::parse(&fixture.commit)?;
    let expected = expected_files(fixture)?;
    let paths = expected
        .iter()
        .map(|file| file.path.clone())
        .collect::<Vec<_>>();
    let revision = preferred_revision(fixture)?;
    let layout = HubCacheLayout::shared(cache_root, &endpoint, &spec)?;
    let tree = decode_tree(&fs::read(layout.tree_path(&commit))?)?;

    assert_eq!(tree.files().len(), expected.len());
    for file in &expected {
        let entry = tree
            .files()
            .get(&file.path)
            .ok_or_else(|| invalid_data(format!("tree is missing {}", file.path.as_str())))?;
        assert_eq!(entry.size(), file.size);
    }

    let snapshot_importer =
        CompatibleSnapshotImporter::shared(cache_root, &endpoint, &spec, effects())?;
    let imported_snapshot = snapshot_importer.import(&revision, &paths)?;
    verify_snapshot(&imported_snapshot, &commit, &paths, &expected, true)?;

    let offline = CompatibleCacheOffline::shared(cache_root, &endpoint, &spec, effects())?;
    verify_all_revisions(&offline, fixture, &commit, &paths, &expected)?;

    let writer = StandardCacheWriter::shared(cache_root, &endpoint, &spec, effects())?;
    let mut source_calls = 0_u64;
    let written = writer.publish::<io::Empty, _>(&revision, &commit, &tree, &paths, |path| {
        source_calls = source_calls.saturating_add(1);
        Err(io::Error::other(format!(
            "network source must not be opened for {}",
            path.as_str()
        )))
    })?;
    assert_eq!(source_calls, 0);
    verify_snapshot(&written, &commit, &paths, &expected, false)?;
    verify_all_revisions(&offline, fixture, &commit, &paths, &expected)?;

    Ok(kind)
}

fn verify_all_revisions(
    offline: &CompatibleCacheOffline,
    fixture: &RepositoryFixture,
    commit: &CommitId,
    paths: &[RepoPath],
    expected: &[ExpectedFile],
) -> Result<(), Box<dyn Error>> {
    let immutable = Revision::parse(commit.as_str())?;
    let snapshot = offline.open(&immutable, paths)?;
    verify_snapshot(&snapshot, commit, paths, expected, false)?;
    for reference in &fixture.refs {
        let revision = Revision::parse(&reference.revision)?;
        let snapshot = offline.open(&revision, paths)?;
        verify_snapshot(&snapshot, commit, paths, expected, false)?;
    }
    Ok(())
}

fn verify_snapshot(
    snapshot: &CompatibleSnapshot,
    commit: &CommitId,
    paths: &[RepoPath],
    expected: &[ExpectedFile],
    verify_forms: bool,
) -> Result<(), Box<dyn Error>> {
    let selection = ExactSelection::new(paths)?;
    assert_eq!(snapshot.commit(), commit);
    assert_eq!(snapshot.selection(), selection.id());
    assert_eq!(snapshot.files().len(), expected.len());

    let mut expected = expected.to_vec();
    expected.sort_unstable_by(|left, right| left.path.cmp(&right.path));
    for (actual, expected) in snapshot.files().iter().zip(&expected) {
        assert_eq!(actual.path(), &expected.path);
        assert_eq!(actual.hub_blob_key().as_str(), expected.hub_blob_key);
        assert_eq!(actual.size(), expected.size);
        assert_eq!(actual.digest(), expected.digest);
        assert_eq!(
            BlobDigest::for_bytes(&fs::read(actual.content_path())?),
            expected.digest
        );
        if verify_forms {
            assert_eq!(actual.form(), expected.form);
        }
    }
    Ok(())
}

fn expected_files(fixture: &RepositoryFixture) -> Result<Vec<ExpectedFile>, Box<dyn Error>> {
    fixture
        .files
        .iter()
        .map(|file| {
            Ok(ExpectedFile {
                path: RepoPath::parse(&file.path)?,
                hub_blob_key: file.etag.clone(),
                size: file.size,
                digest: BlobDigest::parse(&file.content_sha256)?,
                form: parse_snapshot_form(&file.snapshot_form)?,
            })
        })
        .collect()
}

fn preferred_revision(fixture: &RepositoryFixture) -> Result<Revision, Box<dyn Error>> {
    let revision = slash_revision(fixture)
        .or_else(|| {
            fixture
                .refs
                .first()
                .map(|reference| reference.revision.as_str())
        })
        .ok_or_else(|| invalid_data("repository fixture has no revision"))?;
    Ok(Revision::parse(revision)?)
}

fn slash_revision(fixture: &RepositoryFixture) -> Option<&str> {
    fixture
        .refs
        .iter()
        .map(|reference| reference.revision.as_str())
        .find(|revision| revision.contains('/'))
}

fn parse_repository_kind(value: &str) -> Result<RepositoryKind, io::Error> {
    match value {
        "model" => Ok(RepositoryKind::Model),
        "dataset" => Ok(RepositoryKind::Dataset),
        "space" => Ok(RepositoryKind::Space),
        unknown => Err(invalid_data(format!(
            "unsupported repository kind {unknown:?}"
        ))),
    }
}

fn parse_snapshot_form(value: &str) -> Result<HubSnapshotFileForm, io::Error> {
    match value {
        "snapshot_only_regular" => Ok(HubSnapshotFileForm::SnapshotOnly),
        "copied_regular_with_blob" => Ok(HubSnapshotFileForm::CopiedWithBlob),
        "relative_symlink_runtime" => Ok(HubSnapshotFileForm::RelativeSymlink),
        unknown => Err(invalid_data(format!(
            "unsupported snapshot form {unknown:?}"
        ))),
    }
}

fn resolve_relative(base: &Path, value: &str) -> Result<PathBuf, io::Error> {
    let relative = Path::new(value);
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid_data(
            "inventory cache root is not a safe relative path",
        ));
    }
    Ok(base.join(relative))
}

fn effects() -> Effects {
    Effects::new(
        Arc::new(OsFileSystem),
        Arc::new(RandomOperationIds),
        Arc::new(SystemClock),
        Arc::new(NoPublicationFaults),
    )
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
