//! Observable contract tests for the pinned Python standard-cache corpus.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha1::Sha1;
use sha2::{Digest, Sha256};

const PINNED_COMMIT: &str = "36fd32c84d630f455a23b9a3bc4dc7b76d19cdde";
const FIXTURE_VERSION: &str = "1.24.0";

#[derive(Debug, Deserialize)]
struct Provenance {
    format_version: u64,
    package: String,
    package_version: String,
    git_commit: String,
    git_tag: String,
    writer_sources: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct Inventory {
    format_version: u64,
    cache_root: String,
    runtime_symlinks_materialized: bool,
    repositories: Vec<RepositoryFixture>,
}

#[derive(Debug, Deserialize)]
struct LocalDirInventory {
    format_version: u64,
    local_directories: Vec<LocalDirFixture>,
}

#[derive(Debug, Deserialize)]
struct LocalDirFixture {
    path: String,
    repo_type: String,
    repo_id: String,
    commit: String,
    tree_path: String,
    gitignore_path: String,
    cachedir_tag_path: String,
    files: Vec<LocalDirFileFixture>,
}

#[derive(Debug, Deserialize)]
struct LocalDirFileFixture {
    path: String,
    metadata_path: String,
    etag: String,
    blob_id: String,
    lfs_sha256: Option<String>,
    lfs_size: Option<u64>,
    size: u64,
    content_sha256: String,
    metadata_timestamp: f64,
}

#[derive(Debug, Deserialize)]
struct RepositoryFixture {
    repo_type: String,
    repo_id: String,
    cache_directory: String,
    commit: String,
    refs: Vec<RefFixture>,
    tree_path: String,
    files: Vec<FileFixture>,
    missing_paths: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RefFixture {
    revision: String,
    path: String,
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
    snapshot_form: SnapshotForm,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum SnapshotForm {
    SnapshotOnlyRegular,
    CopiedRegularWithBlob,
    RelativeSymlinkRuntime,
}

#[derive(Debug, Deserialize)]
struct TreeRecord {
    format_version: u64,
    files: BTreeMap<String, TreeFile>,
}

#[derive(Debug, Deserialize)]
struct TreeFile {
    size: u64,
    blob_id: String,
    lfs_sha256: Option<String>,
    lfs_size: Option<u64>,
}

#[derive(Debug)]
struct ExpectedFileRecord {
    repo_type: &'static str,
    repo_id: &'static str,
    path: &'static str,
    etag: &'static str,
    blob_id: &'static str,
    lfs_sha256: Option<&'static str>,
    lfs_size: Option<u64>,
    size: u64,
    content_sha256: &'static str,
    snapshot_form: SnapshotForm,
}

const EXPECTED_FILE_RECORDS: [ExpectedFileRecord; 3] = [
    ExpectedFileRecord {
        repo_type: "model",
        repo_id: "fixture-model",
        path: "config.json",
        etag: "2a90aae99746563212682db129aa431699537cb2",
        blob_id: "2a90aae99746563212682db129aa431699537cb2",
        lfs_sha256: None,
        lfs_size: None,
        size: 40,
        content_sha256: "da2dcf17b64bf30e3ac0d1353b6a7fcdbb75a3255c953c8c9c38cb7f4bc92dcc",
        snapshot_form: SnapshotForm::SnapshotOnlyRegular,
    },
    ExpectedFileRecord {
        repo_type: "dataset",
        repo_id: "fixture-org/fixture-dataset",
        path: "data/train.jsonl",
        etag: "c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844",
        blob_id: "da60683925f47d433804d08c7128d3fd1bd850f1",
        lfs_sha256: Some("c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844"),
        lfs_size: Some(35),
        size: 35,
        content_sha256: "c57254400e0fe6ea150986e0a0e1f94bac4ee4b0bb8dba97a13a9daa044e6844",
        snapshot_form: SnapshotForm::CopiedRegularWithBlob,
    },
    ExpectedFileRecord {
        repo_type: "space",
        repo_id: "fixture-org/fixture-space",
        path: "src/app.py",
        etag: "8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05",
        blob_id: "8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05",
        lfs_sha256: None,
        lfs_size: None,
        size: 49,
        content_sha256: "329ab5ce0c3179d1dfb17f3fddc1c420ec9f04d2ad1e6a3bea07e09b278b806e",
        snapshot_form: SnapshotForm::RelativeSymlinkRuntime,
    },
];

#[derive(Debug)]
struct ExpectedLocalDirFileRecord {
    path: &'static str,
    metadata_path: &'static str,
    etag: &'static str,
    blob_id: &'static str,
    lfs_sha256: Option<&'static str>,
    lfs_size: Option<u64>,
    size: u64,
    content_sha256: &'static str,
}

const EXPECTED_LOCAL_DIR_FILE_RECORDS: [ExpectedLocalDirFileRecord; 2] = [
    ExpectedLocalDirFileRecord {
        path: "config/fixture.json",
        metadata_path: ".cache/huggingface/download/config/fixture.json.metadata",
        etag: "1d3e832db20793bc16ef45d42eace92e9b3d09ef",
        blob_id: "1d3e832db20793bc16ef45d42eace92e9b3d09ef",
        lfs_sha256: None,
        lfs_size: None,
        size: 40,
        content_sha256: "3d3662c17282a5207d8b0959f8cb360938ad1353331188abec48b420da48ddb3",
    },
    ExpectedLocalDirFileRecord {
        path: "weights/nested/model.safetensors",
        metadata_path: ".cache/huggingface/download/weights/nested/model.safetensors.metadata",
        etag: "56b93e7dc344b0707e63012ae0b7bce9c78180a7a416a72237fa01c6f5254184",
        blob_id: "3995220defb3805bcc59e11a08ec28acfbb402e8",
        lfs_sha256: Some("56b93e7dc344b0707e63012ae0b7bce9c78180a7a416a72237fa01c6f5254184"),
        lfs_size: Some(31),
        size: 31,
        content_sha256: "56b93e7dc344b0707e63012ae0b7bce9c78180a7a416a72237fa01c6f5254184",
    },
];

#[test]
fn provenance_pins_the_exact_upstream_writers() -> Result<(), Box<dyn std::error::Error>> {
    let provenance: Provenance = read_json(&fixture_root().join("provenance.json"))?;

    assert_eq!(provenance.format_version, 1);
    assert_eq!(provenance.package, "huggingface_hub");
    assert_eq!(provenance.package_version, FIXTURE_VERSION);
    assert_eq!(provenance.git_commit, PINNED_COMMIT);
    assert_eq!(provenance.git_tag, "v1.24.0");
    assert_eq!(
        provenance.writer_sources,
        BTreeMap::from([
            (
                "src/huggingface_hub/_local_folder.py".to_owned(),
                "2e60361293d0bd45b8a877e55291882144279e42".to_owned(),
            ),
            (
                "src/huggingface_hub/_tree_cache.py".to_owned(),
                "09fe01f8e9d2fb1d7264647e3456e261bed95f50".to_owned(),
            ),
            (
                "src/huggingface_hub/file_download.py".to_owned(),
                "67d60b626fb7a38d330105345930080c7a1cb580".to_owned(),
            ),
        ])
    );

    Ok(())
}

#[test]
fn inventory_covers_the_standard_cache_compatibility_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let inventory = read_inventory()?;

    assert_eq!(inventory.format_version, 1);
    assert_eq!(inventory.cache_root, "cache");
    assert!(!inventory.runtime_symlinks_materialized);
    assert_eq!(
        inventory
            .repositories
            .iter()
            .map(|repository| (
                repository.repo_type.as_str(),
                repository.repo_id.as_str(),
                repository.commit.as_str(),
            ))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            (
                "dataset",
                "fixture-org/fixture-dataset",
                "2222222222222222222222222222222222222222",
            ),
            (
                "model",
                "fixture-model",
                "1111111111111111111111111111111111111111",
            ),
            (
                "space",
                "fixture-org/fixture-space",
                "3333333333333333333333333333333333333333",
            ),
        ])
    );
    assert_eq!(
        inventory
            .repositories
            .iter()
            .map(|repository| repository.repo_type.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["dataset", "model", "space"])
    );
    assert!(
        inventory
            .repositories
            .iter()
            .any(|repository| !repository.repo_id.contains('/'))
    );
    assert!(
        inventory
            .repositories
            .iter()
            .any(|repository| repository.repo_id.contains('/'))
    );
    assert!(inventory.repositories.iter().any(|repository| {
        repository
            .refs
            .iter()
            .any(|reference| reference.revision.contains('/'))
    }));
    assert!(
        inventory
            .repositories
            .iter()
            .any(|repository| { repository.files.iter().any(|file| file.path.contains('/')) })
    );
    assert!(
        inventory
            .repositories
            .iter()
            .all(|repository| !repository.missing_paths.is_empty())
    );
    assert_eq!(
        inventory
            .repositories
            .iter()
            .flat_map(|repository| &repository.files)
            .map(|file| &file.snapshot_form)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            &SnapshotForm::SnapshotOnlyRegular,
            &SnapshotForm::CopiedRegularWithBlob,
            &SnapshotForm::RelativeSymlinkRuntime,
        ])
    );
    Ok(())
}

#[test]
fn inventory_pins_independently_expected_file_records() -> Result<(), Box<dyn std::error::Error>> {
    let inventory = read_inventory()?;

    assert_eq!(
        inventory
            .repositories
            .iter()
            .map(|repository| repository.files.len())
            .sum::<usize>(),
        EXPECTED_FILE_RECORDS.len()
    );
    for repository in &inventory.repositories {
        for file in &repository.files {
            let expected = EXPECTED_FILE_RECORDS.iter().find(|expected| {
                expected.repo_type == repository.repo_type
                    && expected.repo_id == repository.repo_id
                    && expected.path == file.path
            });
            let Some(expected) = expected else {
                panic!(
                    "unexpected fixture file {}/{}/{}",
                    repository.repo_type, repository.repo_id, file.path
                );
            };

            assert_eq!(file.etag, expected.etag);
            assert_eq!(file.blob_id, expected.blob_id);
            assert_eq!(file.lfs_sha256.as_deref(), expected.lfs_sha256);
            assert_eq!(file.lfs_size, expected.lfs_size);
            assert_eq!(file.size, expected.size);
            assert_eq!(file.content_sha256, expected.content_sha256);
            assert_eq!(file.snapshot_form, expected.snapshot_form);
        }
    }

    Ok(())
}

#[test]
fn checked_in_cache_matches_its_inventory() -> Result<(), Box<dyn std::error::Error>> {
    let inventory = read_inventory()?;
    let cache_root = fixture_root().join(&inventory.cache_root);
    let expected_repository_directories = inventory
        .repositories
        .iter()
        .map(|repository| repository.cache_directory.as_str())
        .collect::<BTreeSet<_>>();
    let mut actual_repository_directories = BTreeSet::new();
    for entry in std::fs::read_dir(&cache_root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            actual_repository_directories.insert(entry.file_name().to_string_lossy().into_owned());
        }
    }

    assert_eq!(
        actual_repository_directories,
        expected_repository_directories
            .into_iter()
            .map(str::to_owned)
            .collect()
    );

    for repository in &inventory.repositories {
        assert_eq!(
            repository.cache_directory,
            format!(
                "{}s--{}",
                repository.repo_type,
                repository.repo_id.replace('/', "--")
            )
        );
        let repository_root = cache_root.join(&repository.cache_directory);

        for reference in &repository.refs {
            assert_eq!(reference.path, format!("refs/{}", reference.revision));
            assert_eq!(
                std::fs::read(join_posix(&repository_root, &reference.path))?,
                repository.commit.as_bytes()
            );
        }

        let tree: TreeRecord = read_json(&join_posix(&repository_root, &repository.tree_path))?;
        assert_eq!(tree.format_version, 1);
        assert_eq!(tree.files.len(), repository.files.len());

        for file in &repository.files {
            let Some(tree_file) = tree.files.get(&file.path) else {
                panic!("tree omitted fixture file {}", file.path);
            };
            verify_validator_relationships(file, tree_file);
            verify_file_fixture(&repository_root, &repository.commit, file)?;
        }

        for missing_path in &repository.missing_paths {
            let marker = join_posix(
                &repository_root.join(".no_exist").join(&repository.commit),
                missing_path,
            );
            assert!(
                marker.is_file(),
                "missing marker was not a file: {}",
                marker.display()
            );
            assert_eq!(std::fs::metadata(marker)?.len(), 0);
        }
    }

    assert_no_symlinks(&cache_root)?;
    Ok(())
}

#[test]
fn checked_in_standard_text_outputs_use_lf() -> Result<(), Box<dyn std::error::Error>> {
    let inventory = read_inventory()?;
    let cache_root = fixture_root().join(inventory.cache_root);

    assert_lf_only(&cache_root)?;
    Ok(())
}

#[test]
fn local_dir_inventory_pins_regular_and_lfs_identity_shapes()
-> Result<(), Box<dyn std::error::Error>> {
    let inventory = read_local_dir_inventory()?;

    assert_eq!(inventory.format_version, 1);
    assert_eq!(inventory.local_directories.len(), 1);
    let fixture = &inventory.local_directories[0];
    assert_eq!(fixture.path, "local-dir");
    assert_eq!(fixture.repo_type, "model");
    assert_eq!(fixture.repo_id, "fixture-org/fixture-local-dir");
    assert_eq!(fixture.commit, "4444444444444444444444444444444444444444");
    assert_eq!(
        fixture.tree_path,
        ".cache/huggingface/trees/4444444444444444444444444444444444444444.json"
    );
    assert_eq!(fixture.gitignore_path, ".cache/huggingface/.gitignore");
    assert_eq!(fixture.cachedir_tag_path, ".cache/huggingface/CACHEDIR.TAG");
    assert_eq!(fixture.files.len(), EXPECTED_LOCAL_DIR_FILE_RECORDS.len());

    for file in &fixture.files {
        let expected = EXPECTED_LOCAL_DIR_FILE_RECORDS
            .iter()
            .find(|expected| expected.path == file.path);
        let Some(expected) = expected else {
            panic!("unexpected local_dir fixture file {}", file.path);
        };
        assert_eq!(file.metadata_path, expected.metadata_path);
        assert_eq!(file.etag, expected.etag);
        assert_eq!(file.blob_id, expected.blob_id);
        assert_eq!(file.lfs_sha256.as_deref(), expected.lfs_sha256);
        assert_eq!(file.lfs_size, expected.lfs_size);
        assert_eq!(file.size, expected.size);
        assert_eq!(file.content_sha256, expected.content_sha256);
        assert_eq!(
            file.metadata_timestamp.to_bits(),
            1_720_000_000.25_f64.to_bits()
        );
    }
    assert_eq!(
        fixture
            .files
            .iter()
            .map(|file| file.lfs_sha256.is_some())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([false, true])
    );

    Ok(())
}

#[test]
fn checked_in_local_dir_matches_python_metadata_and_tree() -> Result<(), Box<dyn std::error::Error>>
{
    let inventory = read_local_dir_inventory()?;
    let fixture = &inventory.local_directories[0];
    let local_dir = join_posix(&fixture_root(), &fixture.path);

    assert_eq!(
        std::fs::read(join_posix(&local_dir, &fixture.gitignore_path))?,
        b"*"
    );
    assert_eq!(
        std::fs::read(join_posix(&local_dir, &fixture.cachedir_tag_path))?,
        b"Signature: 8a477f597d28d172789f06886806bc55\n\
# This file is a cache directory tag created by huggingface_hub.\n\
# For information about cache directory tags, see:\n\
#\thttps://bford.info/cachedir/\n"
    );

    let tree: TreeRecord = read_json(&join_posix(&local_dir, &fixture.tree_path))?;
    assert_eq!(tree.format_version, 1);
    assert_eq!(tree.files.len(), fixture.files.len());
    for file in &fixture.files {
        let file_path = join_posix(&local_dir, &file.path);
        let metadata = std::fs::symlink_metadata(&file_path)?;
        assert!(metadata.file_type().is_file());
        assert_eq!(metadata.len(), file.size);
        let content = std::fs::read(&file_path)?;
        assert_eq!(sha256_hex(&content), file.content_sha256);

        let Some(tree_file) = tree.files.get(&file.path) else {
            panic!("local_dir tree omitted fixture file {}", file.path);
        };
        assert_eq!(tree_file.size, file.size);
        assert_eq!(tree_file.blob_id, file.blob_id);
        assert_eq!(tree_file.lfs_sha256, file.lfs_sha256);
        assert_eq!(tree_file.lfs_size, file.lfs_size);

        let metadata_bytes = std::fs::read(join_posix(&local_dir, &file.metadata_path))?;
        assert_eq!(
            metadata_bytes,
            format!(
                "{}\n{}\n{}\n",
                fixture.commit, file.etag, file.metadata_timestamp
            )
            .as_bytes()
        );
        verify_local_dir_content_identity(file, &content);
    }

    assert_no_symlinks(&local_dir)?;
    assert_lf_only(&local_dir.join(".cache").join("huggingface"))?;
    Ok(())
}

fn verify_validator_relationships(file: &FileFixture, tree_file: &TreeFile) {
    assert_eq!(tree_file.size, file.size);
    assert_eq!(tree_file.blob_id, file.blob_id);
    assert_eq!(tree_file.lfs_sha256, file.lfs_sha256);
    assert_eq!(tree_file.lfs_size, file.lfs_size);

    match (&file.lfs_sha256, file.lfs_size) {
        (None, None) => assert_eq!(file.etag, file.blob_id),
        (Some(lfs_sha256), Some(lfs_size)) => {
            assert_eq!(file.etag, *lfs_sha256);
            assert_eq!(file.content_sha256, *lfs_sha256);
            assert_eq!(file.size, lfs_size);
            assert_ne!(file.blob_id, file.etag);
        }
        _ => panic!("LFS SHA-256 and size must be present together"),
    }
}

fn verify_file_fixture(
    repository_root: &Path,
    commit: &str,
    file: &FileFixture,
) -> Result<(), Box<dyn std::error::Error>> {
    let snapshot_path = join_posix(&repository_root.join("snapshots").join(commit), &file.path);
    let blob_path = repository_root.join("blobs").join(&file.etag);

    let content = match file.snapshot_form {
        SnapshotForm::SnapshotOnlyRegular => {
            let content = read_and_verify_regular_file(&snapshot_path, file)?;
            assert!(!blob_path.try_exists()?);
            content
        }
        SnapshotForm::CopiedRegularWithBlob => {
            let snapshot_content = read_and_verify_regular_file(&snapshot_path, file)?;
            let blob_content = read_and_verify_regular_file(&blob_path, file)?;
            assert_eq!(snapshot_content, blob_content);
            snapshot_content
        }
        SnapshotForm::RelativeSymlinkRuntime => {
            assert!(!snapshot_path.try_exists()?);
            read_and_verify_regular_file(&blob_path, file)?
        }
    };
    verify_content_identity(file, &content);

    Ok(())
}

fn read_and_verify_regular_file(
    path: &Path,
    fixture: &FileFixture,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let metadata = std::fs::symlink_metadata(path)?;
    assert!(
        metadata.file_type().is_file(),
        "not a regular file: {}",
        path.display()
    );
    assert_eq!(metadata.len(), fixture.size);
    let content = std::fs::read(path)?;
    assert_eq!(sha256_hex(&content), fixture.content_sha256);
    Ok(content)
}

fn verify_content_identity(file: &FileFixture, content: &[u8]) {
    if let Some(lfs_sha256) = &file.lfs_sha256 {
        assert_eq!(sha256_hex(content), *lfs_sha256);
        assert_eq!(
            git_blob_id(&lfs_pointer(lfs_sha256, file.size)),
            file.blob_id
        );
    } else {
        assert_eq!(git_blob_id(content), file.blob_id);
    }
}

fn verify_local_dir_content_identity(file: &LocalDirFileFixture, content: &[u8]) {
    if let Some(lfs_sha256) = &file.lfs_sha256 {
        assert_eq!(sha256_hex(content), *lfs_sha256);
        assert_eq!(
            git_blob_id(&lfs_pointer(lfs_sha256, file.size)),
            file.blob_id
        );
        assert_eq!(file.etag, *lfs_sha256);
    } else {
        assert_eq!(git_blob_id(content), file.blob_id);
        assert_eq!(file.etag, file.blob_id);
    }
}

fn assert_no_symlinks(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let entry_path = entry.path();
        assert!(
            !file_type.is_symlink(),
            "checked-in symlink: {}",
            entry_path.display()
        );
        if file_type.is_dir() {
            assert_no_symlinks(&entry_path)?;
        }
    }
    Ok(())
}

fn assert_lf_only(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let entry_path = entry.path();
        if file_type.is_dir() {
            assert_lf_only(&entry_path)?;
        } else if file_type.is_file() {
            assert!(
                !std::fs::read(&entry_path)?.contains(&b'\r'),
                "standard text output contains CR bytes: {}",
                entry_path.display()
            );
        }
    }
    Ok(())
}

fn read_inventory() -> Result<Inventory, Box<dyn std::error::Error>> {
    read_json(&fixture_root().join("inventory.json"))
}

fn read_local_dir_inventory() -> Result<LocalDirInventory, Box<dyn std::error::Error>> {
    read_json(&fixture_root().join("local-dir-inventory.json"))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Box<dyn std::error::Error>> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("huggingface_hub-v1.24.0")
}

fn join_posix(root: &Path, relative: &str) -> PathBuf {
    relative
        .split('/')
        .fold(root.to_owned(), |path, part| path.join(part))
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn git_blob_id(bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn lfs_pointer(content_sha256: &str, size: u64) -> Vec<u8> {
    format!(
        "version https://git-lfs.github.com/spec/v1\noid sha256:{content_sha256}\nsize {size}\n"
    )
    .into_bytes()
}
