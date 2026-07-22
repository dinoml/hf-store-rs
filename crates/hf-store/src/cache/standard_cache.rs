use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::compatible_cache::{
    CompatibleCacheError, CompatibleCacheOffline, CompatibleSnapshot, ExactSelection,
    publish_manifest,
};
#[cfg(unix)]
use super::hub_cache::canonical_relative_link_target;
use super::hub_cache::{
    HubCacheIndex, HubCacheReadError, HubCacheReader, HubSnapshotFileForm, compatible_blob_key,
    copy_and_validate_content,
};
use super::hub_layout::{HubBlobKey, HubCacheLayout};
use super::hub_metadata::{HubTree, HubTreeEntry, encode_tree};
use super::key::BlobDigest;
use super::metadata::SnapshotFileRecord;
use super::publication::{CacheError, CacheKernel, Effects};
#[cfg(unix)]
use super::rooted_fs::RelativeSymlinkOutcome;
use super::rooted_fs::{CreateOnceOutcome, RootedFileSystem, RootedRegularFile};

pub(super) const CACHEDIR_TAG: &[u8] = b"Signature: 8a477f597d28d172789f06886806bc55\n\
# This file is a cache directory tag created by huggingface_hub.\n\
# For information about cache directory tags, see:\n\
#\thttps://bford.info/cachedir/\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SnapshotMaterialization {
    Auto,
    Copy,
}

#[derive(Clone, Debug)]
pub(super) struct StandardCacheWriter {
    layout: HubCacheLayout,
    reader: HubCacheReader,
    sidecar: CacheKernel,
    root: Arc<dyn RootedFileSystem>,
    effects: Effects,
    materialization: SnapshotMaterialization,
}

impl StandardCacheWriter {
    pub(super) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
    ) -> Result<Self, CompatibleCacheError> {
        Self::shared_with_materialization(
            root,
            endpoint,
            spec,
            effects,
            SnapshotMaterialization::Auto,
        )
    }

    fn shared_with_materialization(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
        materialization: SnapshotMaterialization,
    ) -> Result<Self, CompatibleCacheError> {
        let layout = HubCacheLayout::shared(root, endpoint, spec)?;
        let authority = effects
            .open_cache_authority(layout.capability_root())
            .map_err(CacheError::from)?;
        let reader = HubCacheReader::from_layout(layout.clone(), authority.reader())?;
        let root = authority.writer();
        let sidecar =
            CacheKernel::for_compatible_cache(&layout, Arc::clone(&root), effects.clone())?;
        Ok(Self {
            layout,
            reader,
            sidecar,
            root,
            effects,
            materialization,
        })
    }

    pub(super) fn publish<R, F>(
        &self,
        revision: &Revision,
        commit: &CommitId,
        full_tree: &HubTree,
        paths: &[RepoPath],
        mut open_source: F,
    ) -> Result<CompatibleSnapshot, CompatibleCacheError>
    where
        R: Read,
        F: FnMut(&RepoPath) -> io::Result<R>,
    {
        validate_revision_commit(revision, commit)?;
        if CommitId::parse(revision.as_str()).is_err() {
            let _validated_ref_path = self.layout.ref_path(revision)?;
        }
        let plan = WritePlan::new(full_tree, paths)?;
        let (prepared, mut cleanup) =
            self.prepare_blobs(commit, full_tree, &plan, &mut open_source)?;
        let result = self.publish_prepared(revision, commit, full_tree, &plan, &prepared);
        cleanup.remove_all();
        result.map_err(CompatibleCacheError::with_may_have_published)
    }

    fn prepare_blobs<R, F>(
        &self,
        commit: &CommitId,
        full_tree: &HubTree,
        plan: &WritePlan,
        open_source: &mut F,
    ) -> Result<(BTreeMap<HubBlobKey, PreparedBlob>, StagingCleanup), CompatibleCacheError>
    where
        R: Read,
        F: FnMut(&RepoPath) -> io::Result<R>,
    {
        let mut cleanup = StagingCleanup::new(Arc::clone(&self.root));
        let mut prepared = BTreeMap::new();
        let reusable_index = match self.reader.index_from_tree(commit, full_tree) {
            Ok(index) => Some(index),
            Err(error) if error.is_incomplete() => None,
            Err(error) => return Err(error.into()),
        };
        for (key, blob) in &plan.blobs {
            let blob_relative = self.relative(&self.layout.blob_path(key))?;
            let prepared_blob = match self
                .root
                .open_regular(&blob_relative)
                .map_err(CacheError::from)?
            {
                RootedRegularFile::File { mut reader, size } => {
                    if size != blob.entry.size() {
                        return Err(CompatibleCacheError::corrupt());
                    }
                    let (actual_size, digest) =
                        copy_and_validate_content(reader.as_mut(), &mut io::sink(), &blob.entry)?;
                    PreparedBlob {
                        entry: blob.entry.clone(),
                        size: actual_size,
                        digest,
                        staging: None,
                    }
                }
                RootedRegularFile::Missing => self.prepare_missing_blob(
                    blob,
                    reusable_index.as_ref(),
                    open_source,
                    &mut cleanup,
                )?,
                RootedRegularFile::Other => return Err(CompatibleCacheError::corrupt()),
            };
            prepared.insert(key.clone(), prepared_blob);
        }
        Ok((prepared, cleanup))
    }

    fn prepare_missing_blob<R, F>(
        &self,
        blob: &PlannedBlob,
        reusable_index: Option<&HubCacheIndex>,
        open_source: &mut F,
        cleanup: &mut StagingCleanup,
    ) -> Result<PreparedBlob, CompatibleCacheError>
    where
        R: Read,
        F: FnMut(&RepoPath) -> io::Result<R>,
    {
        let cached = if let Some(index) = reusable_index {
            match self.reader.read_snapshot_file(index, &blob.source_path) {
                Ok(file) => Some((index, file)),
                Err(error) if error.is_missing() || error.is_incomplete() => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        if let Some((_index, file)) = cached.as_ref() {
            if file.form() != HubSnapshotFileForm::SnapshotOnly {
                return Ok(PreparedBlob {
                    entry: blob.entry.clone(),
                    size: file.size(),
                    digest: file.digest(),
                    staging: None,
                });
            }
        }

        let staging_name = self.effects.next_staging_name().map_err(CacheError::from)?;
        let staging = self.layout.staged_blob(&staging_name);
        let staging_relative = self.relative(&staging)?;
        self.ensure_parent(&staging)?;
        let mut output = self
            .root
            .create_new(&staging_relative)
            .map_err(CacheError::from)?;
        cleanup.track(staging_relative.clone());
        let copied = if let Some((index, file)) = cached {
            let copied = self.reader.copy_regular_snapshot_content(
                index,
                &blob.source_path,
                output.as_mut(),
            );
            match copied {
                Ok((size, digest)) if size == file.size() && digest == file.digest() => {
                    Ok((size, digest))
                }
                Ok((_size, _digest)) => Err(HubCacheReadError::corrupt()),
                Err(error) => Err(error),
            }
        } else {
            let mut source = open_source(&blob.source_path).map_err(CacheError::from)?;
            copy_and_validate_content(&mut source, output.as_mut(), &blob.entry)
        };
        let (actual_size, digest) = match copied {
            Ok(validated) => validated,
            Err(error) => {
                drop(output);
                return Err(error.into());
            }
        };
        output.sync_all().map_err(CacheError::from)?;
        drop(output);
        Ok(PreparedBlob {
            entry: blob.entry.clone(),
            size: actual_size,
            digest,
            staging: Some(staging_relative),
        })
    }

    fn publish_prepared(
        &self,
        revision: &Revision,
        commit: &CommitId,
        full_tree: &HubTree,
        plan: &WritePlan,
        prepared: &BTreeMap<HubBlobKey, PreparedBlob>,
    ) -> Result<CompatibleSnapshot, CompatibleCacheError> {
        self.initialize_python_layout()?;
        self.publish_blobs(prepared)?;
        let index = self.publish_tree(commit, full_tree)?;
        self.root
            .ensure_dir(&self.relative(&self.layout.snapshot_directory(commit))?)
            .map_err(CacheError::from)?;
        self.materialize_snapshot(&index, plan, prepared)?;
        self.sidecar.initialize()?;
        for (key, blob) in prepared {
            self.sidecar
                .publish_hub_blob_binding(key, blob.digest, blob.size)?;
        }
        let records = plan
            .files
            .iter()
            .map(|file| {
                let blob = prepared
                    .get(&file.key)
                    .ok_or_else(CompatibleCacheError::corrupt)?;
                Ok(SnapshotFileRecord::new(
                    &file.path,
                    blob.digest,
                    blob.size,
                    Some(file.key.clone()),
                ))
            })
            .collect::<Result<Vec<_>, CompatibleCacheError>>()?;
        publish_manifest(&self.sidecar, commit, plan.selection.id(), records)?;

        let immutable_revision = Revision::parse(commit.as_str())?;
        let offline = CompatibleCacheOffline::from_parts(self.reader.clone(), self.sidecar.clone());
        let snapshot = offline.open(&immutable_revision, plan.selection.paths())?;
        if CommitId::parse(revision.as_str()).is_err() {
            self.publish_ref(revision, commit, full_tree)?;
        }
        Ok(snapshot)
    }

    fn initialize_python_layout(&self) -> Result<(), CompatibleCacheError> {
        let tag = self.layout.cachedir_tag();
        let tag_relative = self.relative(&tag)?;
        let staging = self.effects.next_staging_name().map_err(CacheError::from)?;
        let outcome = self
            .root
            .create_once(&tag_relative, CACHEDIR_TAG, &staging)
            .map_err(CacheError::from)?;
        if outcome == CreateOnceOutcome::Created {
            self.sync_parent(&tag_relative)?;
        }
        let actual = match self
            .root
            .read_regular_bounded(&tag_relative, CACHEDIR_TAG.len().saturating_mul(2))
            .map_err(CacheError::from)?
        {
            super::rooted_fs::RootedRead::Bytes(bytes) => bytes,
            super::rooted_fs::RootedRead::Missing | super::rooted_fs::RootedRead::Other => {
                return Err(CompatibleCacheError::corrupt());
            }
        };
        if !valid_cachedir_tag(&actual) {
            return Err(CompatibleCacheError::corrupt());
        }
        self.root
            .ensure_dir(&self.relative(&self.layout.snapshots_directory())?)
            .map_err(CacheError::from)?;
        Ok(())
    }

    fn publish_blobs(
        &self,
        prepared: &BTreeMap<HubBlobKey, PreparedBlob>,
    ) -> Result<(), CompatibleCacheError> {
        for (key, blob) in prepared {
            let destination = self.layout.blob_path(key);
            let destination_relative = self.relative(&destination)?;
            let lock = self.layout.blob_lock(key);
            let _guard = self
                .root
                .lock_exclusive(&self.relative(&lock)?)
                .map_err(CacheError::from)?;
            if let Some(staging) = &blob.staging {
                let outcome = self
                    .root
                    .install_staged_create_once(staging, &destination_relative)
                    .map_err(CacheError::from)?;
                if outcome == CreateOnceOutcome::Created {
                    self.sync_parent(&destination_relative)?;
                }
            }
            self.validate_blob(&destination_relative, blob)?;
        }
        Ok(())
    }

    fn validate_blob(
        &self,
        path: &Path,
        expected: &PreparedBlob,
    ) -> Result<(), CompatibleCacheError> {
        let (mut reader, size) = match self.root.open_regular(path).map_err(CacheError::from)? {
            RootedRegularFile::File { reader, size } => (reader, size),
            RootedRegularFile::Missing | RootedRegularFile::Other => {
                return Err(CompatibleCacheError::corrupt());
            }
        };
        if size != expected.size {
            return Err(CompatibleCacheError::corrupt());
        }
        let (size, digest) =
            copy_and_validate_content(reader.as_mut(), &mut io::sink(), &expected.entry)?;
        if size != expected.size || digest != expected.digest {
            return Err(CompatibleCacheError::corrupt());
        }
        Ok(())
    }

    fn publish_tree(
        &self,
        commit: &CommitId,
        full_tree: &HubTree,
    ) -> Result<super::hub_cache::HubCacheIndex, CompatibleCacheError> {
        let destination = self.layout.tree_path(commit);
        self.ensure_parent(&destination)?;
        let encoded = encode_tree(full_tree)
            .map_err(HubCacheReadError::tree_metadata)
            .map_err(CompatibleCacheError::from)?;
        let staging = self.effects.next_staging_name().map_err(CacheError::from)?;
        let outcome = self
            .root
            .create_once(&self.relative(&destination)?, &encoded, &staging)
            .map_err(CacheError::from)?;
        if outcome == CreateOnceOutcome::Created {
            self.sync_parent(&self.relative(&destination)?)?;
        }
        let immutable_revision = Revision::parse(commit.as_str())?;
        let index = self.reader.read_index(&immutable_revision)?;
        if index.commit() != commit || index.tree() != full_tree {
            return Err(CompatibleCacheError::corrupt());
        }
        Ok(index)
    }

    fn materialize_snapshot(
        &self,
        index: &super::hub_cache::HubCacheIndex,
        plan: &WritePlan,
        prepared: &BTreeMap<HubBlobKey, PreparedBlob>,
    ) -> Result<(), CompatibleCacheError> {
        for file in &plan.files {
            let blob = prepared
                .get(&file.key)
                .ok_or_else(CompatibleCacheError::corrupt)?;
            match self.reader.read_snapshot_file(index, &file.path) {
                Ok(current) => {
                    validate_snapshot_file(&current, &file.key, blob)?;
                    continue;
                }
                Err(error) if error.is_incomplete() => {}
                Err(error) => return Err(error.into()),
            }

            let snapshot = self.layout.snapshot_file(index.commit(), &file.path);
            let snapshot_relative = self.relative(&snapshot)?;
            let standard_blob = self.layout.blob_path(&file.key);
            let blob_relative = self.relative(&standard_blob)?;
            let staging = self.effects.next_staging_name().map_err(CacheError::from)?;
            let copied = match self.materialization {
                SnapshotMaterialization::Copy => true,
                SnapshotMaterialization::Auto => {
                    #[cfg(unix)]
                    {
                        self.try_snapshot_symlink(&snapshot_relative, &blob_relative)?
                    }
                    #[cfg(not(unix))]
                    {
                        true
                    }
                }
            };
            if copied {
                let outcome = self
                    .root
                    .copy_regular_create_once(&blob_relative, &snapshot_relative, &staging)
                    .map_err(CacheError::from)?;
                if outcome == CreateOnceOutcome::Created {
                    self.sync_parent(&snapshot_relative)?;
                }
            }
            let current = self.reader.read_snapshot_file(index, &file.path)?;
            validate_snapshot_file(&current, &file.key, blob)?;
        }
        Ok(())
    }

    #[cfg(unix)]
    fn try_snapshot_symlink(
        &self,
        snapshot_relative: &Path,
        blob_relative: &Path,
    ) -> Result<bool, CompatibleCacheError> {
        let target = canonical_relative_link_target(snapshot_relative, blob_relative)
            .ok_or_else(CompatibleCacheError::corrupt)?;
        match self
            .root
            .create_relative_symlink_once(snapshot_relative, &target)
            .map_err(CacheError::from)?
        {
            RelativeSymlinkOutcome::Created => {
                self.sync_parent(snapshot_relative)?;
                Ok(false)
            }
            RelativeSymlinkOutcome::Existing => Ok(false),
            RelativeSymlinkOutcome::Unsupported => Ok(true),
        }
    }

    fn publish_ref(
        &self,
        revision: &Revision,
        commit: &CommitId,
        full_tree: &HubTree,
    ) -> Result<(), CompatibleCacheError> {
        let destination = self.layout.ref_path(revision)?;
        self.ensure_parent(&destination)?;
        let lock = self.layout.sidecar().ref_lock(revision)?;
        let _guard = self
            .root
            .lock_exclusive(&self.relative(&lock)?)
            .map_err(CacheError::from)?;
        let staging = self.effects.next_staging_name().map_err(CacheError::from)?;
        self.root
            .replace(
                &self.relative(&destination)?,
                commit.as_str().as_bytes(),
                &staging,
            )
            .map_err(CacheError::from)?;
        self.sync_parent(&self.relative(&destination)?)?;
        let index = self.reader.read_index(revision)?;
        if index.commit() != commit || index.tree() != full_tree {
            return Err(CompatibleCacheError::corrupt());
        }
        Ok(())
    }

    fn sync_parent(&self, path: &Path) -> Result<(), CompatibleCacheError> {
        let parent = path
            .parent()
            .ok_or_else(|| CacheError::from(io::Error::other("cache path has no parent")))?;
        self.root.sync_directory(parent).map_err(CacheError::from)?;
        Ok(())
    }

    fn ensure_parent(&self, path: &Path) -> Result<(), CompatibleCacheError> {
        let parent = path
            .parent()
            .ok_or_else(|| CacheError::from(io::Error::other("cache path has no parent")))?;
        self.root
            .ensure_dir(&self.relative(parent)?)
            .map_err(CacheError::from)?;
        Ok(())
    }

    fn relative(&self, path: &Path) -> Result<PathBuf, CompatibleCacheError> {
        path.strip_prefix(self.layout.capability_root())
            .map(Path::to_path_buf)
            .map_err(|_outside| {
                CacheError::from(io::Error::other(
                    "standard cache path is outside its retained capability root",
                ))
                .into()
            })
    }
}

#[derive(Debug)]
struct WritePlan {
    selection: ExactSelection,
    files: Box<[PlannedFile]>,
    blobs: BTreeMap<HubBlobKey, PlannedBlob>,
}

impl WritePlan {
    fn new(full_tree: &HubTree, paths: &[RepoPath]) -> Result<Self, CompatibleCacheError> {
        let selection = ExactSelection::new(paths)?;
        let mut files = Vec::with_capacity(selection.paths().len());
        let mut blobs: BTreeMap<HubBlobKey, PlannedBlob> = BTreeMap::new();
        for path in selection.paths() {
            let entry = full_tree
                .files()
                .get(path)
                .ok_or_else(CompatibleCacheError::incomplete)?;
            let key = compatible_blob_key(entry)?;
            match blobs.get(&key) {
                Some(existing) if existing.entry.size() != entry.size() => {
                    return Err(CompatibleCacheError::corrupt());
                }
                Some(_existing) => {}
                None => {
                    blobs.insert(
                        key.clone(),
                        PlannedBlob {
                            entry: entry.clone(),
                            source_path: path.clone(),
                        },
                    );
                }
            }
            files.push(PlannedFile {
                path: path.clone(),
                key,
            });
        }
        Ok(Self {
            selection,
            files: files.into_boxed_slice(),
            blobs,
        })
    }
}

#[derive(Debug)]
struct PlannedFile {
    path: RepoPath,
    key: HubBlobKey,
}

#[derive(Debug)]
struct PlannedBlob {
    entry: HubTreeEntry,
    source_path: RepoPath,
}

#[derive(Debug)]
struct PreparedBlob {
    entry: HubTreeEntry,
    size: u64,
    digest: BlobDigest,
    staging: Option<PathBuf>,
}

struct StagingCleanup {
    root: Arc<dyn RootedFileSystem>,
    paths: Vec<PathBuf>,
}

impl StagingCleanup {
    fn new(root: Arc<dyn RootedFileSystem>) -> Self {
        Self {
            root,
            paths: Vec::new(),
        }
    }

    fn track(&mut self, path: PathBuf) {
        self.paths.push(path);
    }

    fn remove_all(&mut self) {
        for path in self.paths.drain(..) {
            let _cleanup_result = self.root.remove_file(&path);
        }
    }
}

impl Drop for StagingCleanup {
    fn drop(&mut self) {
        self.remove_all();
    }
}

fn validate_revision_commit(
    revision: &Revision,
    commit: &CommitId,
) -> Result<(), CompatibleCacheError> {
    if CommitId::parse(revision.as_str()).is_ok_and(|requested| requested != *commit) {
        Err(CompatibleCacheError::corrupt())
    } else {
        Ok(())
    }
}

fn validate_snapshot_file(
    current: &super::hub_cache::HubCacheFile,
    key: &HubBlobKey,
    expected: &PreparedBlob,
) -> Result<(), CompatibleCacheError> {
    if current.hub_blob_key() == key
        && current.size() == expected.size
        && current.digest() == expected.digest
    {
        Ok(())
    } else {
        Err(CompatibleCacheError::corrupt())
    }
}

fn valid_cachedir_tag(actual: &[u8]) -> bool {
    if actual == CACHEDIR_TAG {
        return true;
    }
    let mut crlf = Vec::with_capacity(CACHEDIR_TAG.len().saturating_add(8));
    for byte in CACHEDIR_TAG {
        if *byte == b'\n' {
            crlf.push(b'\r');
        }
        crlf.push(*byte);
    }
    actual == crlf
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::error::Error;
    use std::fs;
    use std::io::{self, Cursor};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use sha1::{Digest as _, Sha1};
    use sha2::Sha256;
    use tempfile::TempDir;

    use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec, Revision};

    use super::{CACHEDIR_TAG, SnapshotMaterialization, StandardCacheWriter};
    use crate::cache::compatible_cache::CompatibleCacheOffline;
    use crate::cache::hub_layout::HubCacheLayout;
    use crate::cache::hub_metadata::{HubTree, HubTreeEntry, decode_tree};
    use crate::cache::publication::{
        Effects, NoPublicationFaults, OsFileSystem, RandomOperationIds, SystemClock,
    };

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const CONFIG_BYTES: &[u8] = b"{\"model_type\":\"fixture\"}\n";
    const WEIGHTS_BYTES: &[u8] = b"fixture-weights\n";
    const MODEL_COMMIT: &str = "1111111111111111111111111111111111111111";
    const DATASET_COMMIT: &str = "2222222222222222222222222222222222222222";
    const SPACE_COMMIT: &str = "3333333333333333333333333333333333333333";
    const DATASET_BLOB_ID: &str = "da60683925f47d433804d08c7128d3fd1bd850f1";
    const MODEL_CONFORMANCE_BYTES: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/cache/models--fixture-model/snapshots/1111111111111111111111111111111111111111/config.json"
    );
    const DATASET_CONFORMANCE_BYTES: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/cache/datasets--fixture-org--fixture-dataset/snapshots/2222222222222222222222222222222222222222/data/train.jsonl"
    );
    const SPACE_CONFORMANCE_BYTES: &[u8] = include_bytes!(
        "../../tests/fixtures/huggingface_hub-v1.24.0/cache/spaces--fixture-org--fixture-space/blobs/8d7bedcfa905ca2dc23b3a5c5f048cd8d4eacd05"
    );

    #[test]
    fn writes_a_complete_python_layout_and_opens_it_offline() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            CACHEDIR_TAG,
            include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/cache/CACHEDIR.TAG")
        );
        let fixture = Fixture::new()?;
        let config = RepoPath::parse("config.json")?;
        let weights = RepoPath::parse("nested/model.safetensors")?;
        let config_entry = git_entry(CONFIG_BYTES)?;
        let weights_entry = lfs_entry(WEIGHTS_BYTES)?;
        let tree = HubTree::new([
            (config.clone(), config_entry.clone()),
            (weights.clone(), weights_entry.clone()),
        ])?;
        let contents = contents([
            (config.as_str(), CONFIG_BYTES),
            (weights.as_str(), WEIGHTS_BYTES),
        ]);
        let revision = Revision::parse("refs/pr/7")?;

        let snapshot = fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &revision,
            &fixture.commit,
            &tree,
            &[weights.clone(), config.clone()],
            source(&contents, None),
        )?;

        assert_eq!(snapshot.commit(), &fixture.commit);
        assert_eq!(snapshot.files().len(), 2);
        assert_eq!(fs::read(fixture.root.join("CACHEDIR.TAG"))?, CACHEDIR_TAG);
        let layout = fixture.layout()?;
        assert_eq!(
            decode_tree(&fs::read(layout.tree_path(&fixture.commit))?)?,
            tree
        );
        assert_eq!(
            fs::read(layout.ref_path(&revision)?)?,
            fixture.commit.as_str().as_bytes()
        );
        assert!(layout.sidecar().ref_lock(&revision)?.is_file());
        assert_eq!(
            fs::read(layout.snapshot_file(&fixture.commit, &config))?,
            CONFIG_BYTES
        );
        assert_eq!(
            fs::read(layout.snapshot_file(&fixture.commit, &weights))?,
            WEIGHTS_BYTES
        );

        let offline = CompatibleCacheOffline::shared(
            &fixture.root,
            &fixture.endpoint,
            &fixture.spec,
            Fixture::effects(),
        )?;
        let reopened = offline.open(&revision, &[config, weights])?;
        assert_eq!(reopened.commit(), &fixture.commit);
        assert_eq!(reopened.files().len(), 2);
        Ok(())
    }

    #[test]
    fn idempotent_rerun_reopens_without_opening_a_source() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        let revision = Revision::parse("main")?;
        let writer = fixture.writer(SnapshotMaterialization::Copy)?;
        writer.publish(
            &revision,
            &fixture.commit,
            &tree,
            std::slice::from_ref(&path),
            source(&contents, None),
        )?;

        let reopened = writer.publish(
            &revision,
            &fixture.commit,
            &tree,
            std::slice::from_ref(&path),
            |_path| {
                Err::<Cursor<Vec<u8>>, _>(io::Error::other(
                    "idempotent cache hit must not open a source",
                ))
            },
        )?;

        assert_eq!(reopened.commit(), &fixture.commit);
        assert_eq!(reopened.files().len(), 1);
        Ok(())
    }

    #[test]
    fn conflicting_immutable_tree_is_preserved_and_ref_is_not_published()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let requested_path = RepoPath::parse("config.json")?;
        let requested_tree = HubTree::new([(requested_path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let existing_path = RepoPath::parse("other.json")?;
        let existing_tree =
            HubTree::new([(existing_path, git_entry(b"conflicting immutable tree\n")?)])?;
        let layout = fixture.layout()?;
        let existing_bytes = crate::cache::hub_metadata::encode_tree(&existing_tree)?;
        write(&layout.tree_path(&fixture.commit), &existing_bytes)?;
        let contents = contents([(requested_path.as_str(), CONFIG_BYTES)]);
        let revision = Revision::parse("main")?;

        let error = fixture
            .writer(SnapshotMaterialization::Copy)?
            .publish(
                &revision,
                &fixture.commit,
                &requested_tree,
                &[requested_path],
                source(&contents, None),
            )
            .expect_err("a conflicting tree at one immutable commit must fail");

        assert!(error.is_corrupt());
        assert_eq!(fs::read(layout.tree_path(&fixture.commit))?, existing_bytes);
        assert!(!layout.ref_path(&revision)?.try_exists()?);
        assert!(!layout.sidecar().cache_root().try_exists()?);
        Ok(())
    }

    #[test]
    fn full_commit_and_empty_selection_create_no_symbolic_ref_or_source_read()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(path, git_entry(CONFIG_BYTES)?)])?;
        let revision = Revision::parse(fixture.commit.as_str())?;
        let snapshot = fixture
            .writer(SnapshotMaterialization::Copy)?
            .publish::<Cursor<Vec<u8>>, _>(&revision, &fixture.commit, &tree, &[], |_path| {
                Err(io::Error::other("empty selection must not open a source"))
            })?;
        let layout = fixture.layout()?;

        assert!(snapshot.files().is_empty());
        assert!(layout.snapshot_directory(&fixture.commit).is_dir());
        assert!(!layout.repository_directory().join("refs").try_exists()?);
        Ok(())
    }

    #[test]
    fn accepts_and_preserves_an_existing_crlf_cachedir_tag() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let crlf = String::from_utf8(CACHEDIR_TAG.to_vec())?.replace('\n', "\r\n");
        fs::write(fixture.root.join("CACHEDIR.TAG"), crlf.as_bytes())?;
        let path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);

        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            &[path],
            source(&contents, None),
        )?;

        assert_eq!(
            fs::read(fixture.root.join("CACHEDIR.TAG"))?,
            crlf.as_bytes()
        );
        Ok(())
    }

    #[test]
    fn reuses_an_existing_python_blob_without_opening_a_source() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let tree = HubTree::new([(path.clone(), entry.clone())])?;
        let layout = fixture.layout()?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        write(&layout.blob_path(&key), CONFIG_BYTES)?;
        let calls = Arc::new(AtomicUsize::new(0));

        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            &[path],
            |_path| {
                calls.fetch_add(1, Ordering::Relaxed);
                Err::<Cursor<Vec<u8>>, _>(io::Error::other("cache hit must not open a source"))
            },
        )?;

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        Ok(())
    }

    #[test]
    fn promotes_a_python_snapshot_only_file_without_opening_a_source() -> Result<(), Box<dyn Error>>
    {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let layout = fixture.layout()?;
        write(&layout.snapshot_file(&fixture.commit, &path), CONFIG_BYTES)?;
        let calls = AtomicUsize::new(0);

        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            &[path],
            |_path| {
                calls.fetch_add(1, Ordering::Relaxed);
                Err::<Cursor<Vec<u8>>, _>(io::Error::other(
                    "snapshot-only reuse must not open a source",
                ))
            },
        )?;

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(fs::read(layout.blob_path(&key))?, CONFIG_BYTES);
        Ok(())
    }

    #[test]
    fn opens_one_source_for_duplicate_paths_with_the_same_blob() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let first = RepoPath::parse("a/config.json")?;
        let second = RepoPath::parse("b/config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let tree = HubTree::new([(first.clone(), entry.clone()), (second.clone(), entry)])?;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);

        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            &[first, second],
            move |_path| {
                observed.fetch_add(1, Ordering::Relaxed);
                Ok(Cursor::new(CONFIG_BYTES.to_vec()))
            },
        )?;

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[test]
    fn invalid_source_bytes_publish_no_python_visible_state() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let contents = contents([(path.as_str(), b"wrong-content-with-same-size\n".as_slice())]);

        let error = fixture
            .writer(SnapshotMaterialization::Copy)?
            .publish(
                &Revision::parse("main")?,
                &fixture.commit,
                &tree,
                &[path],
                source(&contents, None),
            )
            .expect_err("Git identity mismatch must fail");

        assert!(error.is_corrupt());
        assert!(!fixture.root.join("CACHEDIR.TAG").try_exists()?);
        assert!(!fixture.layout()?.repository_directory().try_exists()?);
        Ok(())
    }

    #[test]
    fn unsafe_symbolic_ref_is_rejected_before_source_or_cache_mutation()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let calls = AtomicUsize::new(0);

        let error = fixture
            .writer(SnapshotMaterialization::Copy)?
            .publish(
                &Revision::parse("refs\\main")?,
                &fixture.commit,
                &tree,
                &[path],
                |_path| {
                    calls.fetch_add(1, Ordering::Relaxed);
                    Ok(Cursor::new(CONFIG_BYTES.to_vec()))
                },
            )
            .expect_err("host path syntax must not be mapped into standard-cache refs");

        assert!(error.is_unsafe());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert!(fs::read_dir(&fixture.root)?.next().is_none());
        Ok(())
    }

    #[test]
    fn regular_fallback_is_independent_from_the_retained_blob() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        let layout = fixture.layout()?;

        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            std::slice::from_ref(&path),
            source(&contents, None),
        )?;

        fs::write(
            layout.snapshot_file(&fixture.commit, &path),
            vec![b'x'; CONFIG_BYTES.len()],
        )?;
        assert_eq!(fs::read(layout.blob_path(&key))?, CONFIG_BYTES);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn unix_default_uses_the_canonical_relative_symlink() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("nested/config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        let layout = fixture.layout()?;

        fixture.writer(SnapshotMaterialization::Auto)?.publish(
            &Revision::parse("main")?,
            &fixture.commit,
            &tree,
            std::slice::from_ref(&path),
            source(&contents, None),
        )?;

        let snapshot = layout.snapshot_file(&fixture.commit, &path);
        let snapshot_relative = snapshot.strip_prefix(layout.capability_root())?;
        let blob = layout.blob_path(&key);
        let blob_relative = blob.strip_prefix(layout.capability_root())?;
        assert_eq!(
            fs::read_link(&snapshot)?,
            crate::cache::hub_cache::canonical_relative_link_target(
                snapshot_relative,
                blob_relative,
            )
            .ok_or("relative link target")?
        );
        Ok(())
    }

    #[test]
    fn rust_writer_conformance_emitter_covers_all_repository_kinds() -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let inventory = emit_rust_writer_conformance(&directory.path().join("output"))?;
        let value: Value = serde_json::from_slice(&fs::read(inventory)?)?;

        assert_eq!(value["producer"], "hf-store-rs");
        assert_eq!(value["cache_root"], "cache");
        let repositories = value["repositories"]
            .as_array()
            .ok_or("conformance repositories")?;
        assert_eq!(repositories.len(), 3);
        assert_eq!(repositories[0]["repo_type"], "model");
        assert_eq!(repositories[1]["repo_type"], "dataset");
        assert_eq!(repositories[2]["repo_type"], "space");
        assert_eq!(repositories[1]["refs"][1]["revision"], "refs/pr/7");
        assert_eq!(value["runtime_symlinks_materialized"], cfg!(unix));
        for repository in repositories {
            assert_eq!(
                repository["files"][0]["snapshot_form"],
                if cfg!(unix) {
                    "relative_symlink_runtime"
                } else {
                    "copied_regular_with_blob"
                }
            );
        }
        Ok(())
    }

    #[test]
    #[ignore = "invoked by the pinned-Python conformance job with an explicit output path"]
    fn emit_python_conformance_fixture() -> Result<(), Box<dyn Error>> {
        let output = std::env::var_os("HF_STORE_CONFORMANCE_OUTPUT")
            .map(std::path::PathBuf::from)
            .ok_or("HF_STORE_CONFORMANCE_OUTPUT is required")?;
        let inventory = emit_rust_writer_conformance(&output)?;
        println!("{}", inventory.display());
        Ok(())
    }

    struct ConformanceCase {
        spec: RepositorySpec,
        commit: CommitId,
        refs: &'static [&'static str],
        path: RepoPath,
        bytes: &'static [u8],
        entry: HubTreeEntry,
    }

    fn emit_rust_writer_conformance(
        output: &std::path::Path,
    ) -> Result<std::path::PathBuf, Box<dyn Error>> {
        fs::create_dir(output)?;
        let cache = output.join("cache");
        fs::create_dir(&cache)?;
        let endpoint = Endpoint::hugging_face();
        let cases = rust_writer_conformance_cases()?;
        let mut repositories = Vec::with_capacity(cases.len());
        let mut runtime_symlinks_materialized = false;

        for case in cases {
            let layout = HubCacheLayout::shared(&cache, &endpoint, &case.spec)?;
            let tree = HubTree::new([(case.path.clone(), case.entry.clone())])?;
            let writer = StandardCacheWriter::shared_with_materialization(
                &cache,
                &endpoint,
                &case.spec,
                Fixture::effects(),
                SnapshotMaterialization::Auto,
            )?;
            let mut refs = Vec::with_capacity(case.refs.len());
            for revision in case.refs {
                let revision = Revision::parse(revision)?;
                writer.publish(
                    &revision,
                    &case.commit,
                    &tree,
                    std::slice::from_ref(&case.path),
                    |_path| Ok(Cursor::new(case.bytes.to_vec())),
                )?;
                refs.push(json!({
                    "revision": revision.as_str(),
                    "path": relative_posix(
                        layout.repository_directory(),
                        &layout.ref_path(&revision)?,
                    )?,
                }));
            }
            let snapshot = layout.snapshot_file(&case.commit, &case.path);
            let snapshot_form = if fs::symlink_metadata(&snapshot)?.file_type().is_symlink() {
                runtime_symlinks_materialized = true;
                "relative_symlink_runtime"
            } else {
                "copied_regular_with_blob"
            };
            let key = crate::cache::hub_cache::compatible_blob_key(&case.entry)?;
            repositories.push(json!({
                "repo_type": case.spec.kind().to_string(),
                "repo_id": case.spec.id().as_str(),
                "cache_directory": relative_posix(&cache, layout.repository_directory())?,
                "commit": case.commit.as_str(),
                "refs": refs,
                "tree_path": relative_posix(
                    layout.repository_directory(),
                    &layout.tree_path(&case.commit),
                )?,
                "files": [{
                    "path": case.path.as_str(),
                    "etag": key.as_str(),
                    "blob_id": case.entry.blob_id(),
                    "size": case.entry.size(),
                    "content_sha256": sha256_hex(case.bytes),
                    "snapshot_form": snapshot_form,
                }],
                "missing_paths": [],
            }));
        }

        let inventory = json!({
            "format_version": 1,
            "producer": "hf-store-rs",
            "cache_root": "cache",
            "runtime_symlinks_materialized": runtime_symlinks_materialized,
            "repositories": repositories,
        });
        let inventory_path = output.join("inventory.json");
        let mut encoded = serde_json::to_vec_pretty(&inventory)?;
        encoded.push(b'\n');
        fs::write(&inventory_path, encoded)?;
        Ok(inventory_path)
    }

    fn rust_writer_conformance_cases() -> Result<[ConformanceCase; 3], Box<dyn Error>> {
        let dataset_size = u64::try_from(DATASET_CONFORMANCE_BYTES.len())?;
        let dataset_sha256 = sha256_hex(DATASET_CONFORMANCE_BYTES);
        Ok([
            ConformanceCase {
                spec: RepositorySpec::model(RepositoryId::parse("fixture-model")?),
                commit: CommitId::parse(MODEL_COMMIT)?,
                refs: &["main"],
                path: RepoPath::parse("config.json")?,
                bytes: MODEL_CONFORMANCE_BYTES,
                entry: git_entry(MODEL_CONFORMANCE_BYTES)?,
            },
            ConformanceCase {
                spec: RepositorySpec::dataset(RepositoryId::parse("fixture-org/fixture-dataset")?),
                commit: CommitId::parse(DATASET_COMMIT)?,
                refs: &["main", "refs/pr/7"],
                path: RepoPath::parse("data/train.jsonl")?,
                bytes: DATASET_CONFORMANCE_BYTES,
                entry: HubTreeEntry::new(dataset_size, DATASET_BLOB_ID)?
                    .with_lfs(&dataset_sha256, dataset_size)?,
            },
            ConformanceCase {
                spec: RepositorySpec::space(RepositoryId::parse("fixture-org/fixture-space")?),
                commit: CommitId::parse(SPACE_COMMIT)?,
                refs: &["main"],
                path: RepoPath::parse("src/app.py")?,
                bytes: SPACE_CONFORMANCE_BYTES,
                entry: git_entry(SPACE_CONFORMANCE_BYTES)?,
            },
        ])
    }

    fn relative_posix(base: &std::path::Path, path: &std::path::Path) -> Result<String, io::Error> {
        let relative = path
            .strip_prefix(base)
            .map_err(|_outside| io::Error::other("conformance path escaped its base"))?;
        let rendered = relative.to_string_lossy().replace('\\', "/");
        if rendered.is_empty() || rendered.starts_with('/') || rendered.contains(':') {
            Err(io::Error::other("conformance path is not portable"))
        } else {
            Ok(rendered)
        }
    }

    struct Fixture {
        _directory: TempDir,
        root: std::path::PathBuf,
        endpoint: Endpoint,
        spec: RepositorySpec,
        commit: CommitId,
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let root = directory.path().join("hub");
            fs::create_dir(&root)?;
            Ok(Self {
                _directory: directory,
                root,
                endpoint: Endpoint::hugging_face(),
                spec: RepositorySpec::model(RepositoryId::parse("org/repo")?),
                commit: CommitId::parse(COMMIT)?,
            })
        }

        fn effects() -> Effects {
            Effects::new(
                Arc::new(OsFileSystem),
                Arc::new(RandomOperationIds),
                Arc::new(SystemClock),
                Arc::new(NoPublicationFaults),
            )
        }

        fn layout(&self) -> Result<HubCacheLayout, Box<dyn Error>> {
            Ok(HubCacheLayout::shared(
                &self.root,
                &self.endpoint,
                &self.spec,
            )?)
        }

        fn writer(
            &self,
            materialization: SnapshotMaterialization,
        ) -> Result<StandardCacheWriter, Box<dyn Error>> {
            Ok(StandardCacheWriter::shared_with_materialization(
                &self.root,
                &self.endpoint,
                &self.spec,
                Self::effects(),
                materialization,
            )?)
        }
    }

    fn source<'a>(
        contents: &'a BTreeMap<String, Vec<u8>>,
        calls: Option<&'a AtomicUsize>,
    ) -> impl FnMut(&RepoPath) -> io::Result<Cursor<Vec<u8>>> + 'a {
        move |path| {
            if let Some(calls) = calls {
                calls.fetch_add(1, Ordering::Relaxed);
            }
            contents
                .get(path.as_str())
                .cloned()
                .map(Cursor::new)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing test content"))
        }
    }

    fn contents<const N: usize>(items: [(&str, &[u8]); N]) -> BTreeMap<String, Vec<u8>> {
        items
            .into_iter()
            .map(|(path, bytes)| (path.to_owned(), bytes.to_vec()))
            .collect()
    }

    fn git_entry(bytes: &[u8]) -> Result<HubTreeEntry, Box<dyn Error>> {
        Ok(HubTreeEntry::new(
            u64::try_from(bytes.len())?,
            git_blob_id(bytes),
        )?)
    }

    fn git_blob_id(bytes: &[u8]) -> String {
        let mut hasher = Sha1::new();
        hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    fn lfs_entry(bytes: &[u8]) -> Result<HubTreeEntry, Box<dyn Error>> {
        let size = u64::try_from(bytes.len())?;
        let sha256 = sha256_hex(bytes);
        Ok(HubTreeEntry::new(size, "opaque-lfs-pointer")?.with_lfs(sha256, size)?)
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }

    fn write(path: &std::path::Path, bytes: &[u8]) -> io::Result<()> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("test path has no parent"))?;
        fs::create_dir_all(parent)?;
        fs::write(path, bytes)
    }
}
