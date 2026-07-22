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
use super::rooted_fs::{CreateOnceOutcome, RootedFileSystem, RootedLockGuard, RootedRegularFile};

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
    #[cfg(test)]
    blob_lock_observer: Option<BlobLockObserver>,
}

#[cfg(test)]
#[derive(Clone)]
struct BlobLockObserver {
    attempted: Arc<dyn Fn() + Send + Sync>,
    acquired: Arc<dyn Fn() + Send + Sync>,
}

#[cfg(test)]
impl std::fmt::Debug for BlobLockObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BlobLockObserver")
    }
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
            #[cfg(test)]
            blob_lock_observer: None,
        })
    }

    #[cfg(test)]
    pub(super) fn shared_for_test(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
        effects: Effects,
        materialization: SnapshotMaterialization,
    ) -> Result<Self, CompatibleCacheError> {
        Self::shared_with_materialization(root, endpoint, spec, effects, materialization)
    }

    #[cfg(test)]
    pub(super) fn observe_blob_lock_for_test(
        &mut self,
        attempted: Arc<dyn Fn() + Send + Sync>,
        acquired: Arc<dyn Fn() + Send + Sync>,
    ) {
        self.blob_lock_observer = Some(BlobLockObserver {
            attempted,
            acquired,
        });
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
        let PreparedBlobs {
            blobs,
            mut cleanup,
            guards,
        } = self.prepare_blobs(commit, full_tree, &plan, &mut open_source)?;
        let result = self.publish_prepared(revision, commit, full_tree, &plan, &blobs);
        cleanup.remove_all();
        drop(guards);
        result.map_err(CompatibleCacheError::with_may_have_published)
    }

    fn prepare_blobs<R, F>(
        &self,
        commit: &CommitId,
        full_tree: &HubTree,
        plan: &WritePlan,
        open_source: &mut F,
    ) -> Result<PreparedBlobs, CompatibleCacheError>
    where
        R: Read,
        F: FnMut(&RepoPath) -> io::Result<R>,
    {
        let mut cleanup = StagingCleanup::new(Arc::clone(&self.root));
        let mut prepared = BTreeMap::new();
        let mut guards = Vec::with_capacity(plan.blobs.len());
        let reusable_index = match self.reader.index_from_tree(commit, full_tree) {
            Ok(index) => Some(index),
            Err(error) if error.is_incomplete() => None,
            Err(error) => return Err(error.into()),
        };
        let mut published_any = false;
        let tree_context = TreeContext { commit, full_tree };
        for (key, blob) in &plan.blobs {
            let prepared_blob = self.prepare_blob(
                tree_context,
                key,
                blob,
                reusable_index.as_ref(),
                open_source,
                &mut cleanup,
            );
            let (prepared_blob, published, guard) = match prepared_blob {
                Ok(prepared_blob) => prepared_blob,
                Err(error) if published_any => return Err(error.with_may_have_published()),
                Err(error) => return Err(error),
            };
            published_any |= published;
            prepared.insert(key.clone(), prepared_blob);
            guards.push(guard);
        }
        Ok(PreparedBlobs {
            blobs: prepared,
            cleanup,
            guards,
        })
    }

    fn prepare_blob<R, F>(
        &self,
        tree_context: TreeContext<'_>,
        key: &HubBlobKey,
        blob: &PlannedBlob,
        reusable_index: Option<&HubCacheIndex>,
        open_source: &mut F,
        cleanup: &mut StagingCleanup,
    ) -> Result<(PreparedBlob, bool, Box<dyn RootedLockGuard>), CompatibleCacheError>
    where
        R: Read,
        F: FnMut(&RepoPath) -> io::Result<R>,
    {
        let destination = self.layout.blob_path(key);
        let destination_relative = self.relative(&destination)?;
        let lock = self.layout.blob_lock(key);
        #[cfg(test)]
        if let Some(observer) = &self.blob_lock_observer {
            (observer.attempted)();
        }
        let guard = self
            .root
            .lock_exclusive(&self.relative(&lock)?)
            .map_err(CacheError::from)?;
        #[cfg(test)]
        if let Some(observer) = &self.blob_lock_observer {
            (observer.acquired)();
        }
        let refreshed_index = if reusable_index.is_none() {
            match self
                .reader
                .index_from_tree(tree_context.commit, tree_context.full_tree)
            {
                Ok(index) => Some(index),
                Err(error) if error.is_incomplete() => None,
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };
        let reusable_index = reusable_index.or(refreshed_index.as_ref());
        let prepared = match self
            .root
            .open_regular(&destination_relative)
            .map_err(CacheError::from)?
        {
            RootedRegularFile::File {
                mut reader, size, ..
            } => {
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
            RootedRegularFile::Missing => {
                self.prepare_missing_blob(blob, reusable_index, open_source, cleanup)?
            }
            RootedRegularFile::Other => return Err(CompatibleCacheError::corrupt()),
        };
        let published = self.publish_blob_locked(&destination_relative, &prepared)?;
        Ok((prepared, published, guard))
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
        let staging = self.next_staging_file()?;
        let outcome = self
            .root
            .create_once_from_staging(&tag_relative, CACHEDIR_TAG, &staging)
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

    fn publish_blob_locked(
        &self,
        destination: &Path,
        blob: &PreparedBlob,
    ) -> Result<bool, CompatibleCacheError> {
        let published = if let Some(staging) = &blob.staging {
            // Make a newly published blob scan-safe before it becomes visible.
            // Source validation still precedes this scaffold, so invalid bytes
            // leave no Python-visible repository state.
            self.initialize_python_layout()?;
            let outcome = self
                .root
                .install_staged_create_once(staging, destination)
                .map_err(CacheError::from)?;
            if outcome == CreateOnceOutcome::Created {
                self.sync_parent(destination)
                    .map_err(CompatibleCacheError::with_may_have_published)?;
            }
            outcome == CreateOnceOutcome::Created
        } else {
            false
        };
        self.validate_blob(destination, blob).map_err(|error| {
            if published {
                error.with_may_have_published()
            } else {
                error
            }
        })?;
        Ok(published)
    }

    fn validate_blob(
        &self,
        path: &Path,
        expected: &PreparedBlob,
    ) -> Result<(), CompatibleCacheError> {
        let (mut reader, size) = match self.root.open_regular(path).map_err(CacheError::from)? {
            RootedRegularFile::File { reader, size, .. } => (reader, size),
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
        let staging = self.next_staging_file()?;
        let outcome = self
            .root
            .create_once_from_staging(&self.relative(&destination)?, &encoded, &staging)
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
            let staging = self.next_staging_file()?;
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
                    .copy_regular_create_once_from_staging(
                        &blob_relative,
                        &snapshot_relative,
                        &staging,
                    )
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
        let staging = self.next_staging_file()?;
        self.root
            .replace_from_staging(
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

    fn next_staging_file(&self) -> Result<PathBuf, CompatibleCacheError> {
        let name = self.effects.next_staging_name().map_err(CacheError::from)?;
        let path = self.layout.staged_file(&name);
        self.ensure_parent(&path)?;
        self.relative(&path)
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

#[derive(Clone, Copy)]
struct TreeContext<'a> {
    commit: &'a CommitId,
    full_tree: &'a HubTree,
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

struct PreparedBlobs {
    blobs: BTreeMap<HubBlobKey, PreparedBlob>,
    cleanup: StagingCleanup,
    guards: Vec<Box<dyn RootedLockGuard>>,
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
    use std::fmt::{self, Debug, Display, Formatter};
    use std::fs;
    use std::io::{self, Cursor, Read, Write};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Output, Stdio};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};

    use serde_json::{Value, json};
    use sha1::{Digest as _, Sha1};
    use sha2::Sha256;
    use tempfile::TempDir;

    use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec, Revision};

    use super::{CACHEDIR_TAG, SnapshotMaterialization, StandardCacheWriter};
    use crate::cache::compatible_cache::CompatibleCacheOffline;
    use crate::cache::hub_layout::HubCacheLayout;
    use crate::cache::hub_metadata::{HubTree, HubTreeEntry, decode_tree};
    use crate::cache::key::SelectionId;
    use crate::cache::publication::{
        CacheKernel, Effects, NoPublicationFaults, OsFileSystem, RandomOperationIds, SystemClock,
    };
    use crate::cache::rooted_fs::{
        CreateOnceOutcome, RelativeSymlinkOutcome, RootedEntryKind, RootedFileSystem,
        RootedLockGuard, RootedRead, RootedRegularFile, RootedWrite, StagingName,
    };

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const RACE_FIRST_COMMIT: &str = "4444444444444444444444444444444444444444";
    const RACE_SECOND_COMMIT: &str = "5555555555555555555555555555555555555555";
    const CRASH_NEW_COMMIT: &str = "6666666666666666666666666666666666666666";
    const CONFIG_BYTES: &[u8] = b"{\"model_type\":\"fixture\"}\n";
    const CRASH_NEW_BYTES: &[u8] = b"{\"model_type\":\"crash-boundary\"}\n";
    const WEIGHTS_BYTES: &[u8] = b"fixture-weights\n";
    const CROSS_PROCESS_WRITER_CHILD_TEST: &str =
        "cache::standard_cache::tests::cross_process_standard_cache_writer_child";
    const CROSS_PROCESS_WRITER_CHILD_ENV: &str = "HF_STORE_STANDARD_WRITER_CHILD";
    const CROSS_PROCESS_ROOT_ENV: &str = "HF_STORE_STANDARD_WRITER_ROOT";
    const CROSS_PROCESS_COMMIT_ENV: &str = "HF_STORE_STANDARD_WRITER_COMMIT";
    const CROSS_PROCESS_READY_ENV: &str = "HF_STORE_STANDARD_WRITER_READY";
    const CROSS_PROCESS_GO_ENV: &str = "HF_STORE_STANDARD_WRITER_GO";
    const CROSS_PROCESS_SOURCE_ENV: &str = "HF_STORE_STANDARD_WRITER_SOURCE";
    const CROSS_PROCESS_RESULT_ENV: &str = "HF_STORE_STANDARD_WRITER_RESULT";
    const CRASH_CHILD_TEST: &str =
        "cache::standard_cache::tests::standard_cache_process_exit_child";
    const CRASH_CHILD_ENV: &str = "HF_STORE_STANDARD_WRITER_CRASH_CHILD";
    const CRASH_TARGET_ENV: &str = "HF_STORE_STANDARD_WRITER_CRASH_TARGET";
    const CRASH_PHASE_ENV: &str = "HF_STORE_STANDARD_WRITER_CRASH_PHASE";
    const CRASH_MARKER_ENV: &str = "HF_STORE_STANDARD_WRITER_CRASH_MARKER";
    const CRASH_EXIT_CODE: i32 = 86;
    const CROSS_PROCESS_TIMEOUT: Duration = Duration::from_secs(20);
    const CROSS_PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(5);
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

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum CrashBoundary {
        Blob,
        Tree,
        Snapshot,
        Manifest,
        Ref,
    }

    impl CrashBoundary {
        const ALL: [Self; 5] = [
            Self::Blob,
            Self::Tree,
            Self::Snapshot,
            Self::Manifest,
            Self::Ref,
        ];
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum CrashPhase {
        Before,
        After,
    }

    impl CrashPhase {
        const ALL: [Self; 2] = [Self::Before, Self::After];

        const fn as_str(self) -> &'static str {
            match self {
                Self::Before => "before",
                Self::After => "after",
            }
        }

        fn parse(value: &str) -> io::Result<Self> {
            match value {
                "before" => Ok(Self::Before),
                "after" => Ok(Self::After),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid standard-cache crash phase",
                )),
            }
        }
    }

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
    fn concurrent_writers_lock_before_opening_a_shared_source() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let revision = Revision::parse(fixture.commit.as_str())?;
        let mut writer = fixture.writer(SnapshotMaterialization::Copy)?;
        let lock = fixture.layout()?.blob_lock(&key);
        fs::create_dir_all(
            lock.parent()
                .ok_or_else(|| io::Error::other("blob lock has no parent directory"))?,
        )?;
        let _lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock)?;
        let lock_path = writer.relative(&lock)?;
        writer.root = Arc::new(BlobPublicationProbeRoot {
            inner: Arc::clone(&writer.root),
            lock_barrier: Some((lock_path, Arc::new(Barrier::new(2)))),
            failed_install: None,
            lock_lifetime: None,
            crash_probe: None,
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(2);

        for _writer_index in 0..2 {
            let writer = writer.clone();
            let path = path.clone();
            let tree = tree.clone();
            let revision = revision.clone();
            let commit = fixture.commit.clone();
            let calls = Arc::clone(&calls);
            handles.push(thread::spawn(move || {
                writer
                    .publish(
                        &revision,
                        &commit,
                        &tree,
                        std::slice::from_ref(&path),
                        move |_source_path| {
                            calls.fetch_add(1, Ordering::Relaxed);
                            Ok(Cursor::new(CONFIG_BYTES.to_vec()))
                        },
                    )
                    .map(|_snapshot| ())
                    .map_err(|error| error.to_string())
            }));
        }

        for handle in handles {
            let result = handle
                .join()
                .map_err(|_panic| io::Error::other("concurrent cache writer panicked"))?;
            result.map_err(io::Error::other)?;
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(fs::read(fixture.layout()?.blob_path(&key))?, CONFIG_BYTES);
        Ok(())
    }

    #[test]
    fn competing_processes_publish_complete_shared_cache_snapshots() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let executable = std::env::current_exe()?;
        let coordination = fixture
            .directory
            .path()
            .join("standard-writer-coordination");
        fs::create_dir(&coordination)?;
        let go = coordination.join("go");
        let commits = [RACE_FIRST_COMMIT, RACE_SECOND_COMMIT];
        let mut children = Vec::with_capacity(commits.len());
        let mut ready = Vec::with_capacity(commits.len());
        let mut sources = Vec::with_capacity(commits.len());
        let mut results = Vec::with_capacity(commits.len());

        for (index, commit) in commits.iter().enumerate() {
            let ready_path = coordination.join(format!("writer-{index}.ready"));
            let source_path = coordination.join(format!("writer-{index}.source"));
            let result_path = coordination.join(format!("writer-{index}.result"));
            let child = Command::new(&executable)
                .arg("--exact")
                .arg(CROSS_PROCESS_WRITER_CHILD_TEST)
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CROSS_PROCESS_WRITER_CHILD_ENV, "1")
                .env(CROSS_PROCESS_ROOT_ENV, &fixture.root)
                .env(CROSS_PROCESS_COMMIT_ENV, commit)
                .env(CROSS_PROCESS_READY_ENV, &ready_path)
                .env(CROSS_PROCESS_GO_ENV, &go)
                .env(CROSS_PROCESS_SOURCE_ENV, &source_path)
                .env(CROSS_PROCESS_RESULT_ENV, &result_path)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            children.push(StandardWriterChild::new(child));
            ready.push(ready_path);
            sources.push(source_path);
            results.push(result_path);
        }

        let ready_deadline = Instant::now() + CROSS_PROCESS_TIMEOUT;
        wait_for_standard_writer_children_ready(&mut children, &ready, ready_deadline)?;
        fs::write(&go, b"go")?;
        let exit_deadline = Instant::now() + CROSS_PROCESS_TIMEOUT;
        wait_for_standard_writer_children_success(&mut children, exit_deadline)?;

        let source_opens = sources
            .iter()
            .map(PathBuf::as_path)
            .map(Path::try_exists)
            .collect::<io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|opened| *opened)
            .count();
        assert_eq!(source_opens, 1);

        let mut published_commits = results
            .iter()
            .map(fs::read_to_string)
            .collect::<io::Result<Vec<_>>>()?;
        published_commits.sort_unstable();
        assert_eq!(
            published_commits,
            commits.map(str::to_owned).into_iter().collect::<Vec<_>>()
        );

        let path = RepoPath::parse("config.json")?;
        let revision = Revision::parse("refs/pr/7")?;
        let layout = fixture.layout()?;
        let offline = CompatibleCacheOffline::shared(
            &fixture.root,
            &fixture.endpoint,
            &fixture.spec,
            Fixture::effects(),
        )?;
        for commit in commits {
            let immutable = Revision::parse(commit)?;
            let snapshot = offline.open(&immutable, std::slice::from_ref(&path))?;
            assert_eq!(snapshot.commit().as_str(), commit);
            assert_eq!(snapshot.files().len(), 1);
            assert_eq!(fs::read(snapshot.files()[0].content_path())?, CONFIG_BYTES);
        }

        let final_commit = fs::read_to_string(layout.ref_path(&revision)?)?;
        assert!(commits.contains(&final_commit.as_str()));
        let active = offline.open(&revision, std::slice::from_ref(&path))?;
        assert_eq!(active.commit().as_str(), final_commit);
        assert_eq!(fs::read(active.files()[0].content_path())?, CONFIG_BYTES);
        assert!(fs::read_dir(layout.staging_directory())?.next().is_none());
        assert_no_standard_writer_temporary_files(&fixture.root)?;
        Ok(())
    }

    #[test]
    fn cross_process_standard_cache_writer_child() -> Result<(), Box<dyn Error>> {
        if std::env::var_os(CROSS_PROCESS_WRITER_CHILD_ENV).is_none() {
            return Ok(());
        }

        let root = required_standard_writer_child_path(CROSS_PROCESS_ROOT_ENV)?;
        let commit = CommitId::parse(std::env::var(CROSS_PROCESS_COMMIT_ENV)?)?;
        let ready = required_standard_writer_child_path(CROSS_PROCESS_READY_ENV)?;
        let go = required_standard_writer_child_path(CROSS_PROCESS_GO_ENV)?;
        let source_marker = required_standard_writer_child_path(CROSS_PROCESS_SOURCE_ENV)?;
        let result = required_standard_writer_child_path(CROSS_PROCESS_RESULT_ENV)?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let writer = StandardCacheWriter::shared_with_materialization(
            &root,
            &endpoint,
            &spec,
            Fixture::effects(),
            SnapshotMaterialization::Copy,
        )?;

        fs::write(&ready, b"ready")?;
        wait_for_standard_writer_path(&go, Instant::now() + CROSS_PROCESS_TIMEOUT)?;
        let snapshot = writer.publish(
            &Revision::parse("refs/pr/7")?,
            &commit,
            &tree,
            std::slice::from_ref(&path),
            |_source_path| {
                fs::write(&source_marker, b"opened")?;
                Ok(Cursor::new(CONFIG_BYTES.to_vec()))
            },
        )?;
        fs::write(result, snapshot.commit().as_str().as_bytes())?;
        Ok(())
    }

    #[test]
    fn process_exit_boundaries_preserve_a_complete_active_snapshot() -> Result<(), Box<dyn Error>> {
        for boundary in CrashBoundary::ALL {
            for phase in CrashPhase::ALL {
                run_standard_cache_crash_case(boundary, phase)?;
            }
        }
        Ok(())
    }

    #[test]
    fn standard_cache_process_exit_child() -> Result<(), Box<dyn Error>> {
        if std::env::var_os(CRASH_CHILD_ENV).is_none() {
            return Ok(());
        }

        let root = required_standard_writer_child_path(CROSS_PROCESS_ROOT_ENV)?;
        let target = required_standard_writer_child_path(CRASH_TARGET_ENV)?;
        let marker = required_standard_writer_child_path(CRASH_MARKER_ENV)?;
        let phase = CrashPhase::parse(&std::env::var(CRASH_PHASE_ENV)?)?;
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let path = RepoPath::parse("config.json")?;
        let commit = CommitId::parse(CRASH_NEW_COMMIT)?;
        let tree = HubTree::new([(path.clone(), git_entry(CRASH_NEW_BYTES)?)])?;
        let effects = Fixture::effects();
        let mut writer = StandardCacheWriter::shared_with_materialization(
            &root,
            &endpoint,
            &spec,
            effects.clone(),
            SnapshotMaterialization::Copy,
        )?;
        let probe_root: Arc<dyn RootedFileSystem> = Arc::new(BlobPublicationProbeRoot {
            inner: Arc::clone(&writer.root),
            lock_barrier: None,
            failed_install: None,
            lock_lifetime: None,
            crash_probe: Some(CrashProbe {
                target,
                phase,
                marker,
            }),
        });
        writer.sidecar =
            CacheKernel::for_compatible_cache(&writer.layout, Arc::clone(&probe_root), effects)?;
        writer.root = probe_root;

        writer.publish(
            &Revision::parse("refs/pr/7")?,
            &commit,
            &tree,
            std::slice::from_ref(&path),
            |_source_path| Ok(Cursor::new(CRASH_NEW_BYTES.to_vec())),
        )?;
        Err(io::Error::other("standard-cache crash probe was not reached").into())
    }

    #[test]
    fn blob_lock_is_held_until_the_snapshot_entry_is_complete() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let revision = Revision::parse("main")?;
        let layout = fixture.layout()?;
        let mut writer = fixture.writer(SnapshotMaterialization::Copy)?;
        let active = Arc::new(AtomicBool::new(false));
        let observed_during_snapshot = Arc::new(AtomicBool::new(false));
        writer.root = Arc::new(BlobPublicationProbeRoot {
            inner: Arc::clone(&writer.root),
            lock_barrier: None,
            failed_install: None,
            lock_lifetime: Some(LockLifetimeProbe {
                lock: writer.relative(&layout.blob_lock(&key))?,
                snapshot: writer.relative(&layout.snapshot_file(&fixture.commit, &path))?,
                active: Arc::clone(&active),
                observed_during_snapshot: Arc::clone(&observed_during_snapshot),
            }),
            crash_probe: None,
        });
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        let calls = AtomicUsize::new(0);

        writer.publish(
            &revision,
            &fixture.commit,
            &tree,
            std::slice::from_ref(&path),
            source(&contents, Some(&calls)),
        )?;

        assert!(observed_during_snapshot.load(Ordering::Acquire));
        assert!(!active.load(Ordering::Acquire));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[test]
    fn scan_safe_scaffold_precedes_a_failing_blob_install() -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let entry = git_entry(CONFIG_BYTES)?;
        let key = crate::cache::hub_cache::compatible_blob_key(&entry)?;
        let tree = HubTree::new([(path.clone(), entry)])?;
        let revision = Revision::parse("refs/pr/7")?;
        let layout = fixture.layout()?;
        let mut writer = fixture.writer(SnapshotMaterialization::Copy)?;
        let observed_scan_safe = Arc::new(AtomicBool::new(false));
        writer.root = Arc::new(BlobPublicationProbeRoot {
            inner: Arc::clone(&writer.root),
            lock_barrier: None,
            failed_install: Some(FailedBlobInstall {
                destination: writer.relative(&layout.blob_path(&key))?,
                cachedir_tag: writer.relative(&layout.cachedir_tag())?,
                snapshots: writer.relative(&layout.snapshots_directory())?,
                observed_scan_safe: Arc::clone(&observed_scan_safe),
            }),
            lock_lifetime: None,
            crash_probe: None,
        });
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        let calls = AtomicUsize::new(0);

        writer
            .publish(
                &revision,
                &fixture.commit,
                &tree,
                std::slice::from_ref(&path),
                source(&contents, Some(&calls)),
            )
            .expect_err("the controlled blob install must fail");

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert!(observed_scan_safe.load(Ordering::Acquire));
        assert_eq!(fs::read(layout.cachedir_tag())?, CACHEDIR_TAG);
        assert!(layout.snapshots_directory().is_dir());
        assert!(!layout.blob_path(&key).try_exists()?);
        assert!(!layout.tree_path(&fixture.commit).try_exists()?);
        assert!(!layout.ref_path(&revision)?.try_exists()?);
        assert!(!layout.sidecar().cache_root().try_exists()?);
        assert!(fs::read_dir(layout.staging_directory())?.next().is_none());
        Ok(())
    }

    #[test]
    fn source_open_and_read_errors_are_redacted_from_the_compatible_error_chain()
    -> Result<(), Box<dyn Error>> {
        let fixture = Fixture::new()?;
        let path = RepoPath::parse("config.json")?;
        let tree = HubTree::new([(path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let writer = fixture.writer(SnapshotMaterialization::Copy)?;

        let open_error = writer
            .publish::<io::Empty, _>(
                &Revision::parse("main")?,
                &fixture.commit,
                &tree,
                std::slice::from_ref(&path),
                |_source_path| Err(secret_io_error()),
            )
            .expect_err("accepted an injected source-open failure");
        assert_secret_absent_from_error_chain(&open_error);

        let read_error = writer
            .publish(
                &Revision::parse("main")?,
                &fixture.commit,
                &tree,
                std::slice::from_ref(&path),
                |_source_path| Ok(SecretReader),
            )
            .expect_err("accepted an injected source-read failure");
        assert_secret_absent_from_error_chain(&read_error);
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
        directory: TempDir,
        root: std::path::PathBuf,
        endpoint: Endpoint,
        spec: RepositorySpec,
        commit: CommitId,
    }

    const SECRET_ERROR_SENTINEL: &str = "hf_secret_signed_url_sentinel";

    struct SecretError;

    impl Debug for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_ERROR_SENTINEL)
        }
    }

    impl Display for SecretError {
        fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
            formatter.write_str(SECRET_ERROR_SENTINEL)
        }
    }

    impl Error for SecretError {}

    struct SecretReader;

    impl Read for SecretReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(secret_io_error())
        }
    }

    fn secret_io_error() -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, SecretError)
    }

    fn assert_secret_absent_from_error_chain(error: &(dyn Error + 'static)) {
        let mut current = Some(error);
        while let Some(source) = current {
            assert!(!source.to_string().contains(SECRET_ERROR_SENTINEL));
            assert!(!format!("{source:?}").contains(SECRET_ERROR_SENTINEL));
            current = source.source();
        }
    }

    #[derive(Debug)]
    struct BlobPublicationProbeRoot {
        inner: Arc<dyn RootedFileSystem>,
        lock_barrier: Option<(PathBuf, Arc<Barrier>)>,
        failed_install: Option<FailedBlobInstall>,
        lock_lifetime: Option<LockLifetimeProbe>,
        crash_probe: Option<CrashProbe>,
    }

    #[derive(Debug)]
    struct FailedBlobInstall {
        destination: PathBuf,
        cachedir_tag: PathBuf,
        snapshots: PathBuf,
        observed_scan_safe: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct LockLifetimeProbe {
        lock: PathBuf,
        snapshot: PathBuf,
        active: Arc<AtomicBool>,
        observed_during_snapshot: Arc<AtomicBool>,
    }

    #[derive(Debug)]
    struct CrashProbe {
        target: PathBuf,
        phase: CrashPhase,
        marker: PathBuf,
    }

    impl CrashProbe {
        fn check(&self, path: &Path, phase: CrashPhase) -> io::Result<()> {
            if path != self.target || phase != self.phase {
                return Ok(());
            }
            let mut marker = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&self.marker)?;
            write!(
                marker,
                "{}\n{}\n",
                self.phase.as_str(),
                self.target.display()
            )?;
            marker.sync_all()?;
            std::process::exit(CRASH_EXIT_CODE);
        }
    }

    impl BlobPublicationProbeRoot {
        fn check_crash(&self, path: &Path, phase: CrashPhase) -> io::Result<()> {
            if let Some(probe) = &self.crash_probe {
                probe.check(path, phase)?;
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ProbedLockGuard {
        _inner: Box<dyn RootedLockGuard>,
        active: Arc<AtomicBool>,
    }

    impl RootedLockGuard for ProbedLockGuard {}

    impl Drop for ProbedLockGuard {
        fn drop(&mut self) {
            self.active.store(false, Ordering::Release);
        }
    }

    impl RootedFileSystem for BlobPublicationProbeRoot {
        fn ensure_dir(&self, path: &Path) -> io::Result<()> {
            self.inner.ensure_dir(path)
        }

        fn entry_kind(&self, path: &Path) -> io::Result<RootedEntryKind> {
            self.inner.entry_kind(path)
        }

        fn open_regular(&self, path: &Path) -> io::Result<RootedRegularFile> {
            self.inner.open_regular(path)
        }

        fn read_regular_bounded(&self, path: &Path, limit: usize) -> io::Result<RootedRead> {
            self.inner.read_regular_bounded(path, limit)
        }

        fn create_new(&self, path: &Path) -> io::Result<Box<dyn RootedWrite>> {
            self.inner.create_new(path)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.inner.remove_file(path)
        }

        fn install_staged_create_once(
            &self,
            staging: &Path,
            destination: &Path,
        ) -> io::Result<CreateOnceOutcome> {
            self.check_crash(destination, CrashPhase::Before)?;
            if let Some(probe) = &self.failed_install {
                if destination == probe.destination {
                    let tag_is_valid = match self
                        .inner
                        .read_regular_bounded(&probe.cachedir_tag, CACHEDIR_TAG.len() * 2)?
                    {
                        RootedRead::Bytes(bytes) => super::valid_cachedir_tag(&bytes),
                        RootedRead::Missing | RootedRead::Other => false,
                    };
                    let snapshots_exist =
                        self.inner.entry_kind(&probe.snapshots)? == RootedEntryKind::Directory;
                    probe
                        .observed_scan_safe
                        .store(tag_is_valid && snapshots_exist, Ordering::Release);
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "controlled blob install failure",
                    ));
                }
            }
            let outcome = self
                .inner
                .install_staged_create_once(staging, destination)?;
            self.check_crash(destination, CrashPhase::After)?;
            Ok(outcome)
        }

        fn install_staged_replace(&self, staging: &Path, destination: &Path) -> io::Result<()> {
            self.inner.install_staged_replace(staging, destination)
        }

        fn create_once(
            &self,
            path: &Path,
            bytes: &[u8],
            staging: &StagingName,
        ) -> io::Result<CreateOnceOutcome> {
            self.check_crash(path, CrashPhase::Before)?;
            let outcome = self.inner.create_once(path, bytes, staging)?;
            self.check_crash(path, CrashPhase::After)?;
            Ok(outcome)
        }

        fn create_relative_symlink_once(
            &self,
            path: &Path,
            target: &Path,
        ) -> io::Result<RelativeSymlinkOutcome> {
            self.inner.create_relative_symlink_once(path, target)
        }

        fn copy_regular_create_once(
            &self,
            source: &Path,
            destination: &Path,
            staging: &StagingName,
        ) -> io::Result<CreateOnceOutcome> {
            self.check_crash(destination, CrashPhase::Before)?;
            if let Some(probe) = &self.lock_lifetime {
                if destination == probe.snapshot {
                    probe
                        .observed_during_snapshot
                        .store(probe.active.load(Ordering::Acquire), Ordering::Release);
                }
            }
            let outcome = self
                .inner
                .copy_regular_create_once(source, destination, staging)?;
            self.check_crash(destination, CrashPhase::After)?;
            Ok(outcome)
        }

        fn copy_regular_create_once_from_staging(
            &self,
            source: &Path,
            destination: &Path,
            staging_path: &Path,
        ) -> io::Result<CreateOnceOutcome> {
            self.check_crash(destination, CrashPhase::Before)?;
            if let Some(probe) = &self.lock_lifetime {
                if destination == probe.snapshot {
                    probe
                        .observed_during_snapshot
                        .store(probe.active.load(Ordering::Acquire), Ordering::Release);
                }
            }
            let outcome = self.inner.copy_regular_create_once_from_staging(
                source,
                destination,
                staging_path,
            )?;
            self.check_crash(destination, CrashPhase::After)?;
            Ok(outcome)
        }

        fn replace(&self, path: &Path, bytes: &[u8], staging: &StagingName) -> io::Result<()> {
            self.check_crash(path, CrashPhase::Before)?;
            self.inner.replace(path, bytes, staging)?;
            self.check_crash(path, CrashPhase::After)
        }

        fn replace_from_staging(
            &self,
            path: &Path,
            bytes: &[u8],
            staging_path: &Path,
        ) -> io::Result<()> {
            self.check_crash(path, CrashPhase::Before)?;
            self.inner.replace_from_staging(path, bytes, staging_path)?;
            self.check_crash(path, CrashPhase::After)
        }

        fn lock_exclusive(&self, path: &Path) -> io::Result<Box<dyn RootedLockGuard>> {
            if let Some((lock_path, barrier)) = &self.lock_barrier {
                if path == lock_path {
                    barrier.wait();
                }
            }
            let guard = self.inner.lock_exclusive(path)?;
            if let Some(probe) = &self.lock_lifetime {
                if path == probe.lock {
                    probe.active.store(true, Ordering::Release);
                    return Ok(Box::new(ProbedLockGuard {
                        _inner: guard,
                        active: Arc::clone(&probe.active),
                    }));
                }
            }
            Ok(guard)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.inner.sync_directory(path)
        }

        fn read_dir(&self, path: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.read_dir(path)
        }
    }

    fn run_standard_cache_crash_case(
        boundary: CrashBoundary,
        phase: CrashPhase,
    ) -> Result<(), Box<dyn Error>> {
        StandardCacheCrashCase::new(boundary, phase)?.run()
    }

    struct StandardCacheCrashCase {
        fixture: Fixture,
        path: RepoPath,
        revision: Revision,
        new_tree: HubTree,
        layout: HubCacheLayout,
        blob: PathBuf,
        tree: PathBuf,
        snapshot: PathBuf,
        manifest: PathBuf,
        reference: PathBuf,
        target_relative: PathBuf,
        marker: PathBuf,
        boundary: CrashBoundary,
        phase: CrashPhase,
    }

    impl StandardCacheCrashCase {
        fn new(boundary: CrashBoundary, phase: CrashPhase) -> Result<Self, Box<dyn Error>> {
            let fixture = Fixture::new()?;
            let path = RepoPath::parse("config.json")?;
            let revision = Revision::parse("refs/pr/7")?;
            seed_old_standard_cache_snapshot(&fixture, &revision, &path)?;
            let new_commit = CommitId::parse(CRASH_NEW_COMMIT)?;
            let new_entry = git_entry(CRASH_NEW_BYTES)?;
            let new_tree = HubTree::new([(path.clone(), new_entry.clone())])?;
            let new_key = crate::cache::hub_cache::compatible_blob_key(&new_entry)?;
            let selection = SelectionId::derive(std::slice::from_ref(&path))?;
            let layout = fixture.layout()?;
            let blob = layout.blob_path(&new_key);
            let tree = layout.tree_path(&new_commit);
            let snapshot = layout.snapshot_file(&new_commit, &path);
            let manifest = layout.sidecar().snapshot_manifest(&new_commit, &selection);
            let reference = layout.ref_path(&revision)?;
            let target = match boundary {
                CrashBoundary::Blob => &blob,
                CrashBoundary::Tree => &tree,
                CrashBoundary::Snapshot => &snapshot,
                CrashBoundary::Manifest => &manifest,
                CrashBoundary::Ref => &reference,
            };
            let target_relative = target.strip_prefix(&fixture.root)?.to_path_buf();
            let marker = fixture.directory.path().join(format!(
                "crash-{:?}-{}.marker",
                boundary,
                phase.as_str()
            ));
            Ok(Self {
                fixture,
                path,
                revision,
                new_tree,
                layout,
                blob,
                tree,
                snapshot,
                manifest,
                reference,
                target_relative,
                marker,
                boundary,
                phase,
            })
        }

        fn run(self) -> Result<(), Box<dyn Error>> {
            self.run_child()?;
            self.assert_complete_state()
        }

        fn run_child(&self) -> Result<(), Box<dyn Error>> {
            let child = Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg(CRASH_CHILD_TEST)
                .arg("--nocapture")
                .arg("--test-threads=1")
                .env(CRASH_CHILD_ENV, "1")
                .env(CROSS_PROCESS_ROOT_ENV, &self.fixture.root)
                .env(CRASH_TARGET_ENV, &self.target_relative)
                .env(CRASH_PHASE_ENV, self.phase.as_str())
                .env(CRASH_MARKER_ENV, &self.marker)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()?;
            let output = StandardWriterChild::new(child)
                .wait_until(Instant::now() + CROSS_PROCESS_TIMEOUT)?;
            assert_eq!(
                output.status.code(),
                Some(CRASH_EXIT_CODE),
                "crash child for {:?} {:?} exited unexpectedly; stdout: {}; stderr: {}",
                self.boundary,
                self.phase,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let expected = format!(
                "{}\n{}\n",
                self.phase.as_str(),
                self.target_relative.display()
            );
            assert_eq!(fs::read_to_string(&self.marker)?, expected);
            Ok(())
        }

        fn assert_complete_state(&self) -> Result<(), Box<dyn Error>> {
            let blob_reached = self.reached(CrashBoundary::Blob);
            let tree_reached = self.reached(CrashBoundary::Tree);
            let snapshot_reached = self.reached(CrashBoundary::Snapshot);
            let manifest_reached = self.reached(CrashBoundary::Manifest);
            let ref_reached = self.reached(CrashBoundary::Ref);
            assert_complete_regular_file(&self.blob, blob_reached, CRASH_NEW_BYTES)?;
            if tree_reached {
                assert_eq!(decode_tree(&fs::read(&self.tree)?)?, self.new_tree);
            } else {
                assert!(!self.tree.try_exists()?);
            }
            assert_complete_regular_file(&self.snapshot, snapshot_reached, CRASH_NEW_BYTES)?;
            assert_eq!(self.manifest.try_exists()?, manifest_reached);
            let expected_active = if ref_reached {
                CRASH_NEW_COMMIT
            } else {
                COMMIT
            };
            assert_ref_is_complete(&self.reference, expected_active)?;
            self.assert_offline_snapshots(expected_active, manifest_reached)?;
            assert_eq!(fs::read(self.layout.cachedir_tag())?, CACHEDIR_TAG);
            assert!(self.layout.snapshots_directory().is_dir());
            let staging = fs::read_dir(self.layout.staging_directory())?
                .map(|entry| entry.map(|entry| entry.path()))
                .collect::<io::Result<Vec<_>>>()?;
            assert!(!staging.is_empty());
            assert!(staging.iter().all(|path| path.is_file()));
            assert_no_standard_writer_temporary_files(&self.fixture.root)?;
            Ok(())
        }

        fn assert_offline_snapshots(
            &self,
            expected_active: &str,
            manifest_reached: bool,
        ) -> Result<(), Box<dyn Error>> {
            let offline = CompatibleCacheOffline::shared(
                &self.fixture.root,
                &self.fixture.endpoint,
                &self.fixture.spec,
                Fixture::effects(),
            )?;
            assert_snapshot_bytes(&offline, COMMIT, &self.path, CONFIG_BYTES)?;
            let active = offline.open(&self.revision, std::slice::from_ref(&self.path))?;
            assert_eq!(active.commit().as_str(), expected_active);
            let expected_bytes = if expected_active == CRASH_NEW_COMMIT {
                CRASH_NEW_BYTES
            } else {
                CONFIG_BYTES
            };
            assert_eq!(fs::read(active.files()[0].content_path())?, expected_bytes);
            let new_revision = Revision::parse(CRASH_NEW_COMMIT)?;
            let immutable_new = offline.open(&new_revision, std::slice::from_ref(&self.path));
            if manifest_reached {
                let snapshot = immutable_new?;
                assert_eq!(snapshot.commit().as_str(), CRASH_NEW_COMMIT);
                assert_eq!(
                    fs::read(snapshot.files()[0].content_path())?,
                    CRASH_NEW_BYTES
                );
            } else {
                assert!(
                    immutable_new
                        .expect_err("missing manifest opened")
                        .is_incomplete()
                );
            }
            Ok(())
        }

        fn reached(&self, destination: CrashBoundary) -> bool {
            crash_cut_reached(self.boundary, self.phase, destination)
        }
    }

    fn seed_old_standard_cache_snapshot(
        fixture: &Fixture,
        revision: &Revision,
        path: &RepoPath,
    ) -> Result<(), Box<dyn Error>> {
        let tree = HubTree::new([(path.clone(), git_entry(CONFIG_BYTES)?)])?;
        let contents = contents([(path.as_str(), CONFIG_BYTES)]);
        fixture.writer(SnapshotMaterialization::Copy)?.publish(
            revision,
            &fixture.commit,
            &tree,
            std::slice::from_ref(path),
            source(&contents, None),
        )?;
        Ok(())
    }

    fn assert_ref_is_complete(path: &Path, expected: &str) -> Result<(), Box<dyn Error>> {
        let actual = fs::read_to_string(path)?;
        assert_eq!(actual, expected);
        assert_eq!(actual.len(), 40);
        assert_eq!(CommitId::parse(&actual)?.as_str(), expected);
        Ok(())
    }

    fn assert_snapshot_bytes(
        offline: &CompatibleCacheOffline,
        commit: &str,
        path: &RepoPath,
        bytes: &[u8],
    ) -> Result<(), Box<dyn Error>> {
        let snapshot = offline.open(&Revision::parse(commit)?, std::slice::from_ref(path))?;
        assert_eq!(snapshot.commit().as_str(), commit);
        assert_eq!(fs::read(snapshot.files()[0].content_path())?, bytes);
        Ok(())
    }

    fn crash_cut_reached(
        cut: CrashBoundary,
        phase: CrashPhase,
        destination: CrashBoundary,
    ) -> bool {
        cut > destination || (cut == destination && phase == CrashPhase::After)
    }

    fn assert_complete_regular_file(path: &Path, expected: bool, bytes: &[u8]) -> io::Result<()> {
        if expected {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_file() || fs::read(path)? != bytes {
                return Err(io::Error::other(format!(
                    "published destination is not a complete regular file: {}",
                    path.display()
                )));
            }
        } else if path.try_exists()? {
            return Err(io::Error::other(format!(
                "pre-publication destination is visible: {}",
                path.display()
            )));
        }
        Ok(())
    }

    fn required_standard_writer_child_path(name: &str) -> io::Result<PathBuf> {
        std::env::var_os(name)
            .map(PathBuf::from)
            .ok_or_else(|| io::Error::other(format!("child process is missing {name}")))
    }

    fn wait_for_standard_writer_path(path: &Path, deadline: Instant) -> io::Result<()> {
        loop {
            if path.try_exists()? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("timed out waiting for {}", path.display()),
                ));
            }
            thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
        }
    }

    fn wait_for_standard_writer_children_ready(
        children: &mut [StandardWriterChild],
        ready_paths: &[PathBuf],
        deadline: Instant,
    ) -> io::Result<()> {
        loop {
            let all_ready = ready_paths
                .iter()
                .map(PathBuf::as_path)
                .map(Path::try_exists)
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .all(|ready| ready);
            if all_ready {
                return Ok(());
            }

            for child in &mut *children {
                if let Some(status) = child.try_wait()? {
                    let output = child.finish()?;
                    return Err(standard_writer_child_failure(
                        "writer exited before announcing readiness",
                        status,
                        &output,
                    ));
                }
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for cross-process standard-cache writers",
                ));
            }
            thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
        }
    }

    fn wait_for_standard_writer_children_success(
        children: &mut [StandardWriterChild],
        deadline: Instant,
    ) -> io::Result<()> {
        for child in children {
            let output = child.wait_until(deadline)?;
            if !output.status.success() {
                return Err(standard_writer_child_failure(
                    "cross-process standard-cache writer failed",
                    output.status,
                    &output,
                ));
            }
        }
        Ok(())
    }

    fn standard_writer_child_failure(
        context: &str,
        status: std::process::ExitStatus,
        output: &Output,
    ) -> io::Error {
        io::Error::other(format!(
            "{context} with {status}; stdout: {}; stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }

    fn assert_no_standard_writer_temporary_files(root: &Path) -> io::Result<()> {
        let mut pending = vec![root.to_path_buf()];
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(directory)? {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    pending.push(entry.path());
                    continue;
                }
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(".hf-store-") && name.ends_with(".tmp") {
                    return Err(io::Error::other(format!(
                        "standard-cache writer left temporary file {}",
                        entry.path().display()
                    )));
                }
            }
        }
        Ok(())
    }

    struct StandardWriterChild {
        child: Option<Child>,
    }

    impl StandardWriterChild {
        const fn new(child: Child) -> Self {
            Self { child: Some(child) }
        }

        fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
            self.child
                .as_mut()
                .ok_or_else(|| io::Error::other("standard-cache writer was already reaped"))?
                .try_wait()
        }

        fn finish(&mut self) -> io::Result<Output> {
            self.child
                .take()
                .ok_or_else(|| io::Error::other("standard-cache writer was already reaped"))?
                .wait_with_output()
        }

        fn wait_until(&mut self, deadline: Instant) -> io::Result<Output> {
            loop {
                if self.try_wait()?.is_some() {
                    return self.finish();
                }
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "timed out waiting for a standard-cache writer to exit",
                    ));
                }
                thread::sleep(CROSS_PROCESS_POLL_INTERVAL);
            }
        }
    }

    impl Drop for StandardWriterChild {
        fn drop(&mut self) {
            if let Some(mut child) = self.child.take() {
                let _kill_result = child.kill();
                let _wait_result = child.wait();
            }
        }
    }

    impl Fixture {
        fn new() -> Result<Self, Box<dyn Error>> {
            let directory = TempDir::new()?;
            let root = directory.path().join("hub");
            fs::create_dir(&root)?;
            Ok(Self {
                directory,
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
