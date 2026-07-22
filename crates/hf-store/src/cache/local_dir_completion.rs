//! Atomic logical-completion state for user-owned local directories.

use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io;
use std::sync::Arc;

use crate::validation::ValidationError;
use crate::{CommitId, RepoPath};

use super::key::{BlobDigest, SelectionId};
use super::local_dir_layout::HubLocalDirLayout;
use super::metadata::{LocalDirFileRecord, LocalDirStateRecord, MetadataError, encode_record};
use super::publication::{Effects, PublicationPoint};
use super::rooted_fs::RootedFileSystem;
use super::sanitized_io::SanitizedIo;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LocalDirCompletionFile {
    path: RepoPath,
    size: u64,
    digest: BlobDigest,
}

impl LocalDirCompletionFile {
    pub(super) const fn new(path: RepoPath, size: u64, digest: BlobDigest) -> Self {
        Self { path, size, digest }
    }
}

#[derive(Clone)]
pub(super) struct LocalDirCompletionWriter {
    layout: HubLocalDirLayout,
    root: Arc<dyn RootedFileSystem>,
    effects: Effects,
}

impl LocalDirCompletionWriter {
    pub(super) fn new(
        layout: HubLocalDirLayout,
        root: Arc<dyn RootedFileSystem>,
        effects: Effects,
    ) -> Result<Self, LocalDirCompletionError> {
        let _state = layout.coordination_state_relative()?;
        Ok(Self {
            layout,
            root,
            effects,
        })
    }

    pub(super) fn publish_in_progress(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<(), LocalDirCompletionError> {
        let sidecar = self.layout.completion_sidecar();
        let state = LocalDirStateRecord::in_progress(
            sidecar.origin_key(),
            sidecar.repository_key(),
            commit,
            selection,
        )?;
        self.publish(&state)
    }

    pub(super) fn publish_complete(
        &self,
        commit: &CommitId,
        selection: &SelectionId,
        files: &[LocalDirCompletionFile],
    ) -> Result<(), LocalDirCompletionError> {
        let files = files
            .iter()
            .map(|file| LocalDirFileRecord::new(&file.path, file.digest, file.size))
            .collect();
        let sidecar = self.layout.completion_sidecar();
        let state = LocalDirStateRecord::complete(
            sidecar.origin_key(),
            sidecar.repository_key(),
            commit,
            selection,
            files,
        )?;
        self.publish(&state)
    }

    fn publish(&self, state: &LocalDirStateRecord) -> Result<(), LocalDirCompletionError> {
        let bytes = encode_record(state)?;
        let relative = self.layout.coordination_state_relative()?;
        let parent = relative.parent().ok_or_else(|| {
            LocalDirCompletionError::io(
                &io::Error::new(io::ErrorKind::InvalidInput, "completion path has no parent"),
                false,
            )
        })?;
        self.root.ensure_dir(parent)?;
        let staging = self.effects.next_staging_name()?;
        self.check_fault(PublicationPoint::BeforeCompletionReplace, false)?;
        self.root
            .replace(&relative, &bytes, &staging)
            .map_err(|source| LocalDirCompletionError::io(&source, true))?;
        self.check_fault(PublicationPoint::AfterCompletionReplace, true)?;
        self.root
            .sync_directory(parent)
            .map_err(|source| LocalDirCompletionError::io(&source, true))
    }

    fn check_fault(
        &self,
        point: PublicationPoint,
        may_have_published: bool,
    ) -> Result<(), LocalDirCompletionError> {
        self.effects
            .check_publication_fault(point)
            .map_err(|source| LocalDirCompletionError::io(&source, may_have_published))
    }
}

impl fmt::Debug for LocalDirCompletionWriter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalDirCompletionWriter")
            .finish_non_exhaustive()
    }
}

pub(super) struct LocalDirCompletionError {
    kind: Box<LocalDirCompletionErrorKind>,
    may_have_published: bool,
    backtrace: Backtrace,
}

enum LocalDirCompletionErrorKind {
    Io(SanitizedIo),
    Validation(ValidationError),
    Metadata(MetadataError),
}

impl LocalDirCompletionError {
    fn new(kind: LocalDirCompletionErrorKind, may_have_published: bool) -> Self {
        Self {
            kind: Box::new(kind),
            may_have_published,
            backtrace: Backtrace::capture(),
        }
    }

    fn io(source: &io::Error, may_have_published: bool) -> Self {
        Self::new(
            LocalDirCompletionErrorKind::Io(SanitizedIo::new(source)),
            may_have_published,
        )
    }

    pub(super) const fn may_have_published(&self) -> bool {
        self.may_have_published
    }

    pub(super) const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl fmt::Debug for LocalDirCompletionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let kind = match self.kind.as_ref() {
            LocalDirCompletionErrorKind::Io(_) => "Io",
            LocalDirCompletionErrorKind::Validation(_) => "Validation",
            LocalDirCompletionErrorKind::Metadata(_) => "Metadata",
        };
        formatter
            .debug_struct("LocalDirCompletionError")
            .field("kind", &kind)
            .field("may_have_published", &self.may_have_published)
            .finish_non_exhaustive()
    }
}

impl Display for LocalDirCompletionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("local-dir completion state publication failed")
    }
}

impl Error for LocalDirCompletionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind.as_ref() {
            LocalDirCompletionErrorKind::Validation(source) => Some(source),
            LocalDirCompletionErrorKind::Io(_) | LocalDirCompletionErrorKind::Metadata(_) => None,
        }
    }
}

impl From<io::Error> for LocalDirCompletionError {
    fn from(source: io::Error) -> Self {
        Self::io(&source, false)
    }
}

impl From<ValidationError> for LocalDirCompletionError {
    fn from(source: ValidationError) -> Self {
        Self::new(LocalDirCompletionErrorKind::Validation(source), false)
    }
}

impl From<MetadataError> for LocalDirCompletionError {
    fn from(source: MetadataError) -> Self {
        Self::new(LocalDirCompletionErrorKind::Metadata(source), false)
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fs;
    use std::sync::Arc;

    use tempfile::TempDir;

    use crate::cache::key::{BlobDigest, SelectionId};
    use crate::cache::local_dir_layout::HubLocalDirLayout;
    use crate::cache::metadata::{LocalDirStateRecord, decode_record};
    use crate::cache::publication::{
        Effects, FaultController, NoPublicationFaults, OsFileSystem, PublicationPoint,
        SequenceOperationIds, SystemClock,
    };
    use crate::cache::rooted_fs::{CacheRoot, RootedFileSystem};
    use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec};

    use super::{LocalDirCompletionFile, LocalDirCompletionWriter};

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn in_progress_replaces_prior_completion_and_complete_is_published_last()
    -> Result<(), Box<dyn Error>> {
        let directory = TempDir::new()?;
        let endpoint = Endpoint::hugging_face();
        let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
        let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
        let effects = Effects::new(
            Arc::new(OsFileSystem),
            Arc::new(SequenceOperationIds::new(1)),
            Arc::new(SystemClock),
            Arc::new(NoPublicationFaults),
        );
        let writer = LocalDirCompletionWriter::new(layout.clone(), root, effects)?;
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let file = LocalDirCompletionFile::new(
            path,
            u64::try_from(b"payload".len())?,
            BlobDigest::for_bytes(b"payload"),
        );

        writer.publish_complete(&commit, &selection, std::slice::from_ref(&file))?;
        assert!(read_state(&layout)?.is_complete());
        writer.publish_in_progress(&commit, &selection)?;
        let invalidated = read_state(&layout)?;
        assert!(invalidated.is_in_progress());
        assert!(invalidated.files().is_empty());
        writer.publish_complete(&commit, &selection, &[file])?;
        let complete = read_state(&layout)?;
        assert!(complete.is_complete());
        assert_eq!(complete.files()[0].path(), "weights/model.bin");
        assert_eq!(complete.files()[0].size(), 7);
        assert_eq!(
            complete.files()[0].digest()?,
            BlobDigest::for_bytes(b"payload")
        );
        Ok(())
    }

    #[test]
    fn completion_replacement_faults_expose_only_old_or_new_complete_records()
    -> Result<(), Box<dyn Error>> {
        for point in [
            PublicationPoint::BeforeCompletionReplace,
            PublicationPoint::AfterCompletionReplace,
        ] {
            let directory = TempDir::new()?;
            let endpoint = Endpoint::hugging_face();
            let repository = RepositorySpec::model(RepositoryId::parse("org/repo")?);
            let layout = HubLocalDirLayout::new(directory.path(), &endpoint, &repository)?;
            let root: Arc<dyn RootedFileSystem> = Arc::new(CacheRoot::open(directory.path())?);
            let commit = CommitId::parse(COMMIT)?;
            let path = RepoPath::parse("model.bin")?;
            let selection = SelectionId::derive(std::slice::from_ref(&path))?;
            let file = LocalDirCompletionFile::new(
                path,
                u64::try_from(b"payload".len())?,
                BlobDigest::for_bytes(b"payload"),
            );
            LocalDirCompletionWriter::new(
                layout.clone(),
                Arc::clone(&root),
                Effects::new(
                    Arc::new(OsFileSystem),
                    Arc::new(SequenceOperationIds::new(1)),
                    Arc::new(SystemClock),
                    Arc::new(NoPublicationFaults),
                ),
            )?
            .publish_complete(&commit, &selection, &[file])?;

            let faults = Arc::new(FaultController::default());
            faults.fail_once(point);
            let writer = LocalDirCompletionWriter::new(
                layout.clone(),
                root,
                Effects::new(
                    Arc::new(OsFileSystem),
                    Arc::new(SequenceOperationIds::new(100)),
                    Arc::new(SystemClock),
                    faults,
                ),
            )?;
            let error = writer
                .publish_in_progress(&commit, &selection)
                .expect_err("injected completion replacement fault was ignored");
            assert_eq!(
                error.may_have_published(),
                point == PublicationPoint::AfterCompletionReplace
            );
            let visible = read_state(&layout)?;
            if point == PublicationPoint::BeforeCompletionReplace {
                assert!(visible.is_complete());
            } else {
                assert!(visible.is_in_progress());
            }
        }
        Ok(())
    }

    fn read_state(layout: &HubLocalDirLayout) -> Result<LocalDirStateRecord, Box<dyn Error>> {
        let bytes = fs::read(layout.coordination_state_path())?;
        Ok(decode_record(&bytes)?)
    }
}
