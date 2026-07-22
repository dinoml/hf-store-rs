use serde::Serialize;

use crate::{CacheMode, HubError, Snapshot};

/// Read-only state of one recognized repository-cache entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum InventoryState {
    /// Recognized ordinary cache state.
    Recognized,
    /// Inert transfer or publication staging state.
    Staging,
    /// Resumable or incomplete transfer state.
    Partial,
    /// Quarantined deletion state.
    Trash,
    /// A link, reparse point, or special entry blocks safe classification.
    Unsafe,
    /// Recognized metadata is malformed or inconsistent.
    Corrupt,
    /// Recognized metadata uses an unsupported version.
    UnsupportedVersion,
    /// Private hf-store metadata without an independently complete upstream snapshot.
    SidecarOnly,
    /// A regular snapshot file with no retained physical blob.
    SnapshotOnly,
    /// A regular snapshot copy whose identical retained blob was found.
    CopiedWithBlob,
    /// A contained relative snapshot symlink to a regular physical blob.
    RelativeSymlink,
}

/// One cache-relative entry in a repository inventory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InventoryEntry {
    path: Box<str>,
    state: InventoryState,
    directory: bool,
}

impl InventoryEntry {
    /// Returns a cache-relative, slash-separated path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the classified entry state.
    #[must_use]
    pub const fn state(&self) -> InventoryState {
        self.state
    }

    /// Returns whether this entry is a directory.
    #[must_use]
    pub const fn is_directory(&self) -> bool {
        self.directory
    }
}

/// Deterministic repository-wide read-only cache inventory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheInventoryReport {
    schema: &'static str,
    version: u32,
    cache_mode: CacheMode,
    entries: Box<[InventoryEntry]>,
}

impl CacheInventoryReport {
    pub(crate) fn new(cache_mode: CacheMode, records: Vec<crate::cache::InventoryRecord>) -> Self {
        let entries = records
            .into_iter()
            .map(|record| {
                let state = if record.kind == crate::cache::InventoryRecordKind::Unsafe {
                    InventoryState::Unsafe
                } else if record.metadata == crate::cache::InventoryRecordMetadata::Unsupported {
                    InventoryState::UnsupportedVersion
                } else if record.metadata == crate::cache::InventoryRecordMetadata::Corrupt {
                    InventoryState::Corrupt
                } else if record.semantic == crate::cache::InventoryRecordSemantic::SidecarOnly {
                    InventoryState::SidecarOnly
                } else if record.semantic == crate::cache::InventoryRecordSemantic::SnapshotOnly {
                    InventoryState::SnapshotOnly
                } else if record.semantic == crate::cache::InventoryRecordSemantic::CopiedWithBlob {
                    InventoryState::CopiedWithBlob
                } else if record.semantic == crate::cache::InventoryRecordSemantic::RelativeSymlink
                {
                    InventoryState::RelativeSymlink
                } else {
                    match record.namespace.as_ref() {
                        "staging" => InventoryState::Staging,
                        "partials" => InventoryState::Partial,
                        "trash" => InventoryState::Trash,
                        _ => InventoryState::Recognized,
                    }
                };
                InventoryEntry {
                    path: record.relative_path,
                    state,
                    directory: record.kind == crate::cache::InventoryRecordKind::Directory,
                }
            })
            .collect();
        Self {
            schema: "hf-store.cache-inventory",
            version: 1,
            cache_mode,
            entries,
        }
    }

    /// Returns inventory entries in stable cache-relative path order.
    #[must_use]
    pub fn entries(&self) -> &[InventoryEntry] {
        &self.entries
    }

    /// Returns the report schema name.
    #[must_use]
    pub const fn schema(&self) -> &str {
        self.schema
    }

    /// Returns the report schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns the inventoried cache view.
    #[must_use]
    pub const fn cache_mode(&self) -> CacheMode {
        self.cache_mode
    }
}

/// Stable classification of an exact-selection cache inspection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum InspectionState {
    /// Every selected file and its completion metadata validated.
    Complete,
    /// Required content or completion metadata is absent.
    Incomplete,
    /// Recognized content or metadata failed validation.
    Corrupt,
    /// A recognized record uses an unsupported version.
    UnsupportedVersion,
    /// Required coordination is currently busy.
    Busy,
    /// Inspection could not safely classify a local I/O failure.
    Io,
}

/// A deterministic read-only report for one exact cache selection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InspectionReport {
    schema: &'static str,
    version: u32,
    cache_mode: CacheMode,
    state: InspectionState,
    commit: Option<Box<str>>,
    selection_id: Option<Box<str>>,
    requested_revision: Box<str>,
    requested_paths: Box<[Box<str>]>,
    finding: Option<InspectionFinding>,
    files: Box<[InspectedFile]>,
}

/// Exact safe evidence for an unsuccessful selection inspection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InspectionFinding {
    state: InspectionState,
    subject: &'static str,
}

impl InspectionFinding {
    /// Returns the classified observed state.
    #[must_use]
    pub const fn state(&self) -> InspectionState {
        self.state
    }

    /// Returns the stable logical cache subject that failed validation.
    #[must_use]
    pub const fn subject(&self) -> &str {
        self.subject
    }
}

impl InspectionReport {
    pub(crate) fn complete(
        cache_mode: CacheMode,
        revision: &crate::Revision,
        paths: &[crate::RepoPath],
        snapshot: &Snapshot,
    ) -> Self {
        Self {
            schema: "hf-store.inspection",
            version: 1,
            cache_mode,
            state: InspectionState::Complete,
            commit: Some(snapshot.commit().as_str().into()),
            selection_id: Some(snapshot.selection_id().to_string().into()),
            requested_revision: revision.as_str().into(),
            requested_paths: canonical_paths(paths),
            finding: None,
            files: snapshot.files().iter().map(InspectedFile::from).collect(),
        }
    }

    pub(crate) fn failed(
        cache_mode: CacheMode,
        revision: &crate::Revision,
        paths: &[crate::RepoPath],
        error: &HubError,
    ) -> Self {
        let state = if error.is_cache_incomplete() {
            InspectionState::Incomplete
        } else if error.is_cache_corrupt() || error.is_validation() {
            InspectionState::Corrupt
        } else if error.is_cache_unsupported() {
            InspectionState::UnsupportedVersion
        } else if error.is_cache_busy() {
            InspectionState::Busy
        } else {
            InspectionState::Io
        };
        Self {
            schema: "hf-store.inspection",
            version: 1,
            cache_mode,
            state,
            commit: None,
            selection_id: None,
            requested_revision: revision.as_str().into(),
            requested_paths: canonical_paths(paths),
            finding: Some(InspectionFinding {
                state,
                subject: inspection_subject(state),
            }),
            files: Box::new([]),
        }
    }

    /// Returns the report schema name.
    #[must_use]
    pub const fn schema(&self) -> &str {
        self.schema
    }

    /// Returns the report schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns the inspected cache view.
    #[must_use]
    pub const fn cache_mode(&self) -> CacheMode {
        self.cache_mode
    }

    /// Returns the inspection result classification.
    #[must_use]
    pub const fn state(&self) -> InspectionState {
        self.state
    }

    /// Returns the resolved commit when inspection completed successfully.
    #[must_use]
    pub fn commit(&self) -> Option<&str> {
        self.commit.as_deref()
    }

    /// Returns the exact selection identity when inspection completed successfully.
    #[must_use]
    pub fn selection_id(&self) -> Option<&str> {
        self.selection_id.as_deref()
    }

    /// Returns the exact requested revision spelling.
    #[must_use]
    pub fn requested_revision(&self) -> &str {
        &self.requested_revision
    }

    /// Returns requested repository paths in canonical order.
    #[must_use]
    pub fn requested_paths(&self) -> &[Box<str>] {
        &self.requested_paths
    }

    /// Returns safe failure evidence when the selection was not complete.
    #[must_use]
    pub const fn finding(&self) -> Option<&InspectionFinding> {
        self.finding.as_ref()
    }

    /// Returns validated files in canonical repository-path order.
    #[must_use]
    pub fn files(&self) -> &[InspectedFile] {
        &self.files
    }
}

/// One validated file in an [`InspectionReport`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InspectedFile {
    path: Box<str>,
    sha256: Box<str>,
    size: u64,
    form: crate::SnapshotFileForm,
}

impl InspectedFile {
    /// Returns the canonical repository path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the validated local SHA-256 digest.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    /// Returns the validated size.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns the physical cache form proven during validation.
    #[must_use]
    pub const fn form(&self) -> crate::SnapshotFileForm {
        self.form
    }
}

impl From<&crate::SnapshotFile> for InspectedFile {
    fn from(file: &crate::SnapshotFile) -> Self {
        Self {
            path: file.path().as_str().into(),
            sha256: file.sha256().into(),
            size: file.size(),
            form: file.form(),
        }
    }
}

/// A deterministic verification report derived from a read-only inspection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    schema: &'static str,
    version: u32,
    valid: bool,
    inspection: InspectionReport,
    findings: Box<[VerificationFinding]>,
}

/// One stable negative verification finding.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerificationFinding {
    state: InspectionState,
    subject: &'static str,
    revision: Box<str>,
    paths: Box<[Box<str>]>,
}

impl VerificationFinding {
    /// Returns the negative finding classification.
    #[must_use]
    pub const fn state(&self) -> InspectionState {
        self.state
    }

    /// Returns the logical record or content scope that failed.
    #[must_use]
    pub const fn subject(&self) -> &str {
        self.subject
    }

    /// Returns the exact revision that was verified.
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Returns every selected path whose complete closure could not be proven.
    #[must_use]
    pub fn paths(&self) -> &[Box<str>] {
        &self.paths
    }
}

impl VerificationReport {
    pub(crate) fn from_inspection(inspection: InspectionReport) -> Self {
        let valid = matches!(inspection.state, InspectionState::Complete);
        let findings = match &inspection.finding {
            Some(finding) => vec![VerificationFinding {
                state: finding.state,
                subject: finding.subject,
                revision: inspection.requested_revision.clone(),
                paths: inspection.requested_paths.clone(),
            }]
            .into_boxed_slice(),
            None => Box::new([]),
        };
        Self {
            schema: "hf-store.verification",
            version: 1,
            valid,
            inspection,
            findings,
        }
    }

    /// Returns whether every required record and file validated.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.valid
    }

    /// Returns the underlying inspection evidence.
    #[must_use]
    pub const fn inspection(&self) -> &InspectionReport {
        &self.inspection
    }

    /// Returns stable negative findings; valid reports contain none.
    #[must_use]
    pub fn findings(&self) -> &[VerificationFinding] {
        &self.findings
    }
}

fn canonical_paths(paths: &[crate::RepoPath]) -> Box<[Box<str>]> {
    let mut paths = paths
        .iter()
        .map(|path| Box::<str>::from(path.as_str()))
        .collect::<Vec<_>>();
    paths.sort_unstable();
    paths.dedup();
    paths.into_boxed_slice()
}

const fn inspection_subject(state: InspectionState) -> &'static str {
    match state {
        InspectionState::Complete => "selection",
        InspectionState::Incomplete => "selection-manifest-or-content",
        InspectionState::Corrupt => "selection-manifest-or-selected-file",
        InspectionState::UnsupportedVersion => "selection-manifest",
        InspectionState::Busy => "selection-lease",
        InspectionState::Io => "cache-root",
    }
}
