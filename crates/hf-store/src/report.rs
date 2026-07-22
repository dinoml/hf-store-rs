use serde::Serialize;

use crate::{CacheMode, HubError, Snapshot};

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
    files: Box<[InspectedFile]>,
}

impl InspectionReport {
    pub(crate) fn complete(cache_mode: CacheMode, snapshot: &Snapshot) -> Self {
        Self {
            schema: "hf-store.inspection",
            version: 1,
            cache_mode,
            state: InspectionState::Complete,
            commit: Some(snapshot.commit().as_str().into()),
            selection_id: Some(snapshot.selection_id().to_string().into()),
            files: snapshot.files().iter().map(InspectedFile::from).collect(),
        }
    }

    pub(crate) fn failed(cache_mode: CacheMode, error: &HubError) -> Self {
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
}

impl VerificationReport {
    pub(crate) const fn from_inspection(inspection: InspectionReport) -> Self {
        let valid = matches!(inspection.state, InspectionState::Complete);
        Self {
            schema: "hf-store.verification",
            version: 1,
            valid,
            inspection,
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
}
