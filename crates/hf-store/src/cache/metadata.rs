use std::backtrace::Backtrace;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositoryKind, RepositorySpec, Revision};

use super::hub_layout::HubBlobKey;
use super::key::{BlobDigest, OriginKey, RepositoryKey, SelectionId};

const METADATA_FORMAT_VERSION: u32 = 1;

pub(super) trait CacheRecord: Serialize + DeserializeOwned + Sized {
    const KIND: &'static str;

    fn validate(&self) -> Result<(), ValidationError>;
}

#[derive(Serialize)]
struct RecordRef<'a, T> {
    format_version: u32,
    record_kind: &'static str,
    payload: &'a T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedRecord {
    format_version: u32,
    record_kind: String,
    payload: serde_json::Value,
}

pub(super) fn encode_record<T: CacheRecord>(record: &T) -> Result<Vec<u8>, MetadataError> {
    record.validate().map_err(MetadataError::invalid)?;
    let envelope = RecordRef {
        format_version: METADATA_FORMAT_VERSION,
        record_kind: T::KIND,
        payload: record,
    };
    let mut encoded = serde_json::to_vec(&envelope).map_err(MetadataError::encode)?;
    encoded.push(b'\n');
    Ok(encoded)
}

pub(super) fn decode_record<T: CacheRecord>(bytes: &[u8]) -> Result<T, MetadataError> {
    let value =
        serde_json::from_slice::<serde_json::Value>(bytes).map_err(MetadataError::decode)?;
    if value
        .get("format_version")
        .and_then(serde_json::Value::as_u64)
        .is_some_and(|version| version != u64::from(METADATA_FORMAT_VERSION))
    {
        return Err(MetadataError::unknown_version());
    }
    let envelope = serde_json::from_value::<OwnedRecord>(value).map_err(MetadataError::decode)?;
    if envelope.format_version != METADATA_FORMAT_VERSION {
        return Err(MetadataError::unknown_version());
    }
    if envelope.record_kind != T::KIND {
        return Err(MetadataError::wrong_kind());
    }
    let payload = serde_json::from_value::<T>(envelope.payload).map_err(MetadataError::decode)?;
    payload.validate().map_err(MetadataError::invalid)?;
    Ok(payload)
}

#[derive(Debug)]
pub(super) struct MetadataError {
    kind: MetadataErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum MetadataErrorKind {
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    UnknownVersion,
    WrongKind,
    Invalid(ValidationError),
}

impl MetadataError {
    fn encode(source: serde_json::Error) -> Self {
        Self::new(MetadataErrorKind::Encode(source))
    }

    fn decode(source: serde_json::Error) -> Self {
        Self::new(MetadataErrorKind::Decode(source))
    }

    fn unknown_version() -> Self {
        Self::new(MetadataErrorKind::UnknownVersion)
    }

    fn wrong_kind() -> Self {
        Self::new(MetadataErrorKind::WrongKind)
    }

    fn invalid(source: ValidationError) -> Self {
        Self::new(MetadataErrorKind::Invalid(source))
    }

    fn new(kind: MetadataErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) fn is_unknown_version(&self) -> bool {
        matches!(self.kind, MetadataErrorKind::UnknownVersion)
    }

    pub(super) fn is_corrupt(&self) -> bool {
        matches!(
            self.kind,
            MetadataErrorKind::Decode(_)
                | MetadataErrorKind::WrongKind
                | MetadataErrorKind::Invalid(_)
        )
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for MetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            MetadataErrorKind::Encode(_) => "cache metadata could not be encoded",
            MetadataErrorKind::Decode(_) => "cache metadata is corrupt",
            MetadataErrorKind::UnknownVersion => "cache metadata version is unsupported",
            MetadataErrorKind::WrongKind => "cache metadata has the wrong record kind",
            MetadataErrorKind::Invalid(_) => "cache metadata violates its record invariant",
        };
        formatter.write_str(message)
    }
}

impl Error for MetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            MetadataErrorKind::Encode(source) | MetadataErrorKind::Decode(source) => Some(source),
            MetadataErrorKind::Invalid(source) => Some(source),
            MetadataErrorKind::UnknownVersion | MetadataErrorKind::WrongKind => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FormatRecord {
    layout: String,
}

impl FormatRecord {
    pub(super) fn new() -> Self {
        Self {
            layout: "hf-store-v1".to_owned(),
        }
    }
}

impl CacheRecord for FormatRecord {
    const KIND: &'static str = "format";

    fn validate(&self) -> Result<(), ValidationError> {
        if self.layout == "hf-store-v1" {
            Ok(())
        } else {
            Err(record_malformed("cache format"))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OriginRecord {
    endpoint: String,
}

impl OriginRecord {
    pub(super) fn new(endpoint: &Endpoint) -> Self {
        Self {
            endpoint: endpoint.as_str().to_owned(),
        }
    }
}

impl CacheRecord for OriginRecord {
    const KIND: &'static str = "origin";

    fn validate(&self) -> Result<(), ValidationError> {
        let endpoint = Endpoint::parse(&self.endpoint)?;
        if endpoint.as_str() == self.endpoint {
            Ok(())
        } else {
            Err(record_malformed("origin record"))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepositoryRecord {
    origin_key: String,
    repository_key: String,
    kind: String,
    repository_id: String,
}

impl RepositoryRecord {
    pub(super) fn new(
        origin: &OriginKey,
        repository: &RepositoryKey,
        spec: &RepositorySpec,
    ) -> Self {
        Self {
            origin_key: origin.to_string(),
            repository_key: repository.to_string(),
            kind: repository_kind_name(spec.kind()).to_owned(),
            repository_id: spec.id().as_str().to_owned(),
        }
    }
}

impl CacheRecord for RepositoryRecord {
    const KIND: &'static str = "repository";

    fn validate(&self) -> Result<(), ValidationError> {
        validate_sha256_hex(&self.origin_key, "origin key")?;
        validate_sha256_hex(&self.repository_key, "repository key")?;
        let _id = RepositoryId::parse(&self.repository_id)?;
        if matches!(self.kind.as_str(), "model" | "dataset" | "space") {
            Ok(())
        } else {
            Err(record_malformed("repository record"))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RefRecord {
    revision: String,
    commit: String,
}

impl RefRecord {
    pub(super) fn new(revision: &Revision, commit: &CommitId) -> Self {
        Self {
            revision: revision.as_str().to_owned(),
            commit: commit.as_str().to_owned(),
        }
    }

    pub(super) fn commit(&self) -> &str {
        &self.commit
    }

    pub(super) fn revision(&self) -> &str {
        &self.revision
    }
}

impl CacheRecord for RefRecord {
    const KIND: &'static str = "ref";

    fn validate(&self) -> Result<(), ValidationError> {
        let _revision = Revision::parse(&self.revision)?;
        let _commit = CommitId::parse(&self.commit)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BlobRecord {
    sha256: String,
    size: u64,
    hub_blob_key: Option<String>,
}

impl BlobRecord {
    pub(super) fn new(digest: &BlobDigest, size: u64, hub_blob_key: Option<&HubBlobKey>) -> Self {
        Self {
            sha256: digest.to_string(),
            size,
            hub_blob_key: hub_blob_key.map(|key| key.as_str().to_owned()),
        }
    }
}

impl CacheRecord for BlobRecord {
    const KIND: &'static str = "blob";

    fn validate(&self) -> Result<(), ValidationError> {
        validate_sha256_hex(&self.sha256, "blob digest")?;
        validate_optional_hub_blob_key(self.hub_blob_key.as_deref())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HubBlobBindingRecord {
    hub_blob_key: String,
    sha256: String,
    size: u64,
}

impl HubBlobBindingRecord {
    pub(super) fn new(hub_blob_key: &HubBlobKey, digest: BlobDigest, size: u64) -> Self {
        Self {
            hub_blob_key: hub_blob_key.as_str().to_owned(),
            sha256: digest.to_string(),
            size,
        }
    }

    pub(super) fn hub_blob_key(&self) -> &str {
        &self.hub_blob_key
    }

    pub(super) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(super) fn digest(&self) -> Result<BlobDigest, ValidationError> {
        BlobDigest::parse(&self.sha256)
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }
}

impl CacheRecord for HubBlobBindingRecord {
    const KIND: &'static str = "hub_blob_binding";

    fn validate(&self) -> Result<(), ValidationError> {
        let _hub_blob_key = HubBlobKey::parse(&self.hub_blob_key)?;
        validate_sha256_hex(&self.sha256, "Hub blob binding digest")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteFileRecord {
    path: String,
    size: u64,
    hub_blob_key: Option<String>,
}

impl RemoteFileRecord {
    pub(super) fn new(path: &RepoPath, size: u64, hub_blob_key: Option<HubBlobKey>) -> Self {
        Self {
            path: path.as_str().to_owned(),
            size,
            hub_blob_key: hub_blob_key.map(|key| key.as_str().to_owned()),
        }
    }

    pub(super) fn path(&self) -> &str {
        &self.path
    }

    fn validate(&self) -> Result<RepoPath, ValidationError> {
        let path = RepoPath::parse(&self.path)?;
        validate_optional_hub_blob_key(self.hub_blob_key.as_deref())?;
        Ok(path)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RemoteTreeRecord {
    commit: String,
    files: Vec<RemoteFileRecord>,
}

impl RemoteTreeRecord {
    pub(super) fn new(
        commit: &CommitId,
        mut files: Vec<RemoteFileRecord>,
    ) -> Result<Self, ValidationError> {
        files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        validate_remote_files(&files)?;
        Ok(Self {
            commit: commit.as_str().to_owned(),
            files,
        })
    }

    pub(super) fn files(&self) -> &[RemoteFileRecord] {
        &self.files
    }
}

impl CacheRecord for RemoteTreeRecord {
    const KIND: &'static str = "remote_tree";

    fn validate(&self) -> Result<(), ValidationError> {
        let _commit = CommitId::parse(&self.commit)?;
        validate_remote_files(&self.files)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PartialTransferRecord {
    commit: String,
    path: String,
    expected_size: u64,
    received_size: u64,
    validator: Option<String>,
    target_sha256: Option<String>,
    updated_unix_millis: u64,
}

impl PartialTransferRecord {
    #[allow(
        clippy::too_many_arguments,
        reason = "the transfer resume identity is intentionally explicit"
    )]
    pub(super) fn new(
        commit: &CommitId,
        path: &RepoPath,
        expected_size: u64,
        received_size: u64,
        validator: Option<String>,
        target_digest: Option<BlobDigest>,
        updated_unix_millis: u64,
    ) -> Result<Self, ValidationError> {
        let record = Self {
            commit: commit.as_str().to_owned(),
            path: path.as_str().to_owned(),
            expected_size,
            received_size,
            validator,
            target_sha256: target_digest.map(|digest| digest.to_string()),
            updated_unix_millis,
        };
        record.validate()?;
        Ok(record)
    }

    pub(super) const fn updated_unix_millis(&self) -> u64 {
        self.updated_unix_millis
    }
}

impl CacheRecord for PartialTransferRecord {
    const KIND: &'static str = "partial_transfer";

    fn validate(&self) -> Result<(), ValidationError> {
        let _commit = CommitId::parse(&self.commit)?;
        let _path = RepoPath::parse(&self.path)?;
        if self.received_size > self.expected_size {
            return Err(record_malformed("partial transfer record"));
        }
        if let Some(validator) = self.validator.as_deref() {
            if validator.is_empty() {
                return Err(record_malformed("partial transfer validator"));
            }
            if validator.contains('\0') {
                return Err(ValidationError::new(
                    "partial transfer validator",
                    ValidationErrorKind::ContainsNul,
                ));
            }
        }
        if let Some(digest) = &self.target_sha256 {
            validate_sha256_hex(digest, "partial transfer digest")?;
        }
        if self.received_size > 0 && self.validator.is_none() && self.target_sha256.is_none() {
            return Err(record_malformed("partial transfer resume identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotFileRecord {
    path: String,
    sha256: String,
    size: u64,
    hub_blob_key: Option<String>,
}

impl SnapshotFileRecord {
    pub(super) fn new(
        path: &RepoPath,
        digest: BlobDigest,
        size: u64,
        hub_blob_key: Option<HubBlobKey>,
    ) -> Self {
        Self {
            path: path.as_str().to_owned(),
            sha256: digest.to_string(),
            size,
            hub_blob_key: hub_blob_key.map(|key| key.as_str().to_owned()),
        }
    }

    pub(super) fn path(&self) -> &str {
        &self.path
    }

    pub(super) fn digest(&self) -> Result<BlobDigest, ValidationError> {
        BlobDigest::parse(&self.sha256)
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }

    pub(super) fn hub_blob_key(&self) -> Result<Option<HubBlobKey>, ValidationError> {
        self.hub_blob_key
            .as_deref()
            .map(HubBlobKey::parse)
            .transpose()
    }

    fn validate(&self) -> Result<RepoPath, ValidationError> {
        let path = RepoPath::parse(&self.path)?;
        validate_sha256_hex(&self.sha256, "snapshot blob digest")?;
        validate_optional_hub_blob_key(self.hub_blob_key.as_deref())?;
        Ok(path)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SnapshotManifestRecord {
    commit: String,
    selection_id: String,
    files: Vec<SnapshotFileRecord>,
}

impl SnapshotManifestRecord {
    pub(super) fn new(
        commit: &CommitId,
        selection: &SelectionId,
        mut files: Vec<SnapshotFileRecord>,
    ) -> Result<Self, ValidationError> {
        files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        validate_snapshot_files(&files, selection)?;
        Ok(Self {
            commit: commit.as_str().to_owned(),
            selection_id: selection.to_string(),
            files,
        })
    }

    pub(super) fn files(&self) -> &[SnapshotFileRecord] {
        &self.files
    }

    pub(super) fn commit(&self) -> &str {
        &self.commit
    }

    pub(super) fn selection_id(&self) -> &str {
        &self.selection_id
    }
}

impl CacheRecord for SnapshotManifestRecord {
    const KIND: &'static str = "snapshot_manifest";

    fn validate(&self) -> Result<(), ValidationError> {
        let _commit = CommitId::parse(&self.commit)?;
        validate_sha256_hex(&self.selection_id, "selection identifier")?;
        let paths = validate_snapshot_file_paths(&self.files)?;
        let actual = SelectionId::derive(&paths)?;
        if actual.to_string() == self.selection_id {
            Ok(())
        } else {
            Err(record_malformed("snapshot manifest"))
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalDirFileRecord {
    path: String,
    sha256: String,
    size: u64,
}

impl LocalDirFileRecord {
    pub(super) fn new(path: &RepoPath, digest: BlobDigest, size: u64) -> Self {
        Self {
            path: path.as_str().to_owned(),
            sha256: digest.to_string(),
            size,
        }
    }

    pub(super) fn path(&self) -> &str {
        &self.path
    }

    pub(super) fn digest(&self) -> Result<BlobDigest, ValidationError> {
        BlobDigest::parse(&self.sha256)
    }

    pub(super) const fn size(&self) -> u64 {
        self.size
    }

    fn validate(&self) -> Result<RepoPath, ValidationError> {
        let path = RepoPath::parse(&self.path)?;
        let _digest = self.digest()?;
        Ok(path)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LocalDirStateKind {
    InProgress,
    Complete,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalDirStateRecord {
    origin_key: String,
    repository_key: String,
    commit: String,
    selection_id: String,
    state: LocalDirStateKind,
    files: Vec<LocalDirFileRecord>,
}

impl LocalDirStateRecord {
    pub(super) fn in_progress(
        origin: &OriginKey,
        repository: &RepositoryKey,
        commit: &CommitId,
        selection: &SelectionId,
    ) -> Result<Self, ValidationError> {
        Self::new(
            origin,
            repository,
            commit,
            selection,
            LocalDirStateKind::InProgress,
            Vec::new(),
        )
    }

    pub(super) fn complete(
        origin: &OriginKey,
        repository: &RepositoryKey,
        commit: &CommitId,
        selection: &SelectionId,
        mut files: Vec<LocalDirFileRecord>,
    ) -> Result<Self, ValidationError> {
        files.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        Self::new(
            origin,
            repository,
            commit,
            selection,
            LocalDirStateKind::Complete,
            files,
        )
    }

    fn new(
        origin: &OriginKey,
        repository: &RepositoryKey,
        commit: &CommitId,
        selection: &SelectionId,
        state: LocalDirStateKind,
        files: Vec<LocalDirFileRecord>,
    ) -> Result<Self, ValidationError> {
        let record = Self {
            origin_key: origin.to_string(),
            repository_key: repository.to_string(),
            commit: commit.as_str().to_owned(),
            selection_id: selection.to_string(),
            state,
            files,
        };
        record.validate()?;
        Ok(record)
    }

    pub(super) const fn is_in_progress(&self) -> bool {
        matches!(self.state, LocalDirStateKind::InProgress)
    }

    pub(super) const fn is_complete(&self) -> bool {
        matches!(self.state, LocalDirStateKind::Complete)
    }

    pub(super) fn files(&self) -> &[LocalDirFileRecord] {
        &self.files
    }

    pub(super) fn commit(&self) -> &str {
        &self.commit
    }

    pub(super) fn selection_id(&self) -> &str {
        &self.selection_id
    }
}

impl CacheRecord for LocalDirStateRecord {
    const KIND: &'static str = "local_dir_state";

    fn validate(&self) -> Result<(), ValidationError> {
        validate_sha256_hex(&self.origin_key, "local directory origin key")?;
        validate_sha256_hex(&self.repository_key, "local directory repository key")?;
        let _commit = CommitId::parse(&self.commit)?;
        validate_sha256_hex(&self.selection_id, "local directory selection identifier")?;
        let paths = self
            .files
            .iter()
            .map(LocalDirFileRecord::validate)
            .collect::<Result<Vec<_>, _>>()?;
        validate_order_and_selection(self.files.iter().map(LocalDirFileRecord::path), &paths)?;
        match self.state {
            LocalDirStateKind::InProgress if self.files.is_empty() => Ok(()),
            LocalDirStateKind::InProgress => Err(record_malformed("local directory state")),
            LocalDirStateKind::Complete => {
                let selection = SelectionId::derive(&paths)?;
                if selection.to_string() == self.selection_id {
                    Ok(())
                } else {
                    Err(record_malformed("local directory state"))
                }
            }
        }
    }
}

fn validate_remote_files(files: &[RemoteFileRecord]) -> Result<(), ValidationError> {
    let paths = files
        .iter()
        .map(RemoteFileRecord::validate)
        .collect::<Result<Vec<_>, _>>()?;
    validate_order_and_selection(files.iter().map(RemoteFileRecord::path), &paths)
}

fn validate_snapshot_files(
    files: &[SnapshotFileRecord],
    selection: &SelectionId,
) -> Result<(), ValidationError> {
    let paths = validate_snapshot_file_paths(files)?;
    let actual = SelectionId::derive(&paths)?;
    if &actual == selection {
        Ok(())
    } else {
        Err(record_malformed("snapshot manifest"))
    }
}

fn validate_snapshot_file_paths(
    files: &[SnapshotFileRecord],
) -> Result<Vec<RepoPath>, ValidationError> {
    let paths = files
        .iter()
        .map(SnapshotFileRecord::validate)
        .collect::<Result<Vec<_>, _>>()?;
    validate_order_and_selection(files.iter().map(SnapshotFileRecord::path), &paths)?;
    Ok(paths)
}

fn validate_order_and_selection<'a>(
    stored_paths: impl Iterator<Item = &'a str>,
    paths: &[RepoPath],
) -> Result<(), ValidationError> {
    let stored = stored_paths.collect::<Vec<_>>();
    if stored.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(ValidationError::new(
            "cache record path list",
            ValidationErrorKind::Collision,
        ));
    }
    let _selection = SelectionId::derive(paths)?;
    Ok(())
}

fn validate_optional_hub_blob_key(value: Option<&str>) -> Result<(), ValidationError> {
    if let Some(value) = value {
        let _key = HubBlobKey::parse(value)?;
    }
    Ok(())
}

fn validate_sha256_hex(value: &str, subject: &'static str) -> Result<(), ValidationError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(record_malformed(subject))
    }
}

const fn repository_kind_name(kind: RepositoryKind) -> &'static str {
    match kind {
        RepositoryKind::Model => "model",
        RepositoryKind::Dataset => "dataset",
        RepositoryKind::Space => "space",
    }
}

fn record_malformed(subject: &'static str) -> ValidationError {
    ValidationError::new(subject, ValidationErrorKind::Malformed)
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn every_phase_one_record_round_trips() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let origin_key = OriginKey::derive(&endpoint)?;
        let repository_key = RepositoryKey::derive(&origin_key, &spec)?;
        let revision = Revision::parse("refs/pr/7")?;
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let digest = BlobDigest::for_bytes(b"payload");
        let hub_blob = HubBlobKey::parse("0123456789abcdef")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;

        assert_round_trip(&FormatRecord::new())?;
        assert_round_trip(&OriginRecord::new(&endpoint))?;
        assert_round_trip(&RepositoryRecord::new(&origin_key, &repository_key, &spec))?;
        assert_round_trip(&RefRecord::new(&revision, &commit))?;
        assert_round_trip(&BlobRecord::new(&digest, 7, Some(&hub_blob)))?;
        assert_round_trip(&HubBlobBindingRecord::new(&hub_blob, digest, 7))?;
        assert_round_trip(&RemoteTreeRecord::new(
            &commit,
            vec![RemoteFileRecord::new(&path, 7, Some(hub_blob.clone()))],
        )?)?;
        assert_round_trip(&PartialTransferRecord::new(
            &commit,
            &path,
            7,
            3,
            Some("opaque-etag".to_owned()),
            Some(digest),
            1_721_596_800_000,
        )?)?;
        assert_round_trip(&SnapshotManifestRecord::new(
            &commit,
            &selection,
            vec![SnapshotFileRecord::new(&path, digest, 7, Some(hub_blob))],
        )?)?;
        assert_round_trip(&LocalDirStateRecord::in_progress(
            &origin_key,
            &repository_key,
            &commit,
            &selection,
        )?)?;
        assert_round_trip(&LocalDirStateRecord::complete(
            &origin_key,
            &repository_key,
            &commit,
            &selection,
            vec![LocalDirFileRecord::new(&path, digest, 7)],
        )?)?;

        Ok(())
    }

    #[test]
    fn local_dir_state_has_stable_in_progress_and_complete_encodings()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let origin = OriginKey::derive(&endpoint)?;
        let repository = RepositoryKey::derive(&origin, &spec)?;
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&path))?;
        let digest = BlobDigest::for_bytes(b"payload");

        let in_progress =
            LocalDirStateRecord::in_progress(&origin, &repository, &commit, &selection)?;
        let complete = LocalDirStateRecord::complete(
            &origin,
            &repository,
            &commit,
            &selection,
            vec![LocalDirFileRecord::new(&path, digest, 7)],
        )?;

        assert!(in_progress.is_in_progress());
        assert!(in_progress.files().is_empty());
        assert!(complete.is_complete());
        assert_eq!(complete.files()[0].path(), path.as_str());
        assert_eq!(complete.files()[0].digest()?, digest);
        assert_eq!(complete.files()[0].size(), 7);
        let encoded = String::from_utf8(encode_record(&complete)?)?;
        assert!(encoded.contains("\"record_kind\":\"local_dir_state\""));
        assert!(encoded.contains("\"state\":\"complete\""));
        assert!(encoded.contains("\"path\":\"weights/model.bin\""));
        assert!(!encoded.contains("hub_blob_key"));
        Ok(())
    }

    #[test]
    fn local_dir_state_rejects_files_in_progress_and_selection_mismatch()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let origin = OriginKey::derive(&endpoint)?;
        let repository = RepositoryKey::derive(&origin, &spec)?;
        let commit = CommitId::parse(COMMIT)?;
        let selected = RepoPath::parse("selected.bin")?;
        let other = RepoPath::parse("other.bin")?;
        let selection = SelectionId::derive(std::slice::from_ref(&selected))?;
        let file = LocalDirFileRecord::new(&other, BlobDigest::for_bytes(b"other"), 5);

        LocalDirStateRecord::complete(&origin, &repository, &commit, &selection, vec![file])
            .expect_err("accepted files that did not match the selection identity");

        let malformed = format!(
            concat!(
                "{{\"format_version\":1,\"record_kind\":\"local_dir_state\",\"payload\":{{",
                "\"origin_key\":\"{}\",\"repository_key\":\"{}\",",
                "\"commit\":\"{}\",\"selection_id\":\"{}\",",
                "\"state\":\"in_progress\",\"files\":[{{\"path\":\"other.bin\",",
                "\"sha256\":\"{}\",\"size\":5}}]}}}}"
            ),
            origin,
            repository,
            commit,
            selection,
            BlobDigest::for_bytes(b"other")
        );
        decode_record::<LocalDirStateRecord>(malformed.as_bytes())
            .expect_err("decoded an in-progress state containing completion files");
        Ok(())
    }

    #[test]
    fn hub_blob_binding_has_stable_canonical_json() -> Result<(), Box<dyn std::error::Error>> {
        let hub_blob = HubBlobKey::parse("0123456789abcdef")?;
        let digest = BlobDigest::for_bytes(b"payload");
        let encoded = encode_record(&HubBlobBindingRecord::new(&hub_blob, digest, 7))?;

        assert_eq!(
            std::str::from_utf8(&encoded)?,
            concat!(
                "{\"format_version\":1,\"record_kind\":\"hub_blob_binding\",\"payload\":{",
                "\"hub_blob_key\":\"0123456789abcdef\",",
                "\"sha256\":\"239f59ed55e737c77147cf55ad0c1b030b6d7ee748a7426952f9b852d5a935e5\",",
                "\"size\":7}}\n"
            )
        );
        let decoded = decode_record::<HubBlobBindingRecord>(&encoded)?;
        assert_eq!(decoded.hub_blob_key(), hub_blob.as_str());
        assert_eq!(decoded.digest()?, digest);
        assert_eq!(decoded.size(), 7);

        Ok(())
    }

    #[test]
    fn ref_record_has_stable_canonical_json() -> Result<(), Box<dyn std::error::Error>> {
        let revision = Revision::parse("refs/pr/7")?;
        let commit = CommitId::parse(COMMIT)?;
        let encoded = encode_record(&RefRecord::new(&revision, &commit))?;

        assert_eq!(
            String::from_utf8(encoded)?,
            concat!(
                "{\"format_version\":1,\"record_kind\":\"ref\",\"payload\":{",
                "\"revision\":\"refs/pr/7\",",
                "\"commit\":\"0123456789abcdef0123456789abcdef01234567\"}}\n"
            )
        );

        Ok(())
    }

    #[test]
    fn metadata_rejects_unknown_versions_wrong_kinds_and_corruption() {
        let unknown = br#"{"format_version":2,"record_kind":"ref","payload":{"revision":"main","commit":"0123456789abcdef0123456789abcdef01234567"}}"#;
        let unknown_with_future_payload =
            br#"{"format_version":2,"record_kind":"ref","payload":{"future_ref_encoding":true}}"#;
        let unknown_with_future_envelope = br#"{"format_version":2,"record_kind":"ref","future_header":true,"payload":{"future_ref_encoding":true}}"#;
        let wrong_kind = br#"{"format_version":1,"record_kind":"origin","payload":{"revision":"main","commit":"0123456789abcdef0123456789abcdef01234567"}}"#;
        let reserved_hub_blob = br#"{"format_version":1,"record_kind":"blob","payload":{"sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","size":1,"hub_blob_key":"etag.incomplete"}}"#;

        let version_error = decode_record::<RefRecord>(unknown)
            .expect_err("unknown metadata version must be rejected");
        assert!(version_error.is_unknown_version());
        let future_payload_error = decode_record::<RefRecord>(unknown_with_future_payload)
            .expect_err("version must be classified before decoding its payload");
        assert!(future_payload_error.is_unknown_version());
        let future_envelope_error = decode_record::<RefRecord>(unknown_with_future_envelope)
            .expect_err("unknown versions must be classified before strict envelope decoding");
        assert!(future_envelope_error.is_unknown_version());

        let kind_error = decode_record::<RefRecord>(wrong_kind)
            .expect_err("wrong metadata kind must be rejected");
        assert!(kind_error.is_corrupt());

        let syntax_error =
            decode_record::<RefRecord>(b"{not-json").expect_err("corrupt JSON must be rejected");
        assert!(syntax_error.is_corrupt());

        let reserved_blob_error = decode_record::<BlobRecord>(reserved_hub_blob)
            .expect_err("upstream partial-transfer names must not become blob identities");
        assert!(reserved_blob_error.is_corrupt());
    }

    #[test]
    fn tree_and_manifest_serialization_sort_paths() -> Result<(), Box<dyn std::error::Error>> {
        let commit = CommitId::parse(COMMIT)?;
        let first = RepoPath::parse("a.json")?;
        let second = RepoPath::parse("z.bin")?;
        let digest = BlobDigest::for_bytes(b"payload");
        let selection = SelectionId::derive(&[second.clone(), first.clone()])?;

        let tree = RemoteTreeRecord::new(
            &commit,
            vec![
                RemoteFileRecord::new(&second, 2, None),
                RemoteFileRecord::new(&first, 1, None),
            ],
        )?;
        let manifest = SnapshotManifestRecord::new(
            &commit,
            &selection,
            vec![
                SnapshotFileRecord::new(&second, digest, 2, None),
                SnapshotFileRecord::new(&first, digest, 1, None),
            ],
        )?;

        assert_eq!(tree.files()[0].path(), "a.json");
        assert_eq!(manifest.files()[0].path(), "a.json");
        assert_eq!(manifest.commit(), COMMIT);
        assert_eq!(manifest.selection_id(), selection.to_string());
        assert_eq!(manifest.files()[0].digest()?, digest);
        assert_eq!(manifest.files()[0].size(), 1);
        assert!(manifest.files()[0].hub_blob_key()?.is_none());

        Ok(())
    }

    #[test]
    fn resumable_partials_require_immutable_remote_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let commit = CommitId::parse(COMMIT)?;
        let path = RepoPath::parse("weights/model.bin")?;

        PartialTransferRecord::new(&commit, &path, 10, 1, None, None, 42)
            .expect_err("received bytes without a validator or target must not be resumable");
        PartialTransferRecord::new(&commit, &path, 10, 1, Some(String::new()), None, 42)
            .expect_err("an empty remote validator is not a stable resume identity");

        Ok(())
    }

    fn assert_round_trip<T>(record: &T) -> Result<(), Box<dyn std::error::Error>>
    where
        T: CacheRecord + PartialEq + std::fmt::Debug,
    {
        let encoded = encode_record(record)?;
        assert_eq!(&decode_record::<T>(&encoded)?, record);
        Ok(())
    }
}
