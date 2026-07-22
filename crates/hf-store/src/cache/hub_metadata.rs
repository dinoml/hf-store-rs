use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::num::ParseFloatError;
use std::str::{self, Utf8Error};

use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::ser::PrettyFormatter;

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, RepoPath};

use super::key::SelectionId;

const TREE_FORMAT_VERSION: u32 = 1;
// Hub validators are normally short hashes, but remain protocol-opaque here.
const MAX_OPAQUE_VALUE_BYTES: usize = 8 * 1024;

pub(super) fn decode_ref(bytes: &[u8]) -> Result<CommitId, HubMetadataError> {
    let value = str::from_utf8(bytes).map_err(HubMetadataError::utf8)?;
    CommitId::parse(value).map_err(HubMetadataError::invalid)
}

pub(super) fn encode_ref(commit: &CommitId) -> Vec<u8> {
    commit.as_str().as_bytes().to_vec()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct HubTree {
    files: BTreeMap<RepoPath, HubTreeEntry>,
}

impl HubTree {
    pub(crate) fn new(
        entries: impl IntoIterator<Item = (RepoPath, HubTreeEntry)>,
    ) -> Result<Self, HubMetadataError> {
        let mut files = BTreeMap::new();
        for (path, entry) in entries {
            if files.insert(path, entry).is_some() {
                return Err(HubMetadataError::malformed());
            }
        }

        let tree = Self { files };
        tree.validate()?;
        Ok(tree)
    }

    pub(crate) fn files(&self) -> &BTreeMap<RepoPath, HubTreeEntry> {
        &self.files
    }

    fn from_raw(raw: RawTreeRecord) -> Result<Self, HubMetadataError> {
        let mut entries = Vec::with_capacity(raw.files.0.len());
        for (path, entry) in raw.files.0 {
            let path = RepoPath::parse(path).map_err(HubMetadataError::invalid)?;
            let entry = HubTreeEntry::from_raw(entry)?;
            entries.push((path, entry));
        }
        Self::new(entries)
    }

    fn validate(&self) -> Result<(), HubMetadataError> {
        let paths = self.files.keys().cloned().collect::<Vec<_>>();
        SelectionId::derive(&paths).map_err(HubMetadataError::invalid)?;
        for entry in self.files.values() {
            entry.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct HubTreeEntry {
    size: u64,
    blob_id: Box<str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lfs_sha256: Option<Box<str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lfs_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    xet_hash: Option<Box<str>>,
}

impl HubTreeEntry {
    pub(crate) fn new(size: u64, blob_id: impl AsRef<str>) -> Result<Self, HubMetadataError> {
        Ok(Self {
            size,
            blob_id: validate_opaque(blob_id.as_ref().to_owned(), "Hub tree blob identifier")?,
            lfs_sha256: None,
            lfs_size: None,
            xet_hash: None,
        })
    }

    pub(crate) fn with_lfs(
        mut self,
        sha256: impl AsRef<str>,
        size: u64,
    ) -> Result<Self, HubMetadataError> {
        self.lfs_sha256 = Some(validate_opaque(
            sha256.as_ref().to_owned(),
            "Hub tree LFS identifier",
        )?);
        self.lfs_size = Some(size);
        Ok(self)
    }

    pub(crate) fn with_xet(mut self, hash: impl AsRef<str>) -> Result<Self, HubMetadataError> {
        self.xet_hash = Some(validate_opaque(
            hash.as_ref().to_owned(),
            "Hub tree Xet identifier",
        )?);
        Ok(self)
    }

    pub(crate) const fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn blob_id(&self) -> &str {
        &self.blob_id
    }

    pub(crate) fn lfs_sha256(&self) -> Option<&str> {
        self.lfs_sha256.as_deref()
    }

    pub(crate) const fn lfs_size(&self) -> Option<u64> {
        self.lfs_size
    }

    pub(crate) fn xet_hash(&self) -> Option<&str> {
        self.xet_hash.as_deref()
    }

    fn from_raw(raw: RawTreeEntry) -> Result<Self, HubMetadataError> {
        if raw.lfs_sha256.is_some() != raw.lfs_size.is_some() {
            return Err(HubMetadataError::invalid(ValidationError::new(
                "Hub tree LFS metadata",
                ValidationErrorKind::Malformed,
            )));
        }

        let mut entry = Self::new(raw.size, raw.blob_id)?;
        if let (Some(sha256), Some(size)) = (raw.lfs_sha256, raw.lfs_size) {
            entry = entry.with_lfs(sha256, size)?;
        }
        if let Some(hash) = raw.xet_hash {
            entry = entry.with_xet(hash)?;
        }
        Ok(entry)
    }

    fn validate(&self) -> Result<(), HubMetadataError> {
        validate_opaque_ref(&self.blob_id, "Hub tree blob identifier")?;
        validate_optional_opaque_ref(self.lfs_sha256.as_deref(), "Hub tree LFS identifier")?;
        validate_optional_opaque_ref(self.xet_hash.as_deref(), "Hub tree Xet identifier")?;

        if self.lfs_sha256.is_some() != self.lfs_size.is_some() {
            return Err(HubMetadataError::invalid(ValidationError::new(
                "Hub tree LFS metadata",
                ValidationErrorKind::Malformed,
            )));
        }
        Ok(())
    }
}

pub(super) fn decode_tree(bytes: &[u8]) -> Result<HubTree, HubMetadataError> {
    let envelope =
        serde_json::from_slice::<TreeVersionEnvelope>(bytes).map_err(HubMetadataError::decode)?;
    if envelope.format_version != TREE_FORMAT_VERSION {
        return Err(HubMetadataError::unknown_version());
    }
    let raw = serde_json::from_slice::<RawTreeRecord>(bytes).map_err(HubMetadataError::decode)?;
    HubTree::from_raw(raw)
}

pub(super) fn encode_tree(tree: &HubTree) -> Result<Vec<u8>, HubMetadataError> {
    tree.validate()?;
    let record = TreeRecordRef {
        format_version: TREE_FORMAT_VERSION,
        files: TreeFilesRef(&tree.files),
    };
    let mut encoded = Vec::new();
    let formatter = PrettyFormatter::with_indent(b" ");
    let mut serializer = serde_json::Serializer::with_formatter(&mut encoded, formatter);
    record
        .serialize(&mut serializer)
        .map_err(HubMetadataError::encode)?;
    Ok(encoded)
}

#[derive(Deserialize)]
struct TreeVersionEnvelope {
    format_version: u32,
}

#[derive(Deserialize)]
struct RawTreeRecord {
    files: RawTreeFiles,
}

#[derive(Deserialize)]
struct RawTreeEntry {
    size: u64,
    blob_id: String,
    lfs_sha256: Option<String>,
    lfs_size: Option<u64>,
    xet_hash: Option<String>,
}

struct RawTreeFiles(BTreeMap<String, RawTreeEntry>);

impl<'de> Deserialize<'de> for RawTreeFiles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(RawTreeFilesVisitor)
    }
}

struct RawTreeFilesVisitor;

impl<'de> Visitor<'de> for RawTreeFilesVisitor {
    type Value = RawTreeFiles;

    fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map of Hub repository paths to tree metadata")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut files = BTreeMap::new();
        while let Some((path, entry)) = map.next_entry()? {
            if files.insert(path, entry).is_some() {
                return Err(de::Error::custom("duplicate Hub tree path"));
            }
        }
        Ok(RawTreeFiles(files))
    }
}

#[derive(Serialize)]
struct TreeRecordRef<'a> {
    format_version: u32,
    files: TreeFilesRef<'a>,
}

struct TreeFilesRef<'a>(&'a BTreeMap<RepoPath, HubTreeEntry>);

impl Serialize for TreeFilesRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (path, entry) in self.0 {
            map.serialize_entry(path.as_str(), entry)?;
        }
        map.end()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct LocalDownloadMetadata {
    commit: CommitId,
    etag: Box<str>,
    timestamp: f64,
}

impl LocalDownloadMetadata {
    pub(super) fn new(
        commit: CommitId,
        etag: impl AsRef<str>,
        timestamp: f64,
    ) -> Result<Self, HubMetadataError> {
        validate_timestamp(timestamp)?;
        Ok(Self {
            commit,
            etag: validate_opaque(etag.as_ref().to_owned(), "Hub local-dir ETag")?,
            timestamp,
        })
    }

    pub(super) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(super) fn etag(&self) -> &str {
        &self.etag
    }

    pub(super) const fn timestamp(&self) -> f64 {
        self.timestamp
    }
}

pub(super) fn decode_local_download(
    bytes: &[u8],
) -> Result<LocalDownloadMetadata, HubMetadataError> {
    let [commit, etag, timestamp] = three_lines(bytes)?;
    let commit = CommitId::parse(commit).map_err(HubMetadataError::invalid)?;
    let timestamp = timestamp
        .parse::<f64>()
        .map_err(HubMetadataError::timestamp)?;
    LocalDownloadMetadata::new(commit, etag, timestamp)
}

pub(super) fn encode_local_download(metadata: &LocalDownloadMetadata) -> Vec<u8> {
    format!(
        "{}\n{}\n{}\n",
        metadata.commit, metadata.etag, metadata.timestamp
    )
    .into_bytes()
}

fn three_lines(bytes: &[u8]) -> Result<[&str; 3], HubMetadataError> {
    let text = str::from_utf8(bytes).map_err(HubMetadataError::utf8)?;
    if !text.ends_with('\n') {
        return Err(HubMetadataError::malformed());
    }

    let mut lines = text.split_terminator('\n');
    let first = lines.next().ok_or_else(HubMetadataError::malformed)?;
    let second = lines.next().ok_or_else(HubMetadataError::malformed)?;
    let third = lines.next().ok_or_else(HubMetadataError::malformed)?;
    if lines.next().is_some() {
        return Err(HubMetadataError::malformed());
    }

    Ok([
        portable_line(first)?,
        portable_line(second)?,
        portable_line(third)?,
    ])
}

fn portable_line(line: &str) -> Result<&str, HubMetadataError> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    if line.contains('\r') || line.trim() != line {
        Err(HubMetadataError::malformed())
    } else {
        Ok(line)
    }
}

fn validate_timestamp(timestamp: f64) -> Result<(), HubMetadataError> {
    if !timestamp.is_finite() || timestamp.is_sign_negative() {
        Err(HubMetadataError::malformed())
    } else {
        Ok(())
    }
}

fn validate_opaque(value: String, subject: &'static str) -> Result<Box<str>, HubMetadataError> {
    validate_opaque_ref(&value, subject)?;
    Ok(value.into_boxed_str())
}

fn validate_optional_opaque_ref(
    value: Option<&str>,
    subject: &'static str,
) -> Result<(), HubMetadataError> {
    value.map_or(Ok(()), |value| validate_opaque_ref(value, subject))
}

fn validate_opaque_ref(value: &str, subject: &'static str) -> Result<(), HubMetadataError> {
    if value.is_empty()
        || value.len() > MAX_OPAQUE_VALUE_BYTES
        || value.trim() != value
        || value.chars().any(char::is_control)
    {
        return Err(HubMetadataError::invalid(ValidationError::new(
            subject,
            ValidationErrorKind::Malformed,
        )));
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct HubMetadataError {
    kind: HubMetadataErrorKind,
    backtrace: Backtrace,
}

#[derive(Debug)]
enum HubMetadataErrorKind {
    Encode(serde_json::Error),
    Decode(serde_json::Error),
    Utf8(Utf8Error),
    Timestamp(ParseFloatError),
    Invalid(ValidationError),
    Malformed,
    UnknownVersion,
}

impl HubMetadataError {
    fn encode(source: serde_json::Error) -> Self {
        Self::new(HubMetadataErrorKind::Encode(source))
    }

    fn decode(source: serde_json::Error) -> Self {
        Self::new(HubMetadataErrorKind::Decode(source))
    }

    fn utf8(source: Utf8Error) -> Self {
        Self::new(HubMetadataErrorKind::Utf8(source))
    }

    fn timestamp(source: ParseFloatError) -> Self {
        Self::new(HubMetadataErrorKind::Timestamp(source))
    }

    fn invalid(source: ValidationError) -> Self {
        Self::new(HubMetadataErrorKind::Invalid(source))
    }

    fn malformed() -> Self {
        Self::new(HubMetadataErrorKind::Malformed)
    }

    fn unknown_version() -> Self {
        Self::new(HubMetadataErrorKind::UnknownVersion)
    }

    fn new(kind: HubMetadataErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    pub(super) fn is_unknown_version(&self) -> bool {
        matches!(self.kind, HubMetadataErrorKind::UnknownVersion)
    }

    pub(super) fn is_corrupt(&self) -> bool {
        !matches!(
            self.kind,
            HubMetadataErrorKind::Encode(_) | HubMetadataErrorKind::UnknownVersion
        )
    }

    pub(super) fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Display for HubMetadataError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            HubMetadataErrorKind::Encode(_) => "Hub metadata could not be encoded",
            HubMetadataErrorKind::UnknownVersion => "Hub metadata version is unsupported",
            HubMetadataErrorKind::Decode(_)
            | HubMetadataErrorKind::Utf8(_)
            | HubMetadataErrorKind::Timestamp(_)
            | HubMetadataErrorKind::Invalid(_)
            | HubMetadataErrorKind::Malformed => "Hub metadata is corrupt",
        };
        formatter.write_str(message)
    }
}

impl Error for HubMetadataError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            HubMetadataErrorKind::Encode(source) | HubMetadataErrorKind::Decode(source) => {
                Some(source)
            }
            HubMetadataErrorKind::Utf8(source) => Some(source),
            HubMetadataErrorKind::Timestamp(source) => Some(source),
            HubMetadataErrorKind::Invalid(source) => Some(source),
            HubMetadataErrorKind::Malformed | HubMetadataErrorKind::UnknownVersion => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";
    const REF_FIXTURE: &[u8] =
        include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/standard-ref-main");
    const TREE_FIXTURE: &[u8] =
        include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/tree-v1.json");
    const LOCAL_DOWNLOAD_FIXTURE: &[u8] =
        include_bytes!("../../tests/fixtures/huggingface_hub-v1.24.0/local-dir-download.metadata");

    #[test]
    fn pinned_standard_ref_decodes_and_rust_encoding_matches()
    -> Result<(), Box<dyn std::error::Error>> {
        let decoded = decode_ref(REF_FIXTURE)?;
        let constructed = CommitId::parse(COMMIT)?;

        assert_eq!(decoded.as_str(), COMMIT);
        assert_eq!(encode_ref(&constructed), REF_FIXTURE);

        Ok(())
    }

    #[test]
    fn pinned_tree_decodes() -> Result<(), Box<dyn std::error::Error>> {
        let tree = decode_tree(TREE_FIXTURE)?;
        let paths = tree
            .files()
            .keys()
            .map(RepoPath::as_str)
            .collect::<Vec<_>>();

        assert_eq!(paths, ["config.json", "nested/model.safetensors"]);
        assert_eq!(tree.files()[&RepoPath::parse("config.json")?].size(), 5);
        assert_eq!(
            tree.files()[&RepoPath::parse("nested/model.safetensors")?].lfs_size(),
            Some(1024)
        );

        Ok(())
    }

    #[test]
    fn rust_tree_encoding_matches_the_pinned_python_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let config = HubTreeEntry::new(5, "1111111111111111111111111111111111111111")?;
        let model = HubTreeEntry::new(42, "2222222222222222222222222222222222222222")?
            .with_lfs(
                "3333333333333333333333333333333333333333333333333333333333333333",
                1024,
            )?
            .with_xet("4444444444444444444444444444444444444444444444444444444444444444")?;
        let tree = HubTree::new([
            (RepoPath::parse("nested/model.safetensors")?, model),
            (RepoPath::parse("config.json")?, config),
        ])?;

        assert_eq!(encode_tree(&tree)?, TREE_FIXTURE);

        Ok(())
    }

    #[test]
    fn pinned_local_download_metadata_decodes() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = decode_local_download(LOCAL_DOWNLOAD_FIXTURE)?;

        assert_eq!(metadata.commit().as_str(), COMMIT);
        assert_eq!(
            metadata.etag(),
            "3333333333333333333333333333333333333333333333333333333333333333"
        );
        assert_eq!(
            metadata.timestamp().to_bits(),
            1_720_000_000.25_f64.to_bits()
        );

        Ok(())
    }

    #[test]
    fn rust_local_download_encoding_matches_the_pinned_python_writer()
    -> Result<(), Box<dyn std::error::Error>> {
        let metadata = LocalDownloadMetadata::new(
            CommitId::parse(COMMIT)?,
            "3333333333333333333333333333333333333333333333333333333333333333",
            1_720_000_000.25,
        )?;

        assert_eq!(encode_local_download(&metadata), LOCAL_DOWNLOAD_FIXTURE);

        Ok(())
    }

    #[test]
    fn rust_record_constructors_enforce_writer_invariants() -> Result<(), Box<dyn std::error::Error>>
    {
        HubTreeEntry::new(1, "").expect_err("accepted an empty blob identifier");
        LocalDownloadMetadata::new(CommitId::parse(COMMIT)?, "etag", f64::NAN)
            .expect_err("accepted a non-finite timestamp");

        let first = HubTreeEntry::new(1, "abc")?;
        let second = HubTreeEntry::new(1, "def")?;
        HubTree::new([
            (RepoPath::parse("Config.json")?, first),
            (RepoPath::parse("config.json")?, second),
        ])
        .expect_err("accepted a portable path collision");

        Ok(())
    }

    #[test]
    fn compatible_readers_reject_ambiguous_or_unsafe_records() {
        let invalid_refs: [&[u8]; 4] = [
            b"",
            b"0123456789abcdef0123456789abcdef01234567\n",
            b"not-a-commit",
            b"0123456789ABCDEF0123456789ABCDEF01234567",
        ];
        for bytes in invalid_refs {
            assert!(decode_ref(bytes).is_err(), "accepted invalid ref record");
        }

        let unsafe_tree =
            br#"{"format_version":1,"files":{"../token":{"size":1,"blob_id":"abc"}}}"#;
        decode_tree(unsafe_tree).expect_err("accepted an unsafe repository path");

        let incomplete_lfs = br#"{"format_version":1,"files":{"model.bin":{"size":1,"blob_id":"abc","lfs_sha256":"def"}}}"#;
        decode_tree(incomplete_lfs).expect_err("accepted incomplete LFS metadata");

        let extra_local_line =
            "0123456789abcdef0123456789abcdef01234567\netag\n1720000000.25\nignored\n";
        decode_local_download(extra_local_line.as_bytes())
            .expect_err("accepted an extra local-dir metadata line");
    }

    #[test]
    fn tree_reader_accepts_additive_fields_but_rejects_ambiguous_paths() {
        let additive = br#"{"format_version":1,"future":"value","files":{"config.json":{"size":5,"blob_id":"abc","future":true}}}"#;
        decode_tree(additive).expect("rejected additive upstream fields");

        let duplicate = br#"{"format_version":1,"files":{"config.json":{"size":5,"blob_id":"abc"},"config.json":{"size":6,"blob_id":"def"}}}"#;
        decode_tree(duplicate).expect_err("accepted duplicate JSON paths");

        let portable_collision = br#"{"format_version":1,"files":{"Config.json":{"size":5,"blob_id":"abc"},"config.json":{"size":5,"blob_id":"abc"}}}"#;
        decode_tree(portable_collision).expect_err("accepted portable path collision");
    }

    #[test]
    fn tree_reader_distinguishes_unknown_versions_from_corruption() {
        let unknown = decode_tree(br#"{"format_version":2,"future_encoding":true}"#)
            .expect_err("accepted an unknown tree-cache version");
        assert!(unknown.is_unknown_version());
        assert!(!unknown.is_corrupt());

        let corrupt = decode_tree(br#"{"format_version":1,"files":[]}"#)
            .expect_err("accepted a malformed tree-cache payload");
        assert!(corrupt.is_corrupt());
        assert!(!corrupt.is_unknown_version());
    }

    #[test]
    fn local_download_reader_accepts_portable_newlines_and_finite_timestamps()
    -> Result<(), Box<dyn std::error::Error>> {
        let windows = "0123456789abcdef0123456789abcdef01234567\r\netag\r\n1720000000.25\r\n";
        let metadata = decode_local_download(windows.as_bytes())?;
        assert_eq!(metadata.etag(), "etag");

        for timestamp in ["NaN", "inf", "-1"] {
            let bytes = format!("{COMMIT}\netag\n{timestamp}\n");
            assert!(
                decode_local_download(bytes.as_bytes()).is_err(),
                "accepted invalid timestamp {timestamp}"
            );
        }

        Ok(())
    }

    #[test]
    fn generated_upstream_tree_bytes_never_escape_the_parser_contract() {
        let mut state = 0x3c6e_f372_fe94_f82b_u64;
        for length in 0..1024_usize {
            let mut bytes = Vec::with_capacity(length);
            for _index in 0..length {
                state = state.rotate_left(9) ^ 0xa54f_f53a_5f1d_36f1;
                bytes.push(state.to_le_bytes()[0]);
            }
            if let Err(error) = decode_tree(&bytes) {
                let rendered = format!("{error:?} {error}");
                assert!(!rendered.contains("hf_secret"));
            }
        }
    }
}
