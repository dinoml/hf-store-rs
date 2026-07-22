use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use sha2::{Digest, Sha256};
use unicode_casefold::UnicodeCaseFold;
use unicode_normalization::UnicodeNormalization;

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{Endpoint, RepoPath, RepositorySpec, Revision};

const KEY_PREFIX: &[u8] = b"hf-store-cache-key\0";
const KEY_FORMAT_VERSION: u8 = 1;
const ORIGIN_DOMAIN: u8 = 1;
const REPOSITORY_DOMAIN: u8 = 2;
const REVISION_DOMAIN: u8 = 3;
const SELECTION_DOMAIN: u8 = 4;
const HUB_BLOB_BINDING_DOMAIN: u8 = 5;
const PARTIAL_TRANSFER_DOMAIN: u8 = 6;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct CacheKey([u8; 32]);

impl CacheKey {
    const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl Display for CacheKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write_hex(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct OriginKey(CacheKey);

impl OriginKey {
    pub(super) fn derive(endpoint: &Endpoint) -> Result<Self, ValidationError> {
        derive_key(ORIGIN_DOMAIN, &[endpoint.as_str().as_bytes()]).map(Self)
    }

    const fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl Display for OriginKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct RepositoryKey(CacheKey);

impl RepositoryKey {
    pub(super) fn derive(
        origin: &OriginKey,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        let kind = [spec.kind().cache_tag()];
        derive_key(
            REPOSITORY_DOMAIN,
            &[origin.as_bytes(), &kind, spec.id().as_str().as_bytes()],
        )
        .map(Self)
    }

    const fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl Display for RepositoryKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct RevisionKey(CacheKey);

impl RevisionKey {
    pub(super) fn derive(
        repository: &RepositoryKey,
        revision: &Revision,
    ) -> Result<Self, ValidationError> {
        derive_key(
            REVISION_DOMAIN,
            &[repository.as_bytes(), revision.as_str().as_bytes()],
        )
        .map(Self)
    }
}

impl Display for RevisionKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// Stable identity of an exact, portable repository path selection.
pub struct SelectionId(CacheKey);

impl SelectionId {
    pub(crate) fn derive(paths: &[RepoPath]) -> Result<Self, ValidationError> {
        let mut exact = paths.iter().map(RepoPath::as_str).collect::<Vec<_>>();
        exact.sort_unstable();
        exact.dedup();
        validate_materialization_collisions(&exact)?;

        let components = exact.iter().map(|path| path.as_bytes()).collect::<Vec<_>>();
        derive_key(SELECTION_DOMAIN, &components).map(Self)
    }
}

impl Display for SelectionId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct HubBlobBindingKey(CacheKey);

impl HubBlobBindingKey {
    pub(super) fn derive(
        repository: &RepositoryKey,
        hub_blob_key: &str,
    ) -> Result<Self, ValidationError> {
        derive_key(
            HUB_BLOB_BINDING_DOMAIN,
            &[repository.as_bytes(), hub_blob_key.as_bytes()],
        )
        .map(Self)
    }
}

impl Display for HubBlobBindingKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct PartialTransferKey(CacheKey);

impl PartialTransferKey {
    pub(super) fn derive(
        repository: &RepositoryKey,
        commit: &crate::CommitId,
        path: &RepoPath,
    ) -> Result<Self, ValidationError> {
        derive_key(
            PARTIAL_TRANSFER_DOMAIN,
            &[
                repository.as_bytes(),
                commit.as_str().as_bytes(),
                path.as_str().as_bytes(),
            ],
        )
        .map(Self)
    }
}

impl Display for PartialTransferKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct BlobDigest([u8; 32]);

impl BlobDigest {
    pub(super) fn for_bytes(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub(super) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(super) fn parse(value: &str) -> Result<Self, ValidationError> {
        let encoded = value.as_bytes();
        if encoded.len() != 64 {
            return Err(blob_digest_malformed());
        }

        let mut decoded = [0_u8; 32];
        for (index, pair) in encoded.chunks_exact(2).enumerate() {
            let high = lower_hex_nibble(pair[0]).ok_or_else(blob_digest_malformed)?;
            let low = lower_hex_nibble(pair[1]).ok_or_else(blob_digest_malformed)?;
            decoded[index] = (high << 4) | low;
        }
        Ok(Self(decoded))
    }
}

impl Display for BlobDigest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write_hex(&self.0, formatter)
    }
}

fn derive_key(domain: u8, components: &[&[u8]]) -> Result<CacheKey, ValidationError> {
    let count = u32::try_from(components.len()).map_err(|_overflow| key_input_too_long())?;
    let mut hasher = Sha256::new();
    hasher.update(KEY_PREFIX);
    hasher.update([KEY_FORMAT_VERSION, domain]);
    hasher.update(count.to_be_bytes());

    for component in components {
        let length = u64::try_from(component.len()).map_err(|_overflow| key_input_too_long())?;
        hasher.update(length.to_be_bytes());
        hasher.update(component);
    }

    Ok(CacheKey(hasher.finalize().into()))
}

fn validate_materialization_collisions(paths: &[&str]) -> Result<(), ValidationError> {
    let mut portable = paths
        .iter()
        .map(|path| (portable_path_key(path), *path))
        .collect::<Vec<_>>();
    portable.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    for pair in portable.windows(2) {
        let (left_key, left_original) = &pair[0];
        let (right_key, right_original) = &pair[1];
        if left_key == right_key && left_original != right_original {
            return Err(materialization_collision());
        }
    }

    let portable_keys = portable
        .iter()
        .map(|(key, _original)| key.as_str())
        .collect::<BTreeSet<_>>();
    for key in &portable_keys {
        for (separator, _) in key.match_indices('/') {
            if portable_keys.contains(&key[..separator]) {
                return Err(materialization_collision());
            }
        }
    }

    Ok(())
}

pub(super) fn portable_path_key(path: &str) -> String {
    let normalized = path.nfc().collect::<String>();
    normalized.as_str().case_fold().nfc().collect()
}

fn materialization_collision() -> ValidationError {
    ValidationError::new("repository path selection", ValidationErrorKind::Collision)
}

fn key_input_too_long() -> ValidationError {
    ValidationError::new("cache key input", ValidationErrorKind::TooLong)
}

const fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn blob_digest_malformed() -> ValidationError {
    ValidationError::new("blob digest", ValidationErrorKind::Malformed)
}

fn write_hex(bytes: &[u8], formatter: &mut Formatter<'_>) -> fmt::Result {
    for byte in bytes {
        write!(formatter, "{byte:02x}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::RepositoryId;

    use super::*;

    #[test]
    fn cache_keys_match_independent_golden_vectors() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::parse("https://huggingface.co")?;
        let spec = RepositorySpec::model(RepositoryId::parse("openai/whisper")?);
        let revision = Revision::parse("refs/pr/7")?;
        let paths = [
            RepoPath::parse("config.json")?,
            RepoPath::parse("model.safetensors")?,
        ];

        let origin = OriginKey::derive(&endpoint)?;
        let repository = RepositoryKey::derive(&origin, &spec)?;
        let revision = RevisionKey::derive(&repository, &revision)?;
        let selection = SelectionId::derive(&paths)?;

        assert_eq!(
            origin.to_string(),
            "19298869894db36725655e1a2e8c11e0c246e7f853a2c35eb445e43b95e47a98"
        );
        assert_eq!(
            repository.to_string(),
            "0a4413781fc420e429e6e067f55f9f79d1a66159f966b86e520f0f3bbe6dffae"
        );
        assert_eq!(
            revision.to_string(),
            "6ecdcffb4133cd848678d40face04b4fb1437ea4eed2bcc9cfc3eda166a712e5"
        );
        assert_eq!(
            selection.to_string(),
            "a2d5850af64acf2289182f3b5158ba54d72ff197e3496b643d26867353c89c33"
        );

        Ok(())
    }

    #[test]
    fn key_domains_separate_equal_source_bytes() -> Result<(), Box<dyn std::error::Error>> {
        let source = b"the same source bytes";
        let origin = derive_key(ORIGIN_DOMAIN, &[source])?;
        let direct_selection = derive_key(SELECTION_DOMAIN, &[source])?;

        assert_ne!(origin, direct_selection);

        Ok(())
    }

    #[test]
    fn hub_blob_binding_keys_match_an_independent_golden_vector()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let origin = OriginKey::derive(&endpoint)?;
        let repository = RepositoryKey::derive(&origin, &spec)?;

        let binding = HubBlobBindingKey::derive(&repository, "45f2f2d3b0f6f8f9e61a")?;

        assert_eq!(
            binding.to_string(),
            "bef25933d4ec07adbf83915e72961d87b86c1008e72985ed927bea3e183723c1"
        );

        Ok(())
    }

    #[test]
    fn blob_digests_decode_only_canonical_lowercase_sha256()
    -> Result<(), Box<dyn std::error::Error>> {
        let expected = BlobDigest::for_bytes(b"validated payload");
        let encoded = expected.to_string();

        assert_eq!(BlobDigest::parse(&encoded)?, expected);
        for invalid in [
            encoded.to_ascii_uppercase(),
            "0".repeat(63),
            "0".repeat(65),
            format!("{}g", "0".repeat(63)),
        ] {
            BlobDigest::parse(&invalid).expect_err("accepted a non-canonical blob digest");
        }

        Ok(())
    }

    #[test]
    fn selection_identity_ignores_order_and_exact_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let first = RepoPath::parse("a/config.json")?;
        let second = RepoPath::parse("z/model.bin")?;

        let ordered = SelectionId::derive(&[first.clone(), second.clone()])?;
        let reordered = SelectionId::derive(&[second.clone(), first.clone()])?;
        let duplicated = SelectionId::derive(&[first.clone(), second, first])?;

        assert_eq!(ordered, reordered);
        assert_eq!(ordered, duplicated);

        Ok(())
    }

    #[test]
    fn selection_rejects_case_unicode_and_file_directory_collisions()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases = vec![
            vec![RepoPath::parse("README.md")?, RepoPath::parse("readme.md")?],
            vec![
                RepoPath::parse("caf\u{e9}.txt")?,
                RepoPath::parse("cafe\u{301}.txt")?,
            ],
            vec![
                RepoPath::parse("\u{3c3}.txt")?,
                RepoPath::parse("\u{3c2}.txt")?,
            ],
            vec![
                RepoPath::parse("weights")?,
                RepoPath::parse("weights/a.bin")?,
            ],
            vec![
                RepoPath::parse("a")?,
                RepoPath::parse("a-b")?,
                RepoPath::parse("a/file.bin")?,
            ],
        ];

        for paths in cases {
            let error = SelectionId::derive(&paths).expect_err("collision must be rejected");
            assert!(error.is_unsafe_path());
        }

        Ok(())
    }
}
