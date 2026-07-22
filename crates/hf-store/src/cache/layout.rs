use std::path::{Component, Path, PathBuf};

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, Endpoint, RepositorySpec, Revision};

use super::hub_layout::HubBlobKey;
use super::key::{
    BlobDigest, HubBlobBindingKey, OriginKey, RepositoryKey, RevisionKey, SelectionId,
};

const CACHE_DIRECTORY: &str = "hf-store-v1";

#[derive(Clone, Debug)]
pub(super) struct CacheLayout {
    capability_root: PathBuf,
    cache_root_relative: PathBuf,
    cache_root: PathBuf,
    origin: OriginKey,
    repository: RepositoryKey,
    kind_directory: &'static str,
}

impl CacheLayout {
    pub(super) fn new(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        Self::nested(root, Path::new(""), endpoint, spec)
    }

    pub(super) fn nested(
        root: impl AsRef<Path>,
        parent: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        let root = root.as_ref();
        let parent = parent.as_ref();
        if !parent
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(ValidationError::new(
                "cache capability path",
                ValidationErrorKind::UnsafePath,
            ));
        }
        let origin = OriginKey::derive(endpoint)?;
        let repository = RepositoryKey::derive(&origin, spec)?;
        let cache_root_relative = parent.join(CACHE_DIRECTORY);
        Ok(Self {
            capability_root: root.to_path_buf(),
            cache_root: root.join(&cache_root_relative),
            cache_root_relative,
            origin,
            repository,
            kind_directory: spec.kind().cache_directory(),
        })
    }

    pub(super) fn capability_root(&self) -> &Path {
        &self.capability_root
    }

    pub(super) fn cache_root_relative(&self) -> &Path {
        &self.cache_root_relative
    }

    pub(super) fn cache_root(&self) -> &Path {
        &self.cache_root
    }

    pub(super) fn origin_directory(&self) -> PathBuf {
        self.cache_root
            .join("origins")
            .join(self.origin.to_string())
    }

    pub(super) fn format_record(&self) -> PathBuf {
        self.cache_root.join("format.json")
    }

    pub(super) fn format_lock(&self) -> PathBuf {
        self.cache_root.join("format.lock")
    }

    pub(super) fn origin_record(&self) -> PathBuf {
        self.origin_directory().join("origin.json")
    }

    pub(super) fn repository_directory(&self) -> PathBuf {
        self.origin_directory()
            .join("repos")
            .join(self.kind_directory)
            .join(self.repository.to_string())
    }

    pub(super) fn repository_record(&self) -> PathBuf {
        self.repository_directory().join("repo.json")
    }

    pub(super) fn ref_record(&self, revision: &Revision) -> Result<PathBuf, ValidationError> {
        let key = RevisionKey::derive(&self.repository, revision)?;
        Ok(self
            .repository_directory()
            .join("refs")
            .join(format!("{key}.json")))
    }

    pub(super) fn blob_path(&self, digest: &BlobDigest) -> PathBuf {
        let digest = digest.to_string();
        let (prefix, suffix) = digest.split_at(2);
        self.repository_directory()
            .join("blobs")
            .join("sha256")
            .join(prefix)
            .join(suffix)
    }

    pub(super) fn blob_lock(&self, digest: &BlobDigest) -> PathBuf {
        self.repository_directory()
            .join("locks")
            .join("blobs")
            .join(format!("{digest}.lock"))
    }

    pub(super) fn ref_lock(&self, revision: &Revision) -> Result<PathBuf, ValidationError> {
        let key = RevisionKey::derive(&self.repository, revision)?;
        Ok(self
            .repository_directory()
            .join("locks")
            .join("refs")
            .join(format!("{key}.lock")))
    }

    pub(super) fn staging_directory(&self) -> PathBuf {
        self.repository_directory().join("staging")
    }

    pub(super) fn staged_blob(&self, operation_id: &str) -> PathBuf {
        self.staging_directory()
            .join(format!("{operation_id}.blob"))
    }

    pub(super) fn snapshot_directory(&self, commit: &CommitId, selection: &SelectionId) -> PathBuf {
        self.repository_directory()
            .join("snapshots")
            .join(format!("{commit}-{selection}"))
    }

    pub(super) fn snapshot_manifest(&self, commit: &CommitId, selection: &SelectionId) -> PathBuf {
        self.snapshot_directory(commit, selection)
            .join("manifest.json")
    }

    pub(super) fn snapshot_lock(&self, commit: &CommitId, selection: &SelectionId) -> PathBuf {
        self.repository_directory()
            .join("locks")
            .join("snapshots")
            .join(format!("{commit}-{selection}.lock"))
    }

    pub(super) fn hub_blob_binding_record(
        &self,
        hub_blob_key: &HubBlobKey,
    ) -> Result<PathBuf, ValidationError> {
        let binding = HubBlobBindingKey::derive(&self.repository, hub_blob_key.as_str())?;
        let binding = binding.to_string();
        let (prefix, suffix) = binding.split_at(2);
        Ok(self
            .repository_directory()
            .join("bindings")
            .join("hub-blobs")
            .join(prefix)
            .join(format!("{suffix}.json")))
    }

    pub(super) fn hub_blob_binding_lock(
        &self,
        hub_blob_key: &HubBlobKey,
    ) -> Result<PathBuf, ValidationError> {
        let binding = HubBlobBindingKey::derive(&self.repository, hub_blob_key.as_str())?;
        Ok(self
            .repository_directory()
            .join("locks")
            .join("bindings")
            .join("hub-blobs")
            .join(format!("{binding}.lock")))
    }
}

#[cfg(test)]
mod tests {
    use crate::{RepoPath, RepositoryId};

    use super::*;

    #[test]
    fn layout_uses_only_fixed_keys_for_untrusted_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("cache-root");
        let endpoint = Endpoint::parse("https://hub.example.test/prefix")?;
        let spec = RepositorySpec::dataset(RepositoryId::parse("private-org/private-repo")?);
        let revision = Revision::parse("refs/pr/private-branch")?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let selection = SelectionId::derive(&[RepoPath::parse("private/file.bin")?])?;
        let layout = CacheLayout::new(&root, &endpoint, &spec)?;
        let paths = [
            layout.origin_directory(),
            layout.repository_directory(),
            layout.ref_record(&revision)?,
            layout.snapshot_directory(&commit, &selection),
        ];

        for path in paths {
            assert!(path.starts_with(layout.cache_root()));
            let rendered = path.to_string_lossy();
            assert!(!rendered.contains("hub.example.test"));
            assert!(!rendered.contains("private-org"));
            assert!(!rendered.contains("private-repo"));
            assert!(!rendered.contains("private-branch"));
        }

        Ok(())
    }

    #[test]
    fn endpoint_kind_and_revision_produce_separate_locations()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("cache-root");
        let id = RepositoryId::parse("org/repo")?;
        let endpoint_a = Endpoint::parse("https://a.example.test")?;
        let endpoint_b = Endpoint::parse("https://b.example.test")?;
        let model = CacheLayout::new(&root, &endpoint_a, &RepositorySpec::model(id.clone()))?;
        let dataset = CacheLayout::new(&root, &endpoint_a, &RepositorySpec::dataset(id.clone()))?;
        let other_origin = CacheLayout::new(&root, &endpoint_b, &RepositorySpec::model(id))?;
        let main = Revision::parse("main")?;
        let pull_request = Revision::parse("refs/pr/1")?;

        assert_ne!(model.repository_directory(), dataset.repository_directory());
        assert_ne!(
            model.repository_directory(),
            other_origin.repository_directory()
        );
        assert_ne!(model.ref_record(&main)?, model.ref_record(&pull_request)?);

        Ok(())
    }

    #[test]
    fn blob_paths_are_sharded_by_validated_sha256_digest() -> Result<(), Box<dyn std::error::Error>>
    {
        let digest = BlobDigest::for_bytes(b"validated payload");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = CacheLayout::new("cache-root", &endpoint, &spec)?;
        let path = layout.blob_path(&digest);
        let hex = digest.to_string();

        assert!(path.ends_with(PathBuf::from("sha256").join(&hex[..2]).join(&hex[2..])));

        Ok(())
    }

    #[test]
    fn nested_layout_keeps_paths_relative_to_the_authorized_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("hub-cache");
        let parent = PathBuf::from("models--org--repo").join(".hf-store");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);

        let layout = CacheLayout::nested(&root, &parent, &endpoint, &spec)?;

        assert_eq!(layout.capability_root(), root);
        assert_eq!(layout.cache_root_relative(), parent.join(CACHE_DIRECTORY));
        assert_eq!(
            layout.cache_root(),
            PathBuf::from("hub-cache")
                .join("models--org--repo/.hf-store")
                .join(CACHE_DIRECTORY)
        );
        Ok(())
    }

    #[test]
    fn nested_layout_rejects_non_normal_capability_paths() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = PathBuf::from("cache-root");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let absolute = std::env::temp_dir().join("absolute-cache-parent");

        for parent in [
            PathBuf::from(".."),
            PathBuf::from("."),
            PathBuf::from("safe/../escape"),
            absolute,
        ] {
            let error = CacheLayout::nested(&root, parent, &endpoint, &spec)
                .expect_err("accepted a non-normal nested cache path");
            assert!(error.is_unsafe_path());
        }
        Ok(())
    }
}
