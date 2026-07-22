use std::path::{Path, PathBuf};

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision};

use super::layout::CacheLayout;
use super::rooted_fs::StagingName;

#[derive(Clone, Debug)]
pub(super) struct HubCacheLayout {
    capability_root: PathBuf,
    repository_relative: PathBuf,
    repository_directory: PathBuf,
    sidecar: CacheLayout,
    endpoint: Endpoint,
    repository: RepositorySpec,
}

impl HubCacheLayout {
    pub(super) fn shared(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        if endpoint != &Endpoint::hugging_face() {
            return Err(ValidationError::new(
                "shared Hub cache endpoint",
                ValidationErrorKind::Malformed,
            ));
        }
        Self::build(root.as_ref(), endpoint, spec)
    }

    pub(super) fn dedicated(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        Self::build(root.as_ref(), endpoint, spec)
    }

    fn build(
        root: &Path,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        let repository_name = spec.id().as_str().replace('/', "--");
        let directory_name = format!("{}--{repository_name}", spec.kind().cache_directory());
        let repository_relative = PathBuf::from(directory_name);
        let repository_directory = root.join(&repository_relative);
        let sidecar =
            CacheLayout::nested(root, repository_relative.join(".hf-store"), endpoint, spec)?;

        Ok(Self {
            capability_root: root.to_path_buf(),
            repository_relative,
            repository_directory,
            sidecar,
            endpoint: endpoint.clone(),
            repository: spec.clone(),
        })
    }

    pub(super) fn capability_root(&self) -> &Path {
        &self.capability_root
    }

    pub(super) fn repository_relative(&self) -> &Path {
        &self.repository_relative
    }

    pub(super) fn repository_directory(&self) -> &Path {
        &self.repository_directory
    }

    pub(super) const fn sidecar(&self) -> &CacheLayout {
        &self.sidecar
    }

    pub(super) const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(super) const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    pub(super) fn cachedir_tag(&self) -> PathBuf {
        self.capability_root.join("CACHEDIR.TAG")
    }

    pub(super) fn ref_path(&self, revision: &Revision) -> Result<PathBuf, ValidationError> {
        let relative = RepoPath::parse(revision.as_str())?;
        Ok(join_repo_path(
            &self.repository_directory.join("refs"),
            &relative,
        ))
    }

    pub(super) fn snapshot_file(&self, commit: &CommitId, path: &RepoPath) -> PathBuf {
        let snapshot = self.snapshot_directory(commit);
        join_repo_path(&snapshot, path)
    }

    pub(super) fn snapshots_directory(&self) -> PathBuf {
        self.repository_directory.join("snapshots")
    }

    pub(super) fn snapshot_directory(&self, commit: &CommitId) -> PathBuf {
        self.snapshots_directory().join(commit.as_str())
    }

    pub(super) fn tree_path(&self, commit: &CommitId) -> PathBuf {
        self.repository_directory
            .join("trees")
            .join(format!("{commit}.json"))
    }

    pub(super) fn blob_path(&self, key: &HubBlobKey) -> PathBuf {
        self.repository_directory.join("blobs").join(key.as_str())
    }

    pub(super) fn blob_lock(&self, key: &HubBlobKey) -> PathBuf {
        self.capability_root
            .join(".locks")
            .join(&self.repository_relative)
            .join(format!("{}.lock", key.as_str()))
    }

    pub(super) fn staging_directory(&self) -> PathBuf {
        self.capability_root
            .join(".locks")
            .join(&self.repository_relative)
            .join(".hf-store-staging")
    }

    pub(super) fn staged_blob(&self, staging: &StagingName) -> PathBuf {
        self.staging_directory().join(format!("{staging}.blob"))
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct HubBlobKey(Box<str>);

impl HubBlobKey {
    pub(super) fn parse(value: impl AsRef<str>) -> Result<Self, ValidationError> {
        let value = value.as_ref();
        let path = RepoPath::parse(value)?;
        if path.as_str().contains('/') {
            return Err(ValidationError::new(
                "Hub cache blob key",
                ValidationErrorKind::UnsafePath,
            ));
        }
        if value
            .get(value.len().saturating_sub(".incomplete".len())..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".incomplete"))
        {
            return Err(ValidationError::new(
                "Hub cache blob key",
                ValidationErrorKind::Collision,
            ));
        }
        Ok(Self(value.into()))
    }

    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
}

fn join_repo_path(base: &Path, path: &RepoPath) -> PathBuf {
    let mut joined = base.to_path_buf();
    for component in path.as_str().split('/') {
        joined.push(component);
    }
    joined
}

#[cfg(test)]
mod tests {
    use crate::RepositoryId;
    use crate::cache::key::SelectionId;

    use super::*;

    #[test]
    fn compatible_repository_directories_match_huggingface_hub()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("hub-cache");
        let endpoint = Endpoint::hugging_face();
        let id = RepositoryId::parse("openai/whisper-large-v3")?;
        let cases = [
            (
                RepositorySpec::model(id.clone()),
                "models--openai--whisper-large-v3",
            ),
            (
                RepositorySpec::dataset(id.clone()),
                "datasets--openai--whisper-large-v3",
            ),
            (
                RepositorySpec::space(id),
                "spaces--openai--whisper-large-v3",
            ),
        ];

        for (spec, directory) in cases {
            let layout = HubCacheLayout::shared(&root, &endpoint, &spec)?;
            assert_eq!(layout.capability_root(), root);
            assert_eq!(layout.repository_relative(), Path::new(directory));
            assert_eq!(layout.repository_directory(), root.join(directory));
            assert_eq!(layout.sidecar().capability_root(), root);
            assert_eq!(
                layout.sidecar().cache_root_relative(),
                PathBuf::from(directory).join(".hf-store/hf-store-v1")
            );
        }

        Ok(())
    }

    #[test]
    fn compatible_writer_paths_match_huggingface_hub_and_hide_unique_staging()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("hub-cache");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared(&root, &endpoint, &spec)?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let blob = HubBlobKey::parse("45f2f2d3b0f6f8f9e61a")?;
        let staging = StagingName::new("operation-17")?;

        assert_eq!(layout.cachedir_tag(), root.join("CACHEDIR.TAG"));
        assert_eq!(
            layout.snapshots_directory(),
            root.join("models--org--repo/snapshots")
        );
        assert_eq!(
            layout.snapshot_directory(&commit),
            root.join("models--org--repo/snapshots/0123456789abcdef0123456789abcdef01234567")
        );
        assert_eq!(
            layout.blob_lock(&blob),
            root.join(".locks/models--org--repo/45f2f2d3b0f6f8f9e61a.lock")
        );
        assert_eq!(
            layout.staging_directory(),
            root.join(".locks/models--org--repo/.hf-store-staging")
        );
        assert_eq!(
            layout.staged_blob(&staging),
            root.join(".locks/models--org--repo/.hf-store-staging/operation-17.blob")
        );

        Ok(())
    }

    #[test]
    fn compatible_refs_and_snapshots_match_upstream_paths() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = PathBuf::from("hub-cache");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared(&root, &endpoint, &spec)?;
        let revision = Revision::parse("refs/pr/17")?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let repo_path = RepoPath::parse("weights/model.safetensors")?;

        assert_eq!(
            layout.ref_path(&revision)?,
            root.join("models--org--repo/refs/refs/pr/17")
        );
        assert_eq!(
            layout.snapshot_file(&commit, &repo_path),
            root.join(
                "models--org--repo/snapshots/0123456789abcdef0123456789abcdef01234567/weights/model.safetensors"
            )
        );
        assert_eq!(
            layout.tree_path(&commit),
            root.join("models--org--repo/trees/0123456789abcdef0123456789abcdef01234567.json")
        );

        Ok(())
    }

    #[test]
    fn compatible_blob_names_are_single_validated_components()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared("hub-cache", &endpoint, &spec)?;
        let key = HubBlobKey::parse("45f2f2d3b0f6f8f9e61a")?;

        assert_eq!(
            layout.blob_path(&key),
            PathBuf::from("hub-cache/models--org--repo/blobs/45f2f2d3b0f6f8f9e61a")
        );

        for value in [
            "",
            ".",
            "..",
            "a/b",
            "a\\b",
            "C:stream",
            "etag.incomplete",
            "ETAG.INCOMPLETE",
            "etag\0secret",
        ] {
            HubBlobKey::parse(value)
                .expect_err(&format!("accepted incompatible blob key {value:?}"));
        }

        Ok(())
    }

    #[test]
    fn shared_root_rejects_custom_endpoints_but_dedicated_root_accepts_them()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::parse("https://hub.example.test")?;
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);

        HubCacheLayout::shared("hub-cache", &endpoint, &spec)
            .expect_err("custom endpoint must not share an ambiguous root");
        let _dedicated = HubCacheLayout::dedicated("hub-cache", &endpoint, &spec)?;

        Ok(())
    }

    #[test]
    fn compatible_layout_reserves_a_versioned_hf_store_sidecar()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("hub-cache");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared(&root, &endpoint, &spec)?;

        assert!(
            layout
                .sidecar()
                .cache_root()
                .starts_with(root.join("models--org--repo").join(".hf-store"))
        );
        assert_ne!(
            layout.sidecar().repository_directory(),
            layout.repository_directory()
        );

        Ok(())
    }

    #[test]
    fn compatible_sidecar_uses_fixed_keys_and_exact_selection_manifests()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("hub-cache");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared(&root, &endpoint, &spec)?;
        let blob_key = HubBlobKey::parse("45f2f2d3b0f6f8f9e61a")?;
        let commit = CommitId::parse("0123456789abcdef0123456789abcdef01234567")?;
        let selection = SelectionId::derive(&[
            RepoPath::parse("config.json")?,
            RepoPath::parse("weights/model.safetensors")?,
        ])?;

        let binding = layout.sidecar().hub_blob_binding_record(&blob_key)?;
        let binding_lock = layout.sidecar().hub_blob_binding_lock(&blob_key)?;
        let manifest = layout.sidecar().snapshot_manifest(&commit, &selection);
        let manifest_lock = layout.sidecar().snapshot_lock(&commit, &selection);
        let sidecar_root = root
            .join("models--org--repo")
            .join(".hf-store")
            .join("hf-store-v1");

        assert!(binding.starts_with(&sidecar_root));
        assert!(binding_lock.starts_with(&sidecar_root));
        assert!(manifest.starts_with(&sidecar_root));
        assert!(manifest_lock.starts_with(&sidecar_root));
        assert!(
            binding.ends_with(
                PathBuf::from("bindings")
                    .join("hub-blobs")
                    .join("be")
                    .join("f25933d4ec07adbf83915e72961d87b86c1008e72985ed927bea3e183723c1.json")
            )
        );
        assert!(
            manifest.ends_with(
                PathBuf::from("snapshots")
                    .join(format!("{commit}-{selection}"))
                    .join("manifest.json")
            )
        );
        assert!(
            binding_lock.ends_with(
                PathBuf::from("locks")
                    .join("bindings")
                    .join("hub-blobs")
                    .join("bef25933d4ec07adbf83915e72961d87b86c1008e72985ed927bea3e183723c1.lock")
            )
        );
        assert!(
            manifest_lock.ends_with(
                PathBuf::from("locks")
                    .join("snapshots")
                    .join(format!("{commit}-{selection}.lock"))
            )
        );

        for path in [binding, binding_lock, manifest, manifest_lock] {
            let rendered = path.to_string_lossy();
            assert!(!rendered.contains(blob_key.as_str()));
            assert!(!rendered.contains("config.json"));
            assert!(!rendered.contains("model.safetensors"));
        }

        Ok(())
    }

    #[test]
    fn compatible_ref_mapping_rejects_host_path_syntax() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubCacheLayout::shared("hub-cache", &endpoint, &spec)?;

        for value in ["../escape", "refs//main", "C:/main", "refs\\main"] {
            let revision = Revision::parse(value)?;
            layout
                .ref_path(&revision)
                .expect_err(&format!("mapped unsafe compatible ref {value:?}"));
        }

        Ok(())
    }
}
