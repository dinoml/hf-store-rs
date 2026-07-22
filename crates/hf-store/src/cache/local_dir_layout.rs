use std::path::{Path, PathBuf};

use crate::validation::{ValidationError, ValidationErrorKind};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec};

use super::layout::CacheLayout;

#[derive(Clone, Debug)]
pub(super) struct HubLocalDirLayout {
    root: PathBuf,
    upstream_bookkeeping: PathBuf,
    completion_sidecar: CacheLayout,
}

impl HubLocalDirLayout {
    pub(super) fn new(
        root: impl AsRef<Path>,
        endpoint: &Endpoint,
        spec: &RepositorySpec,
    ) -> Result<Self, ValidationError> {
        let root = root.as_ref().to_path_buf();
        let upstream_bookkeeping = root.join(".cache").join("huggingface");
        let completion_sidecar =
            CacheLayout::nested(&root, Path::new(".cache/hf-store"), endpoint, spec)?;
        Ok(Self {
            root,
            upstream_bookkeeping,
            completion_sidecar,
        })
    }

    pub(super) fn file_path(&self, path: &RepoPath) -> Result<PathBuf, ValidationError> {
        ensure_not_reserved(path)?;
        Ok(join_repo_path(&self.root, path))
    }

    pub(super) fn download_metadata_path(
        &self,
        path: &RepoPath,
    ) -> Result<PathBuf, ValidationError> {
        ensure_not_reserved(path)?;
        let relative = format!("{}.metadata", path.as_str());
        let relative = RepoPath::parse(relative)?;
        Ok(join_repo_path(
            &self.upstream_bookkeeping.join("download"),
            &relative,
        ))
    }

    pub(super) fn lock_path(&self, path: &RepoPath) -> Result<PathBuf, ValidationError> {
        let mut lock = self.download_metadata_path(path)?;
        let _replaced = lock.set_extension("lock");
        Ok(lock)
    }

    pub(super) fn tree_path(&self, commit: &CommitId) -> PathBuf {
        self.upstream_bookkeeping
            .join("trees")
            .join(format!("{commit}.json"))
    }

    pub(super) fn gitignore_path(&self) -> PathBuf {
        self.upstream_bookkeeping.join(".gitignore")
    }

    pub(super) fn cachedir_tag_path(&self) -> PathBuf {
        self.upstream_bookkeeping.join("CACHEDIR.TAG")
    }

    pub(super) const fn completion_sidecar(&self) -> &CacheLayout {
        &self.completion_sidecar
    }

    pub(super) fn coordination_lock_path(&self) -> PathBuf {
        self.completion_sidecar
            .cache_root()
            .join("locks")
            .join("local-dir.lock")
    }

    pub(super) fn coordination_state_path(&self) -> PathBuf {
        self.completion_sidecar
            .cache_root()
            .join("local-dir-state.json")
    }

    pub(super) fn coordination_lock_relative(&self) -> Result<PathBuf, ValidationError> {
        self.capability_relative(&self.coordination_lock_path())
            .map(Path::to_path_buf)
    }

    pub(super) fn coordination_state_relative(&self) -> Result<PathBuf, ValidationError> {
        self.capability_relative(&self.coordination_state_path())
            .map(Path::to_path_buf)
    }

    pub(super) fn capability_relative<'a>(
        &self,
        path: &'a Path,
    ) -> Result<&'a Path, ValidationError> {
        path.strip_prefix(&self.root).map_err(|_outside| {
            ValidationError::new(
                "local directory capability path",
                ValidationErrorKind::UnsafePath,
            )
        })
    }
}

fn ensure_not_reserved(path: &RepoPath) -> Result<(), ValidationError> {
    if path
        .as_str()
        .split('/')
        .any(looks_like_dos_short_name_alias)
    {
        return Err(ValidationError::new(
            "local directory repository path",
            ValidationErrorKind::UnsafePath,
        ));
    }

    let mut components = path.as_str().split('/');
    let first = components.next();
    let second = components.next();
    let is_cache = first.is_some_and(|value| value.eq_ignore_ascii_case(".cache"));
    let is_reserved = is_cache
        && second.is_none_or(|value| {
            value.eq_ignore_ascii_case("huggingface") || value.eq_ignore_ascii_case("hf-store")
        });
    if is_reserved {
        Err(ValidationError::new(
            "local directory repository path",
            ValidationErrorKind::Collision,
        ))
    } else {
        Ok(())
    }
}

fn looks_like_dos_short_name_alias(component: &str) -> bool {
    // Windows can resolve names such as `HUGGIN~1` to a different long-name
    // entry. Reject the alias shape on every platform so one repository path
    // cannot target ordinary content on Unix and reserved bookkeeping on
    // Windows.
    let stem = component
        .split_once('.')
        .map_or(component, |(stem, _extension)| stem);
    stem.rsplit_once('~').is_some_and(|(prefix, ordinal)| {
        !prefix.is_empty()
            && !ordinal.is_empty()
            && ordinal.bytes().all(|byte| byte.is_ascii_digit())
    })
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

    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn local_dir_paths_match_huggingface_hub_bookkeeping() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = PathBuf::from("local-dir");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new(&root, &endpoint, &spec)?;
        let path = RepoPath::parse("nested/model.tar.gz")?;
        let commit = CommitId::parse(COMMIT)?;

        assert_eq!(layout.file_path(&path)?, root.join("nested/model.tar.gz"));
        assert_eq!(
            layout.download_metadata_path(&path)?,
            root.join(".cache/huggingface/download/nested/model.tar.gz.metadata")
        );
        assert_eq!(
            layout.lock_path(&path)?,
            root.join(".cache/huggingface/download/nested/model.tar.gz.lock")
        );
        assert_eq!(
            layout.tree_path(&commit),
            root.join(format!(".cache/huggingface/trees/{COMMIT}.json"))
        );

        Ok(())
    }

    #[test]
    fn local_dir_reserves_upstream_and_hf_store_metadata_namespaces()
    -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new("local-dir", &endpoint, &spec)?;

        for value in [
            ".cache",
            ".cache/huggingface/download/file.metadata",
            ".cache/hf-store/hf-store-v1/manifest.json",
        ] {
            let path = RepoPath::parse(value)?;
            layout
                .file_path(&path)
                .expect_err(&format!("mapped reserved local-dir path {value:?}"));
        }

        Ok(())
    }

    #[test]
    fn local_dir_rejects_dos_short_name_alias_components() -> Result<(), Box<dyn std::error::Error>>
    {
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse("org/repo")?);
        let layout = HubLocalDirLayout::new("local-dir", &endpoint, &spec)?;

        for value in [
            "CACHE~1/huggingface/config.json",
            ".cache/HUGGIN~1/download/config.json",
            "weights/MODEL~1.BIN",
        ] {
            let path = RepoPath::parse(value)?;
            let error = layout
                .file_path(&path)
                .expect_err("mapped a DOS short-name alias-shaped path");
            assert!(error.is_unsafe_path());
        }

        let ordinary_tilde = RepoPath::parse("weights/model~draft.bin")?;
        assert_eq!(
            layout.file_path(&ordinary_tilde)?,
            PathBuf::from("local-dir/weights/model~draft.bin")
        );

        Ok(())
    }

    #[test]
    fn local_dir_completion_metadata_is_separate_from_upstream_hints()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("local-dir");
        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::dataset(RepositoryId::parse("org/data")?);
        let layout = HubLocalDirLayout::new(&root, &endpoint, &spec)?;

        assert_eq!(layout.completion_sidecar().capability_root(), root);
        assert_eq!(
            layout.completion_sidecar().cache_root_relative(),
            PathBuf::from(".cache/hf-store/hf-store-v1")
        );
        assert!(
            layout
                .completion_sidecar()
                .cache_root()
                .starts_with(root.join(".cache/hf-store"))
        );
        assert!(
            !layout
                .completion_sidecar()
                .cache_root()
                .starts_with(root.join(".cache/huggingface"))
        );

        Ok(())
    }

    #[test]
    fn local_dir_coordination_paths_are_global_to_the_physical_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = PathBuf::from("local-dir");
        let first = HubLocalDirLayout::new(
            &root,
            &Endpoint::hugging_face(),
            &RepositorySpec::model(RepositoryId::parse("org/model")?),
        )?;
        let second = HubLocalDirLayout::new(
            &root,
            &Endpoint::parse("https://hub.example.test/prefix")?,
            &RepositorySpec::dataset(RepositoryId::parse("other/data")?),
        )?;

        let expected_lock = root.join(".cache/hf-store/hf-store-v1/locks/local-dir.lock");
        let expected_state = root.join(".cache/hf-store/hf-store-v1/local-dir-state.json");
        assert_eq!(first.coordination_lock_path(), expected_lock);
        assert_eq!(second.coordination_lock_path(), expected_lock);
        assert_eq!(first.coordination_state_path(), expected_state);
        assert_eq!(second.coordination_state_path(), expected_state);
        assert_eq!(
            first.coordination_lock_relative()?,
            Path::new(".cache/hf-store/hf-store-v1/locks/local-dir.lock")
        );
        assert_eq!(
            first.coordination_state_relative()?,
            Path::new(".cache/hf-store/hf-store-v1/local-dir-state.json")
        );

        // The fixed active-state location serializes every repository using this
        // physical directory. Repository identity belongs in the future state
        // payload, whose repository-scoped location remains distinct here.
        assert_ne!(
            first.completion_sidecar().repository_directory(),
            second.completion_sidecar().repository_directory()
        );

        Ok(())
    }
}
