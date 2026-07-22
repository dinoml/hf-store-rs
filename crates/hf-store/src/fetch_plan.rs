use crate::cache::{HubTree, HubTreeEntry, RepositoryFilter, RepositorySelection, SelectionId};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision, ValidationError};

#[derive(Clone, Debug)]
/// Deterministic, immutable plan for acquiring selected files from one commit.
pub struct FetchPlan {
    endpoint: Endpoint,
    repository: RepositorySpec,
    requested_revision: Revision,
    commit: CommitId,
    selection: RepositorySelection,
    files: Box<[PlannedFile]>,
}

/// One validated Hub file selected by a [`FetchPlan`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedFile {
    path: RepoPath,
    entry: HubTreeEntry,
}

impl FetchPlan {
    pub(crate) fn build(
        endpoint: Endpoint,
        repository: RepositorySpec,
        requested_revision: Revision,
        commit: CommitId,
        tree: &HubTree,
        filter: &RepositoryFilter,
    ) -> Result<Self, ValidationError> {
        let selection = filter.select(tree.files().keys().cloned())?;
        let files = selection
            .paths()
            .iter()
            .filter_map(|path| {
                tree.files().get(path).cloned().map(|entry| PlannedFile {
                    path: path.clone(),
                    entry,
                })
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(Self {
            endpoint,
            repository,
            requested_revision,
            commit,
            selection,
            files,
        })
    }

    /// Returns the endpoint whose metadata produced this plan.
    #[must_use]
    pub const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Returns the repository identity.
    #[must_use]
    pub const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    /// Returns the originally requested revision.
    #[must_use]
    pub const fn requested_revision(&self) -> &Revision {
        &self.requested_revision
    }

    /// Returns the resolved immutable commit.
    #[must_use]
    pub const fn commit(&self) -> &CommitId {
        &self.commit
    }

    /// Returns the identity derived only from the sorted selected path set.
    #[must_use]
    pub const fn selection_id(&self) -> &SelectionId {
        self.selection.selection_id()
    }

    /// Returns selected files in canonical repository-path order.
    #[must_use]
    pub fn files(&self) -> &[PlannedFile] {
        &self.files
    }
}

impl PlannedFile {
    /// Returns the portable repository-relative path.
    #[must_use]
    pub const fn path(&self) -> &RepoPath {
        &self.path
    }

    /// Returns the expected file size in bytes.
    #[must_use]
    pub const fn size(&self) -> u64 {
        self.entry.size()
    }

    /// Returns the Hub Git object identifier or opaque validator.
    #[must_use]
    pub fn blob_id(&self) -> &str {
        self.entry.blob_id()
    }

    /// Returns a proven LFS SHA-256 identity when present.
    #[must_use]
    pub fn lfs_sha256(&self) -> Option<&str> {
        self.entry.lfs_sha256()
    }

    /// Returns a Hub Xet identity when present.
    #[must_use]
    pub fn xet_hash(&self) -> Option<&str> {
        self.entry.xet_hash()
    }

    pub(crate) const fn entry(&self) -> &HubTreeEntry {
        &self.entry
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use crate::{RepositoryId, RepositoryKind};

    use super::*;

    const COMMIT: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn plans_are_sorted_commit_bound_and_filter_only_the_selected_paths()
    -> Result<(), Box<dyn Error>> {
        let first = RepoPath::parse("config.json")?;
        let second = RepoPath::parse("weights/model.bin")?;
        let tree = HubTree::new([
            (second.clone(), HubTreeEntry::new(5, "model-id")?),
            (first.clone(), HubTreeEntry::new(2, "config-id")?),
        ])?;
        let plan = FetchPlan::build(
            Endpoint::parse("https://hub.example")?,
            RepositorySpec::new(RepositoryKind::Model, RepositoryId::parse("owner/repo")?),
            Revision::parse("main")?,
            CommitId::parse(COMMIT)?,
            &tree,
            &RepositoryFilter::new(Some(&["*.json"]), &[]),
        )?;

        assert_eq!(plan.endpoint().as_str(), "https://hub.example");
        assert_eq!(plan.repository().id().as_str(), "owner/repo");
        assert_eq!(plan.requested_revision().as_str(), "main");
        assert_eq!(plan.commit().as_str(), COMMIT);
        assert_eq!(
            plan.files()
                .iter()
                .map(|file| file.path().as_str())
                .collect::<Vec<_>>(),
            ["config.json"]
        );
        assert_eq!(
            plan.selection_id(),
            &SelectionId::derive(std::slice::from_ref(&first))?
        );
        Ok(())
    }

    #[test]
    fn selection_identity_depends_only_on_the_sorted_selected_path_set()
    -> Result<(), Box<dyn Error>> {
        let path = RepoPath::parse("model.bin")?;
        let left = HubTree::new([(path.clone(), HubTreeEntry::new(1, "left")?)])?;
        let right = HubTree::new([(path, HubTreeEntry::new(999, "right")?)])?;
        let filter = RepositoryFilter::new(None, &[]);
        let left = plan(&left, &filter)?;
        let right = plan(&right, &filter)?;
        assert_eq!(left.selection_id(), right.selection_id());
        assert_ne!(left.files(), right.files());
        Ok(())
    }

    fn plan(tree: &HubTree, filter: &RepositoryFilter) -> Result<FetchPlan, Box<dyn Error>> {
        Ok(FetchPlan::build(
            Endpoint::hugging_face(),
            RepositorySpec::model(RepositoryId::parse("owner/repo")?),
            Revision::parse("refs/pr/7")?,
            CommitId::parse(COMMIT)?,
            tree,
            filter,
        )?)
    }
}
