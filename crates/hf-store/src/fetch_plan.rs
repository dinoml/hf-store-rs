use std::collections::BTreeMap;

use crate::cache::{HubTree, HubTreeEntry, RepositoryFilter, RepositorySelection, SelectionId};
use crate::{CommitId, Endpoint, RepoPath, RepositorySpec, Revision, ValidationError};

#[derive(Clone, Debug)]
pub(crate) struct FetchPlan {
    endpoint: Endpoint,
    repository: RepositorySpec,
    requested_revision: Revision,
    commit: CommitId,
    selection: RepositorySelection,
    files: BTreeMap<RepoPath, HubTreeEntry>,
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
                tree.files()
                    .get(path)
                    .cloned()
                    .map(|entry| (path.clone(), entry))
            })
            .collect();
        Ok(Self {
            endpoint,
            repository,
            requested_revision,
            commit,
            selection,
            files,
        })
    }

    pub(crate) const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub(crate) const fn repository(&self) -> &RepositorySpec {
        &self.repository
    }

    pub(crate) const fn requested_revision(&self) -> &Revision {
        &self.requested_revision
    }

    pub(crate) const fn commit(&self) -> &CommitId {
        &self.commit
    }

    pub(crate) const fn selection_id(&self) -> &SelectionId {
        self.selection.selection_id()
    }

    pub(crate) fn files(&self) -> &BTreeMap<RepoPath, HubTreeEntry> {
        &self.files
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
                .keys()
                .map(RepoPath::as_str)
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
