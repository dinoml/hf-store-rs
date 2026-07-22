use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cache::{BlobDigest, GcObservation, PartialGcCandidate};
use crate::{CacheMode, CommitId, Endpoint, HubError, RepoPath, RepositorySpec};

const PLAN_SCHEMA: &str = "hf-store.gc.plan";
const PLAN_VERSION: u32 = 1;

/// Explicit retention policy for a garbage-collection plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcPolicy {
    partial_minimum_age_millis: Option<u64>,
    snapshot_minimum_age_millis: Option<u64>,
    snapshot_keep_floor: usize,
    retained_commits: Box<[Box<str>]>,
}

impl GcPolicy {
    /// Creates a policy that may collect identity-valid abandoned partial transfers.
    ///
    /// Returns `None` when the duration cannot be represented in milliseconds.
    #[must_use]
    pub fn expired_partials(minimum_age: Duration) -> Option<Self> {
        Self::report_only().with_expired_partials(minimum_age)
    }

    /// Creates a non-destructive policy that reports reachability without candidates.
    #[must_use]
    pub fn report_only() -> Self {
        Self {
            partial_minimum_age_millis: None,
            snapshot_minimum_age_millis: None,
            snapshot_keep_floor: 0,
            retained_commits: Box::new([]),
        }
    }

    /// Enables collection of identity-valid abandoned partial transfers.
    ///
    /// Returns `None` when the duration cannot be represented in milliseconds.
    #[must_use]
    pub fn with_expired_partials(mut self, minimum_age: Duration) -> Option<Self> {
        self.partial_minimum_age_millis = Some(minimum_age.as_millis().try_into().ok()?);
        Some(self)
    }

    /// Enables collection of unreferenced immutable snapshots after a grace period.
    ///
    /// The newest `keep_floor` snapshots remain retained even when detached. Returns
    /// `None` when the duration cannot be represented in milliseconds.
    #[must_use]
    pub fn with_unreferenced_snapshots(
        mut self,
        minimum_age: Duration,
        keep_floor: usize,
    ) -> Option<Self> {
        self.snapshot_minimum_age_millis = Some(minimum_age.as_millis().try_into().ok()?);
        self.snapshot_keep_floor = keep_floor;
        Some(self)
    }

    /// Adds an immutable commit root retained independently of mutable refs.
    #[must_use]
    pub fn retain_commit(mut self, commit: &CommitId) -> Self {
        let mut retained = self.retained_commits.into_vec();
        retained.push(commit.as_str().into());
        retained.sort_unstable();
        retained.dedup();
        self.retained_commits = retained.into_boxed_slice();
        self
    }

    /// Returns the configured grace period for partial transfers.
    #[must_use]
    pub const fn partial_minimum_age_millis(&self) -> Option<u64> {
        self.partial_minimum_age_millis
    }

    /// Returns the configured grace period for detached snapshots.
    #[must_use]
    pub const fn snapshot_minimum_age_millis(&self) -> Option<u64> {
        self.snapshot_minimum_age_millis
    }

    /// Returns the per-repository detached-snapshot keep floor.
    #[must_use]
    pub const fn snapshot_keep_floor(&self) -> usize {
        self.snapshot_keep_floor
    }

    /// Returns explicit immutable commit roots in canonical order.
    #[must_use]
    pub fn retained_commits(&self) -> &[Box<str>] {
        &self.retained_commits
    }
}

/// Kind of object scheduled by a version-one GC plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum GcCandidateKind {
    /// An identity-valid partial record and payload pair.
    PartialTransfer,
    /// A complete detached immutable snapshot deletion unit.
    Snapshot,
    /// A validated blob unreachable after the planned snapshot removals.
    Blob,
}

/// Stable reason why an object is eligible in a version-one plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum GcCandidateReason {
    /// A resumable partial exceeded its explicit grace period.
    ExpiredPartial,
    /// An immutable snapshot is detached and exceeded its retention grace.
    DetachedSnapshot,
    /// A validated owned blob has no edge from a retained snapshot.
    UnreachableBlob,
}

/// Stable blocker preventing a stronger garbage-collection action.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum GcBlockerKind {
    /// Python-visible compatible-cache state requires an external quiescence guarantee.
    CompatibleMaintenanceRequired,
}

/// One safe, machine-readable blocker in an immutable plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcBlocker {
    kind: GcBlockerKind,
    subject: Box<str>,
}

impl GcBlocker {
    fn compatible_maintenance() -> Self {
        Self {
            kind: GcBlockerKind::CompatibleMaintenanceRequired,
            subject: "python-visible-cache-state".into(),
        }
    }

    /// Returns the stable blocker classification.
    #[must_use]
    pub const fn kind(&self) -> GcBlockerKind {
        self.kind
    }

    /// Returns the safe logical scope affected by the blocker.
    #[must_use]
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// One immutable candidate observation in a [`GcPlan`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcCandidate {
    id: Box<str>,
    kind: GcCandidateKind,
    reason: GcCandidateReason,
    logical_bytes: u64,
    updated_unix_millis: u64,
    commit: Option<Box<str>>,
    selection_id: Option<Box<str>>,
    repository_path: Option<Box<str>>,
    fingerprint_sha256: Box<str>,
}

impl GcCandidate {
    /// Returns the fixed-size cache object identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the candidate kind.
    #[must_use]
    pub const fn kind(&self) -> GcCandidateKind {
        self.kind
    }

    /// Returns the stable eligibility reason.
    #[must_use]
    pub const fn reason(&self) -> GcCandidateReason {
        self.reason
    }

    /// Returns the estimated logical bytes.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    /// Returns the observation time stored in the candidate fingerprint.
    #[must_use]
    pub const fn updated_unix_millis(&self) -> u64 {
        self.updated_unix_millis
    }

    /// Returns the immutable commit for a partial or snapshot candidate.
    #[must_use]
    pub fn commit(&self) -> Option<&str> {
        self.commit.as_deref()
    }

    /// Returns the selection identity for a snapshot candidate.
    #[must_use]
    pub fn selection_id(&self) -> Option<&str> {
        self.selection_id.as_deref()
    }

    pub(crate) fn observation(&self) -> Result<GcObservation, HubError> {
        if !is_sha256(&self.id) || !is_sha256(&self.fingerprint_sha256) {
            return Err(HubError::protocol());
        }
        let fingerprint =
            BlobDigest::parse(&self.fingerprint_sha256).map_err(HubError::validation)?;
        match self.kind {
            GcCandidateKind::PartialTransfer => {
                if self.reason != GcCandidateReason::ExpiredPartial {
                    return Err(HubError::protocol());
                }
                let commit = parse_commit(self.commit.as_deref())?;
                let path = self
                    .repository_path
                    .as_deref()
                    .ok_or_else(HubError::protocol)
                    .and_then(|value| RepoPath::parse(value).map_err(HubError::validation))?;
                if self.selection_id.is_some() {
                    return Err(HubError::protocol());
                }
                Ok(GcObservation::Partial(
                    PartialGcCandidate::from_observation(
                        self.id.clone(),
                        commit,
                        path,
                        fingerprint,
                        self.logical_bytes,
                        self.updated_unix_millis,
                    ),
                ))
            }
            GcCandidateKind::Snapshot => {
                if self.reason != GcCandidateReason::DetachedSnapshot {
                    return Err(HubError::protocol());
                }
                if self.repository_path.is_some() {
                    return Err(HubError::protocol());
                }
                GcObservation::snapshot_from_plan(
                    self.id.clone(),
                    self.commit.as_deref().ok_or_else(HubError::protocol)?,
                    self.selection_id
                        .as_deref()
                        .ok_or_else(HubError::protocol)?,
                    fingerprint,
                    self.logical_bytes,
                    self.updated_unix_millis,
                )
            }
            GcCandidateKind::Blob => {
                if self.reason != GcCandidateReason::UnreachableBlob {
                    return Err(HubError::protocol());
                }
                if self.commit.is_some()
                    || self.selection_id.is_some()
                    || self.repository_path.is_some()
                {
                    return Err(HubError::protocol());
                }
                GcObservation::blob_from_plan(
                    self.id.clone(),
                    fingerprint,
                    self.logical_bytes,
                    self.updated_unix_millis,
                )
            }
        }
    }
}

impl From<&GcObservation> for GcCandidate {
    fn from(candidate: &GcObservation) -> Self {
        Self {
            id: candidate.key().into(),
            kind: match candidate {
                GcObservation::Partial(_) => GcCandidateKind::PartialTransfer,
                GcObservation::Snapshot(_) => GcCandidateKind::Snapshot,
                GcObservation::Blob(_) => GcCandidateKind::Blob,
            },
            reason: match candidate {
                GcObservation::Partial(_) => GcCandidateReason::ExpiredPartial,
                GcObservation::Snapshot(_) => GcCandidateReason::DetachedSnapshot,
                GcObservation::Blob(_) => GcCandidateReason::UnreachableBlob,
            },
            logical_bytes: candidate.size(),
            updated_unix_millis: candidate.updated_unix_millis(),
            commit: candidate.commit().map(|value| value.as_str().into()),
            selection_id: candidate.selection_id().map(Into::into),
            repository_path: candidate.path().map(|value| value.as_str().into()),
            fingerprint_sha256: candidate.fingerprint().to_string().into(),
        }
    }
}

/// Immutable, deterministic dry-run garbage-collection plan.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GcPlan {
    schema: Box<str>,
    version: u32,
    plan_id: Box<str>,
    cache_mode: CacheMode,
    endpoint: Box<str>,
    repository: Box<str>,
    planned_unix_millis: u64,
    policy: GcPolicy,
    compatible_deletion_blocked: bool,
    blockers: Box<[GcBlocker]>,
    candidates: Box<[GcCandidate]>,
}

impl GcPlan {
    pub(crate) fn new(
        cache_mode: CacheMode,
        endpoint: &Endpoint,
        repository: &RepositorySpec,
        planned_unix_millis: u64,
        policy: GcPolicy,
        internal: &[GcObservation],
    ) -> Result<Self, HubError> {
        let mut candidates = internal.iter().map(GcCandidate::from).collect::<Vec<_>>();
        candidates.sort_unstable_by(|left, right| candidate_key(left).cmp(&candidate_key(right)));
        let repository_identity =
            format!("{}:{}", repository.kind(), repository.id()).into_boxed_str();
        let mut plan = Self {
            schema: PLAN_SCHEMA.into(),
            version: PLAN_VERSION,
            plan_id: Box::from(""),
            cache_mode,
            endpoint: endpoint.as_str().into(),
            repository: repository_identity,
            planned_unix_millis,
            policy,
            compatible_deletion_blocked: cache_mode == CacheMode::Compatible,
            blockers: if cache_mode == CacheMode::Compatible {
                vec![GcBlocker::compatible_maintenance()].into_boxed_slice()
            } else {
                Box::new([])
            },
            candidates: candidates.into_boxed_slice(),
        };
        plan.plan_id = plan.computed_id()?;
        plan.validate()?;
        Ok(plan)
    }

    /// Decodes and validates a version-one executable plan.
    ///
    /// Unknown JSON fields are ignored for forward-compatible additions. The
    /// schema, version, identities, canonical ordering, and plan digest must
    /// all validate before execution can use the value.
    ///
    /// # Errors
    ///
    /// Returns a protocol error for malformed, unsupported, or altered plans.
    pub fn from_json(bytes: &[u8]) -> Result<Self, HubError> {
        let plan: Self = serde_json::from_slice(bytes).map_err(|_error| HubError::protocol())?;
        plan.validate()?;
        Ok(plan)
    }

    /// Encodes the stable version-one executable plan schema.
    ///
    /// # Errors
    ///
    /// Returns a protocol error if serialization unexpectedly fails.
    pub fn to_json(&self) -> Result<Vec<u8>, HubError> {
        serde_json::to_vec_pretty(self).map_err(|_error| HubError::protocol())
    }

    /// Returns the report schema name.
    #[must_use]
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Returns the report schema version.
    #[must_use]
    pub const fn version(&self) -> u32 {
        self.version
    }

    /// Returns the deterministic plan identity.
    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    /// Returns candidates in stable deletion-class and object-identity order.
    #[must_use]
    pub fn candidates(&self) -> &[GcCandidate] {
        &self.candidates
    }

    /// Returns whether Python-visible compatible-cache deletion is blocked.
    #[must_use]
    pub const fn compatible_deletion_blocked(&self) -> bool {
        self.compatible_deletion_blocked
    }

    /// Returns deterministic blockers discovered while building the plan.
    #[must_use]
    pub fn blockers(&self) -> &[GcBlocker] {
        &self.blockers
    }

    /// Returns the cache view to which the plan is bound.
    #[must_use]
    pub const fn cache_mode(&self) -> CacheMode {
        self.cache_mode
    }

    /// Returns the complete explicit retention policy.
    #[must_use]
    pub const fn policy(&self) -> &GcPolicy {
        &self.policy
    }

    /// Returns the plan-time wall-clock instant in Unix milliseconds.
    #[must_use]
    pub const fn planned_unix_millis(&self) -> u64 {
        self.planned_unix_millis
    }

    fn validate(&self) -> Result<(), HubError> {
        if self.schema.as_ref() != PLAN_SCHEMA
            || self.version != PLAN_VERSION
            || !is_sha256(&self.plan_id)
            || self.compatible_deletion_blocked != (self.cache_mode == CacheMode::Compatible)
            || !valid_blockers(self.cache_mode, &self.blockers)
            || Endpoint::parse(&self.endpoint)
                .map_err(HubError::validation)?
                .as_str()
                != self.endpoint.as_ref()
            || self.computed_id()?.as_ref() != self.plan_id.as_ref()
        {
            return Err(HubError::protocol());
        }
        validate_policy(&self.policy)?;
        let mut previous = None;
        for candidate in &self.candidates {
            let identity = candidate.observation()?;
            if identity.key() != candidate.id()
                || previous.is_some_and(|value| value >= candidate_key(candidate))
            {
                return Err(HubError::protocol());
            }
            previous = Some(candidate_key(candidate));
        }
        Ok(())
    }

    fn computed_id(&self) -> Result<Box<str>, HubError> {
        let identity_bytes = serde_json::to_vec(&(
            self.schema.as_ref(),
            self.version,
            self.cache_mode,
            self.endpoint.as_ref(),
            self.repository.as_ref(),
            self.planned_unix_millis,
            &self.policy,
            self.compatible_deletion_blocked,
            &self.blockers,
            &self.candidates,
        ))
        .map_err(|_error| HubError::protocol())?;
        Ok(format!("{:x}", Sha256::digest(identity_bytes)).into())
    }

    pub(crate) fn endpoint_matches(&self, endpoint: &Endpoint) -> bool {
        self.endpoint.as_ref() == endpoint.as_str()
    }

    pub(crate) fn repository_matches(&self, repository: &RepositorySpec) -> bool {
        self.repository.as_ref() == format!("{}:{}", repository.kind(), repository.id())
    }
}

/// Result of executing only the candidates in one immutable plan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GcExecutionReport {
    schema: &'static str,
    version: u32,
    plan_id: Box<str>,
    removed: Box<[Box<str>]>,
    skipped: Box<[Box<str>]>,
    logical_bytes_removed: u64,
}

impl GcExecutionReport {
    pub(crate) fn new(
        plan: &GcPlan,
        removed: Vec<Box<str>>,
        skipped: Vec<Box<str>>,
        bytes: u64,
    ) -> Self {
        Self {
            schema: "hf-store.gc.execution",
            version: 1,
            plan_id: plan.plan_id.clone(),
            removed: removed.into_boxed_slice(),
            skipped: skipped.into_boxed_slice(),
            logical_bytes_removed: bytes,
        }
    }

    /// Returns the executable plan identity.
    #[must_use]
    pub fn plan_id(&self) -> &str {
        &self.plan_id
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

    /// Returns removed candidate identities.
    #[must_use]
    pub fn removed(&self) -> &[Box<str>] {
        &self.removed
    }

    /// Returns candidates skipped after fresh revalidation.
    #[must_use]
    pub fn skipped(&self) -> &[Box<str>] {
        &self.skipped
    }

    /// Returns the estimated logical bytes removed.
    #[must_use]
    pub const fn logical_bytes_removed(&self) -> u64 {
        self.logical_bytes_removed
    }
}

fn parse_commit(value: Option<&str>) -> Result<CommitId, HubError> {
    value
        .ok_or_else(HubError::protocol)
        .and_then(|value| CommitId::parse(value).map_err(HubError::validation))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn candidate_key(candidate: &GcCandidate) -> (GcCandidateKind, &str) {
    (candidate.kind, candidate.id())
}

fn valid_blockers(cache_mode: CacheMode, blockers: &[GcBlocker]) -> bool {
    match cache_mode {
        CacheMode::Owned => blockers.is_empty(),
        CacheMode::Compatible => {
            blockers
                == [GcBlocker {
                    kind: GcBlockerKind::CompatibleMaintenanceRequired,
                    subject: "python-visible-cache-state".into(),
                }]
        }
    }
}

fn validate_policy(policy: &GcPolicy) -> Result<(), HubError> {
    let mut previous = None;
    for commit in &policy.retained_commits {
        let parsed = CommitId::parse(commit).map_err(HubError::validation)?;
        if parsed.as_str() != commit.as_ref()
            || previous.is_some_and(|value: &str| value >= commit.as_ref())
        {
            return Err(HubError::protocol());
        }
        previous = Some(commit.as_ref());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepositoryId;

    #[test]
    fn executable_plan_round_trip_rejects_tampering() -> Result<(), Box<dyn std::error::Error>> {
        let repository = RepositorySpec::model(RepositoryId::parse("org/model")?);
        let candidate = PartialGcCandidate::from_observation(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".into(),
            CommitId::parse("0123456789abcdef0123456789abcdef01234567")?,
            RepoPath::parse("model.safetensors")?,
            BlobDigest::for_bytes(b"record"),
            42,
            100,
        );
        let plan = GcPlan::new(
            CacheMode::Owned,
            &Endpoint::hugging_face(),
            &repository,
            200,
            GcPolicy::expired_partials(Duration::from_millis(50)).ok_or("duration did not fit")?,
            &[GcObservation::Partial(candidate)],
        )?;
        let encoded = plan.to_json()?;
        let decoded = GcPlan::from_json(&encoded)?;
        assert_eq!(decoded.plan_id(), plan.plan_id());
        let mut altered: serde_json::Value = serde_json::from_slice(&encoded)?;
        altered["candidates"][0]["logical_bytes"] = 43.into();
        let _error = GcPlan::from_json(&serde_json::to_vec(&altered)?)
            .expect_err("altered plan must fail its identity check");

        let compatible = GcPlan::new(
            CacheMode::Compatible,
            &Endpoint::hugging_face(),
            &repository,
            200,
            GcPolicy::report_only(),
            &[],
        )?;
        assert!(compatible.compatible_deletion_blocked());
        assert_eq!(compatible.blockers().len(), 1);
        assert_eq!(
            compatible.blockers()[0].kind(),
            GcBlockerKind::CompatibleMaintenanceRequired
        );
        assert_eq!(
            compatible.blockers()[0].subject(),
            "python-visible-cache-state"
        );
        Ok(())
    }
}
