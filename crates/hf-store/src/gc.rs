use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cache::{BlobDigest, PartialGcCandidate};
use crate::{CacheMode, CommitId, Endpoint, HubError, RepoPath, RepositorySpec};

const PLAN_SCHEMA: &str = "hf-store.gc.plan";
const PLAN_VERSION: u32 = 1;

/// Explicit retention policy for a garbage-collection plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcPolicy {
    partial_minimum_age_millis: u64,
}

impl GcPolicy {
    /// Creates a policy that may collect identity-valid abandoned partial transfers.
    ///
    /// Returns `None` when the duration cannot be represented in milliseconds.
    #[must_use]
    pub fn expired_partials(minimum_age: Duration) -> Option<Self> {
        Some(Self {
            partial_minimum_age_millis: minimum_age.as_millis().try_into().ok()?,
        })
    }

    /// Returns the minimum age for partial-transfer candidates.
    #[must_use]
    pub const fn partial_minimum_age_millis(self) -> u64 {
        self.partial_minimum_age_millis
    }
}

/// Kind of object scheduled by a version-one GC plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
#[non_exhaustive]
pub enum GcCandidateKind {
    /// An identity-valid partial record and payload pair.
    PartialTransfer,
}

/// One immutable candidate observation in a [`GcPlan`].
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GcCandidate {
    id: Box<str>,
    kind: GcCandidateKind,
    logical_bytes: u64,
    updated_unix_millis: u64,
    commit: Box<str>,
    repository_path: Box<str>,
    record_sha256: Box<str>,
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

    pub(crate) fn partial_identity(&self) -> Result<PartialGcCandidate, HubError> {
        if self.kind != GcCandidateKind::PartialTransfer
            || self.id.len() != 64
            || !self
                .id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(HubError::protocol());
        }
        let commit = CommitId::parse(&self.commit).map_err(HubError::validation)?;
        let path = RepoPath::parse(&self.repository_path).map_err(HubError::validation)?;
        let record_digest = BlobDigest::parse(&self.record_sha256).map_err(HubError::validation)?;
        Ok(PartialGcCandidate::from_observation(
            self.id.clone(),
            commit,
            path,
            record_digest,
            self.logical_bytes,
            self.updated_unix_millis,
        ))
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
    candidates: Box<[GcCandidate]>,
}

impl GcPlan {
    pub(crate) fn new(
        cache_mode: CacheMode,
        endpoint: &Endpoint,
        repository: &RepositorySpec,
        planned_unix_millis: u64,
        policy: GcPolicy,
        internal: &[PartialGcCandidate],
    ) -> Result<Self, HubError> {
        let candidates = internal
            .iter()
            .map(|candidate| GcCandidate {
                id: candidate.key().into(),
                kind: GcCandidateKind::PartialTransfer,
                logical_bytes: candidate.size(),
                updated_unix_millis: candidate.updated_unix_millis(),
                commit: candidate.commit().as_str().into(),
                repository_path: candidate.path().as_str().into(),
                record_sha256: candidate.record_digest().to_string().into(),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
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
            candidates,
        };
        plan.plan_id = plan.computed_id()?;
        plan.validate()?;
        Ok(plan)
    }

    /// Decodes and validates a version-one executable plan.
    ///
    /// Unknown JSON fields are ignored for forward-compatible additions. The
    /// schema, version, fixed-size identities, canonical ordering, and plan
    /// digest must all validate before execution can use the value.
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

    /// Returns candidates in stable object-identity order.
    #[must_use]
    pub fn candidates(&self) -> &[GcCandidate] {
        &self.candidates
    }

    /// Returns whether compatible-cache deletion is conservatively blocked.
    #[must_use]
    pub const fn compatible_deletion_blocked(&self) -> bool {
        self.compatible_deletion_blocked
    }

    /// Returns the cache view to which the plan is bound.
    #[must_use]
    pub const fn cache_mode(&self) -> CacheMode {
        self.cache_mode
    }

    /// Returns the complete explicit retention policy.
    #[must_use]
    pub const fn policy(&self) -> GcPolicy {
        self.policy
    }

    /// Returns the plan-time wall-clock instant in Unix milliseconds.
    #[must_use]
    pub const fn planned_unix_millis(&self) -> u64 {
        self.planned_unix_millis
    }

    fn validate(&self) -> Result<(), HubError> {
        if self.schema.as_ref() != PLAN_SCHEMA
            || self.version != PLAN_VERSION
            || self.plan_id.len() != 64
            || self.compatible_deletion_blocked != (self.cache_mode == CacheMode::Compatible)
            || Endpoint::parse(&self.endpoint)
                .map_err(HubError::validation)?
                .as_str()
                != self.endpoint.as_ref()
            || self.computed_id()?.as_ref() != self.plan_id.as_ref()
        {
            return Err(HubError::protocol());
        }
        let mut previous = None;
        for candidate in &self.candidates {
            let identity = candidate.partial_identity()?;
            if identity.key() != candidate.id()
                || previous.is_some_and(|value: &str| value >= candidate.id())
            {
                return Err(HubError::protocol());
            }
            previous = Some(candidate.id());
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
            self.policy,
            self.compatible_deletion_blocked,
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
            &[candidate],
        )?;
        let encoded = plan.to_json()?;
        let decoded = GcPlan::from_json(&encoded)?;
        assert_eq!(decoded.plan_id(), plan.plan_id());
        let mut altered: serde_json::Value = serde_json::from_slice(&encoded)?;
        altered["candidates"][0]["logical_bytes"] = 43.into();
        let _error = GcPlan::from_json(&serde_json::to_vec(&altered)?)
            .expect_err("altered plan must fail its identity check");
        Ok(())
    }
}
