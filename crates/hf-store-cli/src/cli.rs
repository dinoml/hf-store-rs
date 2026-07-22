use std::ffi::OsString;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime};

use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use hf_store::{
    CacheMode, CommitId, Endpoint, FetchOptions, FetchRequest, GcPlan, GcPolicy, HubError,
    HubStore, OfflineStore, RepoPath, RepositoryId, RepositoryKind, RepositorySpec, Revision,
};
use serde_json::json;

use crate::config::{ResolvedCache, discover_token, resolve_cache};
use crate::output::{CommandOutcome, OutputFormat, emit, emit_error, usage};

const MAX_PLAN_BYTES: u64 = 16 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "hf-store", version, about)]
struct Cli {
    #[arg(long, value_enum, default_value_t = FormatArg::Human, global = true)]
    format: FormatArg,
    #[arg(long, default_value = "https://huggingface.co", global = true)]
    endpoint: String,
    #[arg(long, global = true)]
    cache_dir: Option<PathBuf>,
    #[arg(long, value_enum, global = true)]
    cache_mode: Option<CacheModeArg>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FormatArg {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CacheModeArg {
    Compatible,
    Owned,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum RepoKindArg {
    Model,
    Dataset,
    Space,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Download or strictly reopen one repository selection.
    Fetch(FetchArgs),
    /// Inventory one owned or compatible repository cache.
    Inspect(RepositoryArgs),
    /// Revalidate one exact repository selection.
    Verify(VerifyArgs),
    /// Plan or execute explicit cache garbage collection.
    Gc {
        #[command(subcommand)]
        command: GcCommand,
    },
}

#[derive(Debug, clap::Args)]
struct RepositoryArgs {
    #[arg(long, value_enum)]
    repo_kind: RepoKindArg,
    repository: String,
}

#[derive(Debug, clap::Args)]
struct FetchArgs {
    #[command(flatten)]
    repository: RepositoryArgs,
    #[arg(long, default_value = "main")]
    revision: String,
    #[arg(long = "allow")]
    allow_patterns: Vec<String>,
    #[arg(long = "ignore")]
    ignore_patterns: Vec<String>,
    /// Exact selected paths required by strict offline mode.
    #[arg(long = "path")]
    paths: Vec<String>,
    #[arg(long)]
    offline: bool,
    #[arg(long)]
    local_dir: Option<PathBuf>,
    #[arg(long)]
    force: bool,
    #[arg(long, conflicts_with = "token_file")]
    no_token: bool,
    #[arg(long, value_name = "PATH", conflicts_with = "no_token")]
    token_file: Option<PathBuf>,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
}

#[derive(Debug, clap::Args)]
struct VerifyArgs {
    #[command(flatten)]
    repository: RepositoryArgs,
    #[arg(long, default_value = "main")]
    revision: String,
    #[arg(long = "path", required = true)]
    paths: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum GcCommand {
    /// Create an immutable read-only plan.
    Plan(GcPlanArgs),
    /// Execute only a previously serialized plan.
    Execute(GcExecuteArgs),
}

#[derive(Debug, clap::Args)]
struct GcPlanArgs {
    #[command(flatten)]
    repository: RepositoryArgs,
    #[arg(long)]
    partial_min_age_seconds: Option<u64>,
    #[arg(long)]
    snapshot_min_age_seconds: Option<u64>,
    #[arg(long, default_value_t = 0)]
    keep_floor: usize,
    #[arg(long = "retain-commit")]
    retained_commits: Vec<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, clap::Args)]
struct GcExecuteArgs {
    #[command(flatten)]
    repository: RepositoryArgs,
    #[arg(long)]
    plan: PathBuf,
    #[arg(long, action = ArgAction::SetTrue)]
    yes: bool,
}

pub(crate) fn run(args: impl IntoIterator<Item = OsString>) -> ExitCode {
    let cli = match Cli::try_parse_from(args) {
        Ok(cli) => cli,
        Err(error) => {
            let _printed = error.print();
            return ExitCode::from(2);
        }
    };
    let format = match cli.format {
        FormatArg::Human => OutputFormat::Human,
        FormatArg::Json => OutputFormat::Json,
    };
    let endpoint = match Endpoint::parse(&cli.endpoint) {
        Ok(endpoint) => endpoint,
        Err(_error) => return usage("endpoint failed validation"),
    };
    let mode = cli.cache_mode.map(|mode| match mode {
        CacheModeArg::Compatible => CacheMode::Compatible,
        CacheModeArg::Owned => CacheMode::Owned,
    });
    let cache = match resolve_cache(&endpoint, mode, cli.cache_dir) {
        Ok(cache) => cache,
        Err(message) => return usage(&message),
    };
    match execute(cli.command, endpoint, &cache) {
        Ok(outcome) => emit(format, &outcome),
        Err(CommandFailure::Usage(message)) => usage(&message),
        Err(CommandFailure::Operation { command, error }) => emit_error(format, command, &error),
    }
}

fn execute(
    command: Command,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    match command {
        Command::Fetch(args) => execute_fetch(args, endpoint, cache),
        Command::Inspect(args) => execute_inspect(&args, endpoint, cache),
        Command::Verify(args) => execute_verify(&args, endpoint, cache),
        Command::Gc {
            command: GcCommand::Plan(args),
        } => execute_gc_plan(args, endpoint, cache),
        Command::Gc {
            command: GcCommand::Execute(args),
        } => execute_gc(&args, endpoint, cache),
    }
}

fn execute_fetch(
    args: FetchArgs,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    let repository = parse_repository(&args.repository)?;
    let revision = parse_revision(&args.revision)?;
    if args.force && args.local_dir.is_none() {
        return Err(CommandFailure::usage(
            "--force is valid only together with --local-dir",
        ));
    }
    if args.offline {
        if !args.paths.is_empty()
            && (!args.allow_patterns.is_empty() || !args.ignore_patterns.is_empty())
        {
            return Err(CommandFailure::usage(
                "exact offline --path values cannot be combined with filters",
            ));
        }
        let store = OfflineStore::new(&cache.directory)
            .endpoint(endpoint)
            .cache_mode(cache.mode);
        let mut request =
            FetchRequest::new(repository, revision).ignore_patterns(args.ignore_patterns);
        if !args.paths.is_empty() {
            request = request.allow_patterns(args.paths);
        } else if !args.allow_patterns.is_empty() {
            request = request.allow_patterns(args.allow_patterns);
        }
        if let Some(local_dir) = args.local_dir {
            let local = store
                .materialize_request_to_local_dir(
                    &request,
                    &local_dir,
                    args.force,
                    &hf_store::CancellationToken::new(),
                )
                .map_err(|error| CommandFailure::operation("fetch", error))?;
            return Ok(local_directory_outcome(&local));
        }
        let snapshot = store
            .open_request(&request)
            .map_err(|error| CommandFailure::operation("fetch", error))?;
        return Ok(snapshot_outcome(&snapshot));
    }

    if !args.paths.is_empty() {
        return Err(CommandFailure::usage(
            "online fetch selects paths with --allow and --ignore",
        ));
    }
    if args.concurrency == 0 {
        return Err(CommandFailure::usage(
            "--concurrency must be greater than zero",
        ));
    }
    let mut request = FetchRequest::new(repository, revision).ignore_patterns(args.ignore_patterns);
    if !args.allow_patterns.is_empty() {
        request = request.allow_patterns(args.allow_patterns);
    }
    if let Some(token) =
        discover_token(args.no_token, args.token_file.as_deref()).map_err(CommandFailure::usage)?
    {
        request = request.authorization(token);
    }
    let store = HubStore::builder()
        .endpoint(endpoint)
        .cache_root(&cache.directory)
        .cache_mode(cache.mode)
        .max_concurrent_downloads(args.concurrency)
        .build();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_error| CommandFailure::usage("failed to initialize the CLI runtime"))?;
    if let Some(local_dir) = args.local_dir {
        let local = runtime
            .block_on(store.fetch_to_local_dir(
                request,
                FetchOptions::default(),
                &local_dir,
                args.force,
            ))
            .map_err(|error| CommandFailure::operation("fetch", error))?;
        Ok(local_directory_outcome(&local))
    } else {
        let snapshot = runtime
            .block_on(store.fetch(request, FetchOptions::default()))
            .map_err(|error| CommandFailure::operation("fetch", error))?;
        Ok(snapshot_outcome(&snapshot))
    }
}

fn execute_inspect(
    args: &RepositoryArgs,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    let repository = parse_repository(args)?;
    let report = OfflineStore::new(&cache.directory)
        .endpoint(endpoint)
        .cache_mode(cache.mode)
        .inspect_repository(&repository)
        .map_err(|error| CommandFailure::operation("inspect", error))?;
    report_outcome("inspect", &report, "ok", "ok", 0)
}

fn execute_verify(
    args: &VerifyArgs,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    let repository = parse_repository(&args.repository)?;
    let revision = parse_revision(&args.revision)?;
    let paths = parse_paths(&args.paths)?;
    let report = OfflineStore::new(&cache.directory)
        .endpoint(endpoint)
        .cache_mode(cache.mode)
        .verify(&repository, &revision, &paths);
    let (status, classification, code) = if report.is_valid() {
        ("ok", "ok", 0)
    } else {
        ("findings", "findings", 1)
    };
    report_outcome("verify", &report, status, classification, code)
}

fn execute_gc_plan(
    args: GcPlanArgs,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    let repository = parse_repository(&args.repository)?;
    let mut policy = GcPolicy::report_only();
    if let Some(seconds) = args.partial_min_age_seconds {
        policy = policy
            .with_expired_partials(Duration::from_secs(seconds))
            .ok_or_else(|| CommandFailure::usage("partial retention duration is too large"))?;
    }
    if let Some(seconds) = args.snapshot_min_age_seconds {
        policy = policy
            .with_unreferenced_snapshots(Duration::from_secs(seconds), args.keep_floor)
            .ok_or_else(|| CommandFailure::usage("snapshot retention duration is too large"))?;
    } else if args.keep_floor != 0 || !args.retained_commits.is_empty() {
        return Err(CommandFailure::usage(
            "snapshot keep roots require --snapshot-min-age-seconds",
        ));
    }
    for value in args.retained_commits {
        let commit = CommitId::parse(value)
            .map_err(|_error| CommandFailure::usage("retained commit failed validation"))?;
        policy = policy.retain_commit(&commit);
    }
    let plan = OfflineStore::new(&cache.directory)
        .endpoint(endpoint)
        .cache_mode(cache.mode)
        .gc_plan(&repository, policy, SystemTime::now())
        .map_err(|error| CommandFailure::operation("gc-plan", error))?;
    if let Some(path) = args.output {
        write_plan_create_new(&path, &cache.directory, &plan)?;
    }
    report_outcome("gc-plan", &plan, "ok", "ok", 0)
}

fn execute_gc(
    args: &GcExecuteArgs,
    endpoint: Endpoint,
    cache: &ResolvedCache,
) -> Result<CommandOutcome, CommandFailure> {
    if !args.yes {
        return Err(CommandFailure::usage(
            "gc execute requires the explicit --yes confirmation",
        ));
    }
    let repository = parse_repository(&args.repository)?;
    let plan = read_plan(&args.plan)?;
    let report = OfflineStore::new(&cache.directory)
        .endpoint(endpoint)
        .cache_mode(cache.mode)
        .gc_execute(&repository, &plan, SystemTime::now())
        .map_err(|error| CommandFailure::operation("gc-execute", error))?;
    let (status, classification, code) = if report.skipped().is_empty() {
        ("ok", "ok", 0)
    } else {
        ("findings", "findings", 1)
    };
    report_outcome("gc-execute", &report, status, classification, code)
}

fn parse_repository(args: &RepositoryArgs) -> Result<RepositorySpec, CommandFailure> {
    let id = RepositoryId::parse(&args.repository)
        .map_err(|_error| CommandFailure::usage("repository identifier failed validation"))?;
    Ok(RepositorySpec::new(
        match args.repo_kind {
            RepoKindArg::Model => RepositoryKind::Model,
            RepoKindArg::Dataset => RepositoryKind::Dataset,
            RepoKindArg::Space => RepositoryKind::Space,
        },
        id,
    ))
}

fn parse_revision(value: &str) -> Result<Revision, CommandFailure> {
    Revision::parse(value).map_err(|_error| CommandFailure::usage("revision failed validation"))
}

fn parse_paths(values: &[String]) -> Result<Vec<RepoPath>, CommandFailure> {
    values
        .iter()
        .map(|value| {
            RepoPath::parse(value)
                .map_err(|_error| CommandFailure::usage("repository path failed validation"))
        })
        .collect()
}

fn report_outcome(
    command: &'static str,
    report: &impl serde::Serialize,
    status: &'static str,
    classification: &'static str,
    code: u8,
) -> Result<CommandOutcome, CommandFailure> {
    let result = serde_json::to_value(report)
        .map_err(|_error| CommandFailure::usage("failed to serialize command report"))?;
    let human = serde_json::to_string_pretty(&result)
        .map_err(|_error| CommandFailure::usage("failed to render command report"))?;
    Ok(CommandOutcome {
        command,
        status,
        classification,
        code,
        human,
        result,
    })
}

fn snapshot_outcome(snapshot: &hf_store::Snapshot) -> CommandOutcome {
    let files = snapshot
        .files()
        .iter()
        .map(|file| {
            json!({
                "path": file.path().as_str(),
                "sha256": file.sha256(),
                "size": file.size(),
                "form": file.form(),
            })
        })
        .collect::<Vec<_>>();
    CommandOutcome {
        command: "fetch",
        status: "ok",
        classification: "ok",
        code: 0,
        human: snapshot.directory().to_string_lossy().into_owned(),
        result: json!({
            "repository_kind": snapshot.repository().kind().to_string(),
            "repository": snapshot.repository().id().as_str(),
            "commit": snapshot.commit().as_str(),
            "selection_id": snapshot.selection_id().to_string(),
            "destination_kind": "snapshot",
            "destination": snapshot.directory().to_string_lossy(),
            "reused": snapshot.was_reused(),
            "files": files,
        }),
    }
}

fn local_directory_outcome(local: &hf_store::LocalDirectory) -> CommandOutcome {
    let files = local
        .files()
        .iter()
        .map(|file| {
            json!({
                "path": file.path().as_str(),
                "sha256": file.sha256(),
                "size": file.size(),
            })
        })
        .collect::<Vec<_>>();
    CommandOutcome {
        command: "fetch",
        status: "ok",
        classification: "ok",
        code: 0,
        human: local.root().to_string_lossy().into_owned(),
        result: json!({
            "repository_kind": local.repository().kind().to_string(),
            "repository": local.repository().id().as_str(),
            "commit": local.commit().as_str(),
            "selection_id": local.selection_id().to_string(),
            "destination_kind": "local-dir",
            "destination": local.root().to_string_lossy(),
            "files": files,
        }),
    }
}

fn write_plan_create_new(
    path: &Path,
    cache_root: &Path,
    plan: &GcPlan,
) -> Result<(), CommandFailure> {
    if path.starts_with(cache_root) {
        return Err(CommandFailure::usage(
            "GC plan output must be outside the selected cache root",
        ));
    }
    let bytes = plan
        .to_json()
        .map_err(|error| CommandFailure::operation("gc-plan", error))?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|_error| CommandFailure::usage("GC plan output already exists or is unsafe"))?;
    file.write_all(&bytes)
        .and_then(|()| file.write_all(b"\n"))
        .and_then(|()| file.sync_all())
        .map_err(|_error| CommandFailure::usage("failed to publish GC plan output"))
}

fn read_plan(path: &Path) -> Result<GcPlan, CommandFailure> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|_error| CommandFailure::usage("GC plan file is unavailable"))?;
    let metadata = file
        .metadata()
        .map_err(|_error| CommandFailure::usage("GC plan metadata is unavailable"))?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_PLAN_BYTES {
        return Err(CommandFailure::usage(
            "GC plan must be a bounded regular file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_PLAN_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_error| CommandFailure::usage("GC plan could not be read"))?;
    GcPlan::from_json(&bytes).map_err(|_error| CommandFailure::usage("GC plan failed validation"))
}

#[derive(Debug)]
enum CommandFailure {
    Usage(String),
    Operation {
        command: &'static str,
        error: HubError,
    },
}

impl CommandFailure {
    fn usage(message: impl Into<String>) -> Self {
        Self::Usage(message.into())
    }

    const fn operation(command: &'static str, error: HubError) -> Self {
        Self::Operation { command, error }
    }
}
