use std::env;
use std::error::Error;
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use serde::Deserialize;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use tempfile::TempDir;

use crate::{CommitId, Endpoint, RepoPath, RepositoryId, RepositorySpec, Revision};

use super::compatible_cache::{CompatibleCacheError, CompatibleCacheOffline, CompatibleSnapshot};
use super::hub_layout::{HubBlobKey, HubCacheLayout};
use super::hub_metadata::{HubTree, HubTreeEntry, encode_tree};
use super::publication::{
    Effects, NoPublicationFaults, OsFileSystem, RandomOperationIds, SystemClock,
};
use super::standard_cache::{SnapshotMaterialization, StandardCacheWriter};

const PYTHON_EXECUTABLE_ENV: &str = "HF_STORE_PYTHON_EXECUTABLE";
const PYTHON_REFERENCE_ROOT_ENV: &str = "HF_STORE_PYTHON_REFERENCE_ROOT";
const EXPECTED_HUB_COMMIT: &str = "36fd32c84d630f455a23b9a3bc4dc7b76d19cdde";
const EXPECTED_FILELOCK_VERSION: &str = "3.25.2";
const REPOSITORY_ID: &str = "fixture-org/writer-race";
const REVISION: &str = "refs/pr/17";
const COMMIT: &str = "4444444444444444444444444444444444444444";
const FILE_PATH: &str = "nested/config.json";
const CONTENT: &[u8] = b"{\"shared_writer\":\"one validated source\"}\n";
const PROCESS_TIMEOUT: Duration = Duration::from_secs(30);
const CONTENTION_PROBE: Duration = Duration::from_millis(250);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RaceMode {
    PythonFirst,
    RustFirst,
}

impl RaceMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::PythonFirst => "python-first",
            Self::RustFirst => "rust-first",
        }
    }

    const fn expected_python_body_calls(self) -> usize {
        match self {
            Self::PythonFirst => 1,
            Self::RustFirst => 0,
        }
    }
}

#[derive(Debug, Deserialize)]
struct PythonRaceResult {
    format_version: u32,
    producer: String,
    status: String,
    mode: String,
    repo_type: String,
    repo_id: String,
    revision: String,
    commit: String,
    filename: String,
    etag: String,
    blob_id: String,
    lfs_sha256: Option<String>,
    size: u64,
    content_sha256: String,
    body_calls: usize,
    filelock_version: String,
    lock_backend: String,
    lock_path: String,
    snapshot_path: String,
    offline_snapshot_path: String,
    pointer_form: String,
    blob_path: String,
    blob_exists: bool,
    tree_path: String,
    tree_exists: bool,
    ref_path: String,
    ref_value: Option<String>,
    scan_warnings: Vec<String>,
    force_copy: bool,
}

struct RaceFixture {
    _directory: TempDir,
    cache_root: PathBuf,
    control_dir: PathBuf,
    result_path: PathBuf,
    content_path: PathBuf,
    endpoint: Endpoint,
    spec: RepositorySpec,
    revision: Revision,
    commit: CommitId,
    file_path: RepoPath,
    tree: HubTree,
    etag: String,
}

impl RaceFixture {
    fn new() -> Result<Self, Box<dyn Error>> {
        let directory = TempDir::new()?;
        let cache_root = directory.path().join("cache");
        let control_dir = directory.path().join("control");
        let result_path = directory.path().join("python-result.json");
        let content_path = directory.path().join("content.bin");
        fs::create_dir(&cache_root)?;
        fs::create_dir(&control_dir)?;
        fs::write(&content_path, CONTENT)?;

        let endpoint = Endpoint::hugging_face();
        let spec = RepositorySpec::model(RepositoryId::parse(REPOSITORY_ID)?);
        let revision = Revision::parse(REVISION)?;
        let commit = CommitId::parse(COMMIT)?;
        let file_path = RepoPath::parse(FILE_PATH)?;
        let etag = git_blob_id(CONTENT);
        let entry = HubTreeEntry::new(u64::try_from(CONTENT.len())?, &etag)?;
        let tree = HubTree::new([(file_path.clone(), entry)])?;

        Ok(Self {
            _directory: directory,
            cache_root,
            control_dir,
            result_path,
            content_path,
            endpoint,
            spec,
            revision,
            commit,
            file_path,
            tree,
            etag,
        })
    }

    fn writer(&self) -> Result<StandardCacheWriter, CompatibleCacheError> {
        StandardCacheWriter::shared_for_test(
            &self.cache_root,
            &self.endpoint,
            &self.spec,
            effects(),
            SnapshotMaterialization::Copy,
        )
    }

    fn layout(&self) -> Result<HubCacheLayout, CompatibleCacheError> {
        HubCacheLayout::shared(&self.cache_root, &self.endpoint, &self.spec)
            .map_err(CompatibleCacheError::from)
    }

    fn spawn_python(&self, mode: RaceMode) -> Result<PythonChild, Box<dyn Error>> {
        let executable = env::var_os(PYTHON_EXECUTABLE_ENV)
            .ok_or_else(|| invalid_data(format!("{PYTHON_EXECUTABLE_ENV} is required")))?;
        let reference_root = env::var_os(PYTHON_REFERENCE_ROOT_ENV)
            .map(PathBuf::from)
            .ok_or_else(|| invalid_data(format!("{PYTHON_REFERENCE_ROOT_ENV} is required")))?;
        verify_reference_checkout(&reference_root)?;
        let script = repository_root().join("conformance/python/compatible_writer_race.py");
        if !script.is_file() {
            return Err(invalid_data("compatible writer race harness is missing").into());
        }
        let python_path = pinned_python_path(&reference_root)?;

        let mut command = Command::new(executable);
        command
            .arg(script)
            .arg("--mode")
            .arg(mode.as_str())
            .arg("--reference-root")
            .arg(&reference_root)
            .arg("--cache-root")
            .arg(&self.cache_root)
            .arg("--control-dir")
            .arg(&self.control_dir)
            .arg("--result")
            .arg(&self.result_path)
            .arg("--content")
            .arg(&self.content_path)
            .arg("--repo-type")
            .arg("model")
            .arg("--repo-id")
            .arg(REPOSITORY_ID)
            .arg("--revision")
            .arg(REVISION)
            .arg("--commit")
            .arg(COMMIT)
            .arg("--filename")
            .arg(FILE_PATH)
            .arg("--etag")
            .arg(&self.etag)
            .arg("--blob-id")
            .arg(&self.etag)
            .arg("--force-copy")
            .arg("--timeout-seconds")
            .arg(PROCESS_TIMEOUT.as_secs().to_string())
            .env("PYTHONPATH", python_path)
            .env("HF_HUB_DISABLE_TELEMETRY", "1")
            .env("HF_HUB_DISABLE_PROGRESS_BARS", "1")
            .env("DO_NOT_TRACK", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command.spawn()?;
        Ok(PythonChild::new(child))
    }

    fn mark(&self, name: &str) -> io::Result<()> {
        create_marker(&self.control_dir.join(name))
    }

    fn marker(&self, name: &str) -> PathBuf {
        self.control_dir.join(name)
    }
}

struct PythonChild {
    child: Option<Child>,
}

impl PythonChild {
    const fn new(child: Child) -> Self {
        Self { child: Some(child) }
    }

    fn wait_for_marker(&mut self, marker: &Path, context: &str) -> io::Result<()> {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if marker.try_exists()? {
                return Ok(());
            }
            if let Some(status) = self.try_wait()? {
                return Err(child_failure(context, status));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, context));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn assert_marker_absent_for(
        &mut self,
        marker: &Path,
        duration: Duration,
        context: &str,
    ) -> io::Result<()> {
        let deadline = Instant::now() + duration;
        loop {
            if marker.try_exists()? {
                return Err(invalid_data(context));
            }
            if let Some(status) = self.try_wait()? {
                return Err(child_failure(context, status));
            }
            if Instant::now() >= deadline {
                return Ok(());
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn wait_success(&mut self) -> io::Result<Output> {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            if let Some(status) = self.try_wait()? {
                let output = self.finish()?;
                if status.success() {
                    return Ok(output);
                }
                return Err(child_failure("pinned Python writer failed", status));
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "timed out waiting for pinned Python writer",
                ));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("pinned Python writer was already reaped"))?
            .try_wait()
    }

    fn finish(&mut self) -> io::Result<Output> {
        self.child
            .take()
            .ok_or_else(|| io::Error::other("pinned Python writer was already reaped"))?
            .wait_with_output()
    }
}

impl Drop for PythonChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _kill_result = child.kill();
            let _wait_result = child.wait();
        }
    }
}

#[test]
fn waiting_writer_refreshes_python_snapshot_index_after_blob_lock() -> Result<(), Box<dyn Error>> {
    let fixture = RaceFixture::new()?;
    let layout = fixture.layout()?;
    let blob_key = HubBlobKey::parse(&fixture.etag)?;
    let lock_path = layout.blob_lock(&blob_key);
    let lock_parent = lock_path
        .parent()
        .ok_or_else(|| invalid_data("blob lock path has no parent"))?;
    fs::create_dir_all(lock_parent)?;
    let lock_file = open_lock_file(&lock_path)?;
    fs4::FileExt::lock(&lock_file)?;

    let mut writer = fixture.writer()?;
    let (attempted_sender, attempted_receiver) = mpsc::channel();
    let (acquired_sender, acquired_receiver) = mpsc::channel();
    observe_writer_lock(&mut writer, attempted_sender, acquired_sender);
    let source_calls = Arc::new(AtomicUsize::new(0));
    let task = spawn_writer(
        writer,
        fixture.revision.clone(),
        fixture.commit.clone(),
        fixture.tree.clone(),
        fixture.file_path.clone(),
        Arc::clone(&source_calls),
        SourceGate::Open,
    );

    receive_signal(
        &attempted_receiver,
        "Rust writer did not attempt the held blob lock",
    )?;
    assert_signal_blocked(
        &acquired_receiver,
        "Rust writer acquired a Python-held blob lock",
    )?;
    publish_python_snapshot_only(&layout, &fixture)?;
    fs4::FileExt::unlock(&lock_file)?;
    receive_signal(
        &acquired_receiver,
        "Rust writer did not acquire the released blob lock",
    )?;

    let snapshot = task.wait()?;
    assert_eq!(source_calls.load(Ordering::Relaxed), 0);
    assert_snapshot(&snapshot, &fixture)?;
    Ok(())
}

#[test]
#[ignore = "requires an explicit Python executable and pinned huggingface_hub reference checkout"]
fn pinned_python_and_rust_writers_share_one_forced_copy_source() -> Result<(), Box<dyn Error>> {
    for mode in [RaceMode::PythonFirst, RaceMode::RustFirst] {
        run_mixed_writer_race(mode)?;
    }
    Ok(())
}

fn run_mixed_writer_race(mode: RaceMode) -> Result<(), Box<dyn Error>> {
    let fixture = RaceFixture::new()?;
    let mut python = fixture.spawn_python(mode)?;
    python.wait_for_marker(
        &fixture.marker("python.ready"),
        "pinned Python writer exited before readiness",
    )?;

    let rust_source_calls = Arc::new(AtomicUsize::new(0));
    let snapshot = match mode {
        RaceMode::PythonFirst => {
            fixture.mark("start")?;
            python.wait_for_marker(
                &fixture.marker("python.lock-acquired"),
                "Python-first writer did not acquire the blob lock",
            )?;
            run_python_first(&fixture, &mut python, Arc::clone(&rust_source_calls))?
        }
        RaceMode::RustFirst => run_rust_first(&fixture, &mut python, &rust_source_calls)?,
    };
    assert_snapshot(&snapshot, &fixture)?;

    let _output = python.wait_success()?;
    let python_result: PythonRaceResult = serde_json::from_slice(&fs::read(&fixture.result_path)?)?;
    validate_python_result(&fixture, mode, &python_result)?;
    let rust_calls = rust_source_calls.load(Ordering::Relaxed);
    assert_eq!(rust_calls + python_result.body_calls, 1);
    verify_warm_rust_reuse(&fixture)?;
    Ok(())
}

fn run_python_first(
    fixture: &RaceFixture,
    python: &mut PythonChild,
    source_calls: Arc<AtomicUsize>,
) -> Result<CompatibleSnapshot, Box<dyn Error>> {
    let mut writer = fixture.writer()?;
    let (attempted_sender, attempted_receiver) = mpsc::channel();
    let (acquired_sender, acquired_receiver) = mpsc::channel();
    observe_writer_lock(&mut writer, attempted_sender, acquired_sender);
    let task = spawn_writer(
        writer,
        fixture.revision.clone(),
        fixture.commit.clone(),
        fixture.tree.clone(),
        fixture.file_path.clone(),
        source_calls,
        SourceGate::Open,
    );

    receive_signal(
        &attempted_receiver,
        "Rust writer did not attempt the Python-held blob lock",
    )?;
    assert_signal_blocked(
        &acquired_receiver,
        "Rust writer acquired the Python-held blob lock before its release",
    )?;
    fixture.mark("release-python")?;
    receive_signal(
        &acquired_receiver,
        "Rust writer did not acquire the Python-released blob lock",
    )?;
    python.wait_for_marker(
        &fixture.marker("python.tree-written"),
        "Python-first writer did not publish its tree",
    )?;
    task.wait()
}

fn run_rust_first(
    fixture: &RaceFixture,
    python: &mut PythonChild,
    source_calls: &Arc<AtomicUsize>,
) -> Result<CompatibleSnapshot, Box<dyn Error>> {
    let mut writer = fixture.writer()?;
    let (attempted_sender, attempted_receiver) = mpsc::channel();
    let (acquired_sender, acquired_receiver) = mpsc::channel();
    observe_writer_lock(&mut writer, attempted_sender, acquired_sender);
    let (source_entered_sender, source_entered_receiver) = mpsc::channel();
    let (release_source_sender, release_source_receiver) = mpsc::channel();
    let task = spawn_writer(
        writer,
        fixture.revision.clone(),
        fixture.commit.clone(),
        fixture.tree.clone(),
        fixture.file_path.clone(),
        Arc::clone(source_calls),
        SourceGate::Hold {
            entered: source_entered_sender,
            release: release_source_receiver,
        },
    );

    receive_signal(
        &attempted_receiver,
        "Rust-first writer did not attempt its blob lock",
    )?;
    receive_signal(
        &acquired_receiver,
        "Rust-first writer did not acquire its blob lock",
    )?;
    receive_signal(
        &source_entered_receiver,
        "Rust-first writer did not enter its byte source",
    )?;
    fixture.mark("start")?;
    python.wait_for_marker(
        &fixture.marker("python.lock-attempted"),
        "Python writer did not contend on the Rust-held blob lock",
    )?;
    python.assert_marker_absent_for(
        &fixture.marker("python.lock-acquired"),
        CONTENTION_PROBE,
        "Python writer acquired the Rust-held blob lock before publication completed",
    )?;
    release_source_sender
        .send(())
        .map_err(|mpsc::SendError(())| invalid_data("Rust source release receiver closed"))?;
    let snapshot = task.wait()?;
    python.wait_for_marker(
        &fixture.marker("python.lock-acquired"),
        "Python writer did not acquire the published Rust blob",
    )?;
    Ok(snapshot)
}

enum SourceGate {
    Open,
    Hold {
        entered: mpsc::Sender<()>,
        release: Receiver<()>,
    },
}

struct WriterTask {
    result: Receiver<Result<CompatibleSnapshot, CompatibleCacheError>>,
    thread: Option<JoinHandle<()>>,
}

impl WriterTask {
    fn wait(mut self) -> Result<CompatibleSnapshot, Box<dyn Error>> {
        let result = self
            .result
            .recv_timeout(PROCESS_TIMEOUT)
            .map_err(|error| receive_error("timed out waiting for Rust writer", error))?;
        let handle = self
            .thread
            .take()
            .ok_or_else(|| invalid_data("Rust writer thread was already joined"))?;
        handle
            .join()
            .map_err(|_panic| invalid_data("Rust writer thread panicked"))?;
        Ok(result?)
    }
}

fn spawn_writer(
    writer: StandardCacheWriter,
    revision: Revision,
    commit: CommitId,
    tree: HubTree,
    path: RepoPath,
    source_calls: Arc<AtomicUsize>,
    mut source_gate: SourceGate,
) -> WriterTask {
    let (result_sender, result_receiver) = mpsc::channel();
    let handle = thread::spawn(move || {
        let result = writer.publish(
            &revision,
            &commit,
            &tree,
            std::slice::from_ref(&path),
            |_source_path| {
                source_calls.fetch_add(1, Ordering::Relaxed);
                if let SourceGate::Hold { entered, release } = &mut source_gate {
                    entered.send(()).map_err(|mpsc::SendError(())| {
                        io::Error::other("Rust source-entered receiver closed")
                    })?;
                    release.recv_timeout(PROCESS_TIMEOUT).map_err(|error| {
                        receive_error("timed out waiting to release Rust source", error)
                    })?;
                }
                Ok(Cursor::new(CONTENT.to_vec()))
            },
        );
        let _send_result = result_sender.send(result);
    });
    WriterTask {
        result: result_receiver,
        thread: Some(handle),
    }
}

fn observe_writer_lock(
    writer: &mut StandardCacheWriter,
    attempted: mpsc::Sender<()>,
    acquired: mpsc::Sender<()>,
) {
    writer.observe_blob_lock_for_test(
        Arc::new(move || {
            let _send_result = attempted.send(());
        }),
        Arc::new(move || {
            let _send_result = acquired.send(());
        }),
    );
}

fn publish_python_snapshot_only(layout: &HubCacheLayout, fixture: &RaceFixture) -> io::Result<()> {
    let snapshot = layout.snapshot_file(&fixture.commit, &fixture.file_path);
    let tree = layout.tree_path(&fixture.commit);
    fs::create_dir_all(
        snapshot
            .parent()
            .ok_or_else(|| invalid_data("snapshot path has no parent"))?,
    )?;
    fs::write(snapshot, CONTENT)?;
    fs::create_dir_all(
        tree.parent()
            .ok_or_else(|| invalid_data("tree path has no parent"))?,
    )?;
    let encoded = encode_tree(&fixture.tree).map_err(|error| invalid_data(error.to_string()))?;
    fs::write(tree, encoded)
}

fn verify_warm_rust_reuse(fixture: &RaceFixture) -> Result<(), Box<dyn Error>> {
    let writer = fixture.writer()?;
    let source_calls = AtomicUsize::new(0);
    let snapshot = writer.publish::<io::Empty, _>(
        &fixture.revision,
        &fixture.commit,
        &fixture.tree,
        std::slice::from_ref(&fixture.file_path),
        |_source_path| {
            source_calls.fetch_add(1, Ordering::Relaxed);
            Err(io::Error::other("warm cache reuse opened a byte source"))
        },
    )?;
    assert_eq!(source_calls.load(Ordering::Relaxed), 0);
    assert_snapshot(&snapshot, fixture)?;

    let offline = CompatibleCacheOffline::shared(
        &fixture.cache_root,
        &fixture.endpoint,
        &fixture.spec,
        effects(),
    )?;
    for revision in [
        Revision::parse(fixture.commit.as_str())?,
        fixture.revision.clone(),
    ] {
        let reopened = offline.open(&revision, std::slice::from_ref(&fixture.file_path))?;
        assert_snapshot(&reopened, fixture)?;
    }
    Ok(())
}

fn assert_snapshot(
    snapshot: &CompatibleSnapshot,
    fixture: &RaceFixture,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(snapshot.commit(), &fixture.commit);
    let [file] = snapshot.files() else {
        return Err(invalid_data("compatible snapshot did not contain exactly one file").into());
    };
    assert_eq!(file.path(), &fixture.file_path);
    assert_eq!(file.hub_blob_key().as_str(), fixture.etag);
    assert_eq!(file.size(), u64::try_from(CONTENT.len())?);
    assert_eq!(fs::read(file.content_path())?, CONTENT);
    Ok(())
}

fn validate_python_result(
    fixture: &RaceFixture,
    mode: RaceMode,
    result: &PythonRaceResult,
) -> io::Result<()> {
    let repository = "models--fixture-org--writer-race";
    let expected_snapshot = format!("{repository}/snapshots/{COMMIT}/{FILE_PATH}");
    let expected_snapshot_root = format!("{repository}/snapshots/{COMMIT}");
    let expected_blob = format!("{repository}/blobs/{}", fixture.etag);
    let expected_tree = format!("{repository}/trees/{COMMIT}.json");
    let expected_ref = format!("{repository}/refs/{REVISION}");
    let expected_lock = format!(".locks/{repository}/{}.lock", fixture.etag);
    let expected_sha256 = hex_sha256(CONTENT);
    let expected_size =
        u64::try_from(CONTENT.len()).map_err(|error| invalid_data(error.to_string()))?;

    if result.format_version != 1
        || result.producer != "huggingface_hub"
        || result.status != "ok"
        || result.mode != mode.as_str()
        || result.repo_type != "model"
        || result.repo_id != REPOSITORY_ID
        || result.revision != REVISION
        || result.commit != COMMIT
        || result.filename != FILE_PATH
        || result.etag != fixture.etag
        || result.blob_id != fixture.etag
        || result.lfs_sha256.is_some()
        || result.size != expected_size
        || result.content_sha256 != expected_sha256
        || result.body_calls != mode.expected_python_body_calls()
        || result.filelock_version != EXPECTED_FILELOCK_VERSION
        || result.lock_backend == "SoftFileLock"
        || result.lock_backend.is_empty()
        || result.lock_path != expected_lock
        || result.snapshot_path != expected_snapshot
        || result.offline_snapshot_path != expected_snapshot_root
        || result.pointer_form != "regular"
        || result.blob_path != expected_blob
        || (mode == RaceMode::RustFirst && !result.blob_exists)
        || result.tree_path != expected_tree
        || !result.tree_exists
        || result.ref_path != expected_ref
        || result.ref_value.as_deref() != Some(COMMIT)
        || !result.scan_warnings.is_empty()
        || !result.force_copy
    {
        return Err(invalid_data(format!(
            "pinned Python writer returned an unexpected conformance result: {result:?}"
        )));
    }
    Ok(())
}

fn receive_signal(receiver: &Receiver<()>, context: &str) -> io::Result<()> {
    receiver
        .recv_timeout(PROCESS_TIMEOUT)
        .map_err(|error| receive_error(context, error))
}

fn assert_signal_blocked(receiver: &Receiver<()>, context: &str) -> io::Result<()> {
    match receiver.recv_timeout(CONTENTION_PROBE) {
        Err(RecvTimeoutError::Timeout) => Ok(()),
        Ok(()) | Err(RecvTimeoutError::Disconnected) => Err(invalid_data(context)),
    }
}

fn receive_error(context: &str, error: RecvTimeoutError) -> io::Error {
    match error {
        RecvTimeoutError::Timeout => io::Error::new(io::ErrorKind::TimedOut, context),
        RecvTimeoutError::Disconnected => invalid_data(format!("{context}: channel disconnected")),
    }
}

fn create_marker(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.sync_all()
}

fn open_lock_file(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn child_failure(context: &str, status: ExitStatus) -> io::Error {
    io::Error::other(format!(
        "{context} with status {status}; child output withheld from diagnostics"
    ))
}

fn verify_reference_checkout(root: &Path) -> io::Result<()> {
    let git_directory = root.join(".git");
    if !git_directory.exists() {
        return Err(invalid_data("pinned Python reference has no Git metadata"));
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["rev-parse", "HEAD"])
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()?;
    if !output.status.success()
        || String::from_utf8_lossy(&output.stdout).trim() != EXPECTED_HUB_COMMIT
    {
        return Err(invalid_data(
            "Python reference is not the pinned huggingface_hub commit",
        ));
    }
    Ok(())
}

fn pinned_python_path(reference_root: &Path) -> io::Result<OsString> {
    let mut paths = vec![reference_root.join("src")];
    if let Some(existing) = env::var_os("PYTHONPATH") {
        paths.extend(env::split_paths(&existing));
    }
    env::join_paths(paths).map_err(|error| invalid_data(error.to_string()))
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn effects() -> Effects {
    Effects::new(
        Arc::new(OsFileSystem),
        Arc::new(RandomOperationIds),
        Arc::new(SystemClock),
        Arc::new(NoPublicationFaults),
    )
}

fn git_blob_id(bytes: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(format!("blob {}\0", bytes.len()).as_bytes());
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
