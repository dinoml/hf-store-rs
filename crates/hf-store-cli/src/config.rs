use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::{Dir, OpenOptions};
use hf_store::{AuthToken, CacheMode, Endpoint};
use zeroize::Zeroizing;

const MAX_TOKEN_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug)]
pub(crate) struct ResolvedCache {
    pub(crate) mode: CacheMode,
    pub(crate) directory: PathBuf,
}

pub(crate) fn resolve_cache(
    endpoint: &Endpoint,
    requested_mode: Option<CacheMode>,
    explicit_directory: Option<PathBuf>,
) -> Result<ResolvedCache, String> {
    let canonical = endpoint == &Endpoint::hugging_face();
    let mode = requested_mode.unwrap_or(if canonical {
        CacheMode::Compatible
    } else {
        CacheMode::Owned
    });
    if !canonical && mode == CacheMode::Compatible && explicit_directory.is_none() {
        return Err("a custom compatible endpoint requires an explicit --cache-dir".to_owned());
    }
    let directory = match explicit_directory {
        Some(path) => path,
        None => match mode {
            CacheMode::Compatible => {
                match first_path_env(&["HF_HUB_CACHE", "HUGGINGFACE_HUB_CACHE"]) {
                    Some(path) => path,
                    None => resolve_hf_home()?.join("hub"),
                }
            }
            CacheMode::Owned => match first_path_env(&["HF_STORE_CACHE"]) {
                Some(path) => path,
                None => resolve_hf_home()?.join("hf-store"),
            },
            _ => return Err("selected cache mode is unsupported by this CLI version".to_owned()),
        },
    };
    Ok(ResolvedCache { mode, directory })
}

pub(crate) fn discover_token(
    no_token: bool,
    token_file: Option<&Path>,
) -> Result<Option<AuthToken>, String> {
    if no_token {
        return Ok(None);
    }
    if let Some(path) = token_file {
        let value = if path == Path::new("-") {
            read_token_stdin()?
        } else {
            read_token_file(path, true)?
                .ok_or_else(|| "explicit token file is missing".to_owned())?
        };
        return validated_token(value).map(Some);
    }
    for name in ["HF_TOKEN", "HUGGING_FACE_HUB_TOKEN"] {
        if let Some(value) = env_unicode(name)? {
            return validated_token(Zeroizing::new(value)).map(Some);
        }
    }
    if let Some(path) = env_path("HF_TOKEN_PATH") {
        return read_token_file(&path, false)?
            .map(validated_token)
            .transpose();
    }
    read_token_file(&resolve_hf_home()?.join("token"), false)?
        .map(validated_token)
        .transpose()
}

fn resolve_hf_home() -> Result<PathBuf, String> {
    if let Some(path) = env_path("HF_HOME") {
        return Ok(path);
    }
    if let Some(path) = env_path("XDG_CACHE_HOME") {
        return Ok(path.join("huggingface"));
    }
    let home = env_path(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .ok_or_else(|| "cannot resolve a cache home; pass --cache-dir".to_owned())?;
    Ok(home.join(".cache").join("huggingface"))
}

fn first_path_env(names: &[&str]) -> Option<PathBuf> {
    for name in names {
        if let Some(value) = env_path(name) {
            return Some(value);
        }
    }
    None
}

fn env_path(name: &str) -> Option<PathBuf> {
    match std::env::var_os(name) {
        None => None,
        Some(value) if value.is_empty() => None,
        Some(value) => Some(PathBuf::from(value)),
    }
}

fn env_unicode(name: &str) -> Result<Option<String>, String> {
    match std::env::var_os(name) {
        None => Ok(None),
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_non_unicode: OsString| format!("{name} is not valid Unicode")),
    }
}

fn read_token_stdin() -> Result<Zeroizing<String>, String> {
    let mut bytes = Vec::new();
    io::stdin()
        .lock()
        .take(MAX_TOKEN_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_error| "failed to read token from standard input".to_owned())?;
    decode_token_bytes(bytes)
}

fn read_token_file(path: &Path, explicit: bool) -> Result<Option<Zeroizing<String>>, String> {
    let Some(name) = path.file_name() else {
        return Err("token path must name a regular file".to_owned());
    };
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let directory = match Dir::open_ambient_dir(parent, cap_std::ambient_authority()) {
        Ok(directory) => directory,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !explicit => return Ok(None),
        Err(_error) => return Err("token file parent is unavailable".to_owned()),
    };
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    let file = match directory.open_with(name, &options) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound && !explicit => return Ok(None),
        Err(_error) => return Err("token file is unavailable or unsafe".to_owned()),
    };
    let metadata = file
        .metadata()
        .map_err(|_error| "token file metadata is unavailable".to_owned())?;
    if !metadata.file_type().is_file() || metadata.len() > MAX_TOKEN_BYTES {
        return Err("token file must be a bounded regular file".to_owned());
    }
    let mut bytes = Vec::new();
    file.take(MAX_TOKEN_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_error| "token file could not be read".to_owned())?;
    decode_token_bytes(bytes).map(Some)
}

fn decode_token_bytes(bytes: Vec<u8>) -> Result<Zeroizing<String>, String> {
    if u64::try_from(bytes.len()).map_err(|_overflow| "token is oversized".to_owned())?
        > MAX_TOKEN_BYTES
    {
        return Err("token is oversized".to_owned());
    }
    String::from_utf8(bytes)
        .map(Zeroizing::new)
        .map_err(|_error| "token is not valid UTF-8".to_owned())
}

fn validated_token(mut value: Zeroizing<String>) -> Result<AuthToken, String> {
    let trimmed = value.trim_matches(|character: char| character.is_ascii_whitespace());
    if trimmed.is_empty() || trimmed.contains(['\r', '\n']) || trimmed.chars().any(char::is_control)
    {
        return Err("token must contain exactly one non-empty line".to_owned());
    }
    let owned = trimmed.to_owned();
    value.clear();
    AuthToken::new(owned).map_err(|_error| "token failed validation".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_validation_rejects_multiline_and_controls_without_echoing_values() {
        for value in ["", "one\ntwo", "secret\u{7f}"] {
            let error = validated_token(Zeroizing::new(value.to_owned()))
                .expect_err("unsafe token unexpectedly validated");
            if !value.is_empty() {
                assert!(!error.contains(value));
            }
        }
    }
}
