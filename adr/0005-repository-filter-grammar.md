# ADR 0005: Repository filter grammar and matching semantics

- Status: Accepted
- Date: 2026-07-21

## Context

RFC 0001 allows a snapshot request to contain allow and ignore patterns. Those
patterns affect the exact selected path set and therefore fetch planning,
materialization collisions, snapshot identity, and offline lookup. A generic
"glob" promise is insufficient because Unix shells, Git ignores, common Rust
glob crates, and Python's `fnmatch` disagree about path separators, recursive
wildcards, case sensitivity, escaping, and malformed character classes.

`huggingface_hub` v1.24.0 at the compatibility commit pinned by ADR 0002 filters
repository paths with Python's case-sensitive `fnmatchcase`. Matching that
observable behavior makes requests portable between the Rust and Python tools
and avoids introducing a second similarly named pattern language.

## Decision

v0.1 allow and ignore patterns use the behavior of
`huggingface_hub.utils.filter_repo_objects` at commit
`36fd32c84d630f455a23b9a3bc4dc7b76d19cdde`. The implementation is private and
must be covered by copied behavioral cases and bidirectional conformance tests;
it must not expose a regex or glob-crate type.

Patterns are anchored against the entire validated repository file path. Paths
are canonical POSIX-style relative `RepoPath` values. Pattern backslashes are
normalized to `/` before matching so a pattern assembled with Windows
separators behaves like its POSIX spelling. Matching is case-sensitive on every
operating system and performs no Unicode normalization or case folding.

The accepted wildcard grammar is Python `fnmatchcase` rather than shell glob:

- `*` matches zero or more characters, including `/`;
- `?` matches exactly one character, including `/`;
- `[sequence]` matches one listed character or range and `[!sequence]` matches
  one character outside it;
- an unmatched `[` is treated literally, backslash is not an escape, and brace
  expansion, extglobs, regular expressions, and Git-ignore syntax are absent;
- `**` has no special recursive meaning beyond adjacent `*` wildcards;
- a pattern ending in `/` has `*` appended, so `path/to/` selects files beneath
  that directory;
- leading dots receive no special treatment.

The filter is applied only to validated file entries after the complete
commit-bound tree has been retrieved. It cannot make an absolute, traversal,
backslash-containing, reserved, or otherwise unsafe remote path acceptable.
Patterns are selection expressions and are never joined to a host filesystem
path.

Allow and ignore lists have these exact semantics:

- an omitted allow list admits every file, while a present but empty allow list
  admits none;
- an omitted or empty ignore list excludes nothing;
- a file passes an allow list when any allow pattern matches;
- after that, any matching ignore pattern excludes the file, so ignore wins;
- no implicit ignore patterns are added for downloads;
- an empty string is a valid pattern: it matches no valid repository file, so
  `allow = [""]` selects nothing and `ignore = [""]` excludes nothing.

Pattern order and duplicate patterns cannot change the selected set. Tree
readers reject duplicate file paths rather than allowing filtering to hide
corrupt metadata. After filtering, materialization collisions are rejected and
the selected paths are sorted lexicographically before planning. As accepted in
ADR 0002, the selection identity hashes only that canonical selected path set,
not the spelling or order of patterns that produced it. A request whose filters
select no files is represented as an explicit empty selection; whether a
network-facing snapshot operation permits that result remains a private service
decision until the request contract is implemented.

The matcher must be deterministic and resource-bounded. It may compile patterns
to an internal automaton or regular expression, but implementation-specific
syntax and errors cannot become observable. In particular, a Rust glob library
must not silently add separator-sensitive `*`, brace expansion, backslash
escaping, platform case folding, or special `**` behavior.

This ADR accepts the pattern language and selection rules only. It does not
accept public pattern wrapper names, request-builder methods, collection types,
or error variants.

## Consequences

- `*.json` selects nested JSON files as well as root files, and
  `data/*.json` also selects `data/nested/file.json`.
- Results are identical across Linux, macOS, and Windows despite host path and
  filesystem case behavior.
- Users familiar with Unix globbing must not assume that `*` stops at `/`; the
  public documentation and examples must call this out.
- Filter expression refactors do not create duplicate snapshots when they
  produce the same canonical selected path set.
- Future recursive-shell, Git-ignore, or regex filters require a distinctly
  named grammar and a later ADR rather than changing v0.1 pattern meaning.

## References

- [`filter_repo_objects` at the pinned compatibility commit](https://github.com/huggingface/huggingface_hub/blob/36fd32c84d630f455a23b9a3bc4dc7b76d19cdde/src/huggingface_hub/utils/_paths.py)
- [Python `fnmatch` documentation](https://docs.python.org/3/library/fnmatch.html)
- [ADR 0002](0002-cache-identity-and-format.md)
- [RFC 0001](../rfcs/0001-hub-store-v0.1.md)
