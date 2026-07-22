use crate::RepoPath;
use crate::validation::{ValidationError, ValidationErrorKind};

use super::key::SelectionId;

#[derive(Clone, Debug)]
pub(crate) struct RepositoryFilter {
    allow: Option<Box<[Pattern]>>,
    ignore: Box<[Pattern]>,
}

impl RepositoryFilter {
    pub(crate) fn new(allow_patterns: Option<&[&str]>, ignore_patterns: &[&str]) -> Self {
        Self {
            allow: allow_patterns.map(compile_patterns),
            ignore: compile_patterns(ignore_patterns),
        }
    }

    pub(crate) fn select(
        &self,
        paths: impl IntoIterator<Item = RepoPath>,
    ) -> Result<RepositorySelection, ValidationError> {
        let mut paths = paths.into_iter().collect::<Vec<_>>();
        paths.sort_unstable();
        if paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(duplicate_tree_path());
        }

        let selected = paths
            .into_iter()
            .filter(|path| self.includes(path.as_str()))
            .collect::<Vec<_>>();
        let selection_id = SelectionId::derive(&selected)?;
        Ok(RepositorySelection {
            paths: selected.into_boxed_slice(),
            selection_id,
        })
    }

    fn includes(&self, path: &str) -> bool {
        let allowed = self
            .allow
            .as_deref()
            .is_none_or(|patterns| patterns.iter().any(|pattern| pattern.matches(path)));
        allowed && !self.ignore.iter().any(|pattern| pattern.matches(path))
    }
}

#[derive(Clone, Debug)]
struct Pattern {
    tokens: Box<[Token]>,
}

impl Pattern {
    fn compile(pattern: &str) -> Self {
        Self::compile_normalized(&normalize_pattern(pattern))
    }

    fn compile_normalized(pattern: &str) -> Self {
        let characters = pattern.chars().collect::<Vec<_>>();
        let mut tokens = Vec::with_capacity(characters.len());
        let mut index = 0;

        while index < characters.len() {
            let Some(&character) = characters.get(index) else {
                break;
            };
            match character {
                '*' => {
                    if !matches!(tokens.last(), Some(Token::Star)) {
                        tokens.push(Token::Star);
                    }
                    index += 1;
                }
                '?' => {
                    tokens.push(Token::AnyCharacter);
                    index += 1;
                }
                '[' => {
                    if let Some((class, next_index)) = CharacterClass::parse(&characters, index + 1)
                    {
                        tokens.push(Token::Class(class));
                        index = next_index;
                    } else {
                        tokens.push(Token::Literal('['));
                        index += 1;
                    }
                }
                literal => {
                    tokens.push(Token::Literal(literal));
                    index += 1;
                }
            }
        }

        Self {
            tokens: tokens.into_boxed_slice(),
        }
    }

    fn matches(&self, path: &str) -> bool {
        let characters = path.chars().collect::<Vec<_>>();
        let mut current = vec![false; characters.len() + 1];
        let mut next = vec![false; characters.len() + 1];
        current[0] = true;

        for token in &self.tokens {
            next.fill(false);
            match token {
                Token::Star => {
                    next[0] = current[0];
                    for index in 1..=characters.len() {
                        next[index] = current[index] || next[index - 1];
                    }
                }
                Token::AnyCharacter => {
                    next[1..].copy_from_slice(&current[..characters.len()]);
                }
                Token::Literal(expected) => {
                    for (index, actual) in characters.iter().enumerate() {
                        next[index + 1] = current[index] && actual == expected;
                    }
                }
                Token::Class(class) => {
                    for (index, character) in characters.iter().enumerate() {
                        next[index + 1] = current[index] && class.matches(*character);
                    }
                }
            }
            std::mem::swap(&mut current, &mut next);
        }

        current[characters.len()]
    }
}

fn compile_patterns(patterns: &[&str]) -> Box<[Pattern]> {
    let mut normalized = patterns
        .iter()
        .map(|pattern| normalize_pattern(pattern))
        .collect::<Vec<_>>();
    normalized.sort_unstable();
    normalized.dedup();
    normalized
        .iter()
        .map(|pattern| Pattern::compile_normalized(pattern))
        .collect()
}

fn normalize_pattern(pattern: &str) -> String {
    let mut normalized = pattern.replace('\\', "/");
    if normalized.ends_with('/') {
        normalized.push('*');
    }
    normalized
}

#[derive(Clone, Debug)]
enum Token {
    Literal(char),
    AnyCharacter,
    Star,
    Class(CharacterClass),
}

#[derive(Clone, Debug)]
struct CharacterClass {
    negated: bool,
    literals: Box<[char]>,
    ranges: Box<[(char, char)]>,
}

impl CharacterClass {
    fn parse(pattern: &[char], start: usize) -> Option<(Self, usize)> {
        let mut end = start;
        if pattern.get(end) == Some(&'!') {
            end += 1;
        }
        if pattern.get(end) == Some(&']') {
            end += 1;
        }
        while pattern.get(end).is_some_and(|character| *character != ']') {
            end += 1;
        }
        if end == pattern.len() {
            return None;
        }

        let content = &pattern[start..end];
        let (negated, members) = match content.strip_prefix(&['!']) {
            Some(members) => (true, members),
            None => (false, content),
        };
        let (literals, ranges) = class_members(members);
        Some((
            Self {
                negated,
                literals: literals.into_boxed_slice(),
                ranges: ranges.into_boxed_slice(),
            },
            end + 1,
        ))
    }

    fn matches(&self, character: char) -> bool {
        let contained = self.literals.contains(&character)
            || self
                .ranges
                .iter()
                .any(|(start, end)| *start <= character && character <= *end);
        contained != self.negated
    }
}

fn class_members(members: &[char]) -> (Vec<char>, Vec<(char, char)>) {
    if !members.contains(&'-') {
        return (members.to_vec(), Vec::new());
    }

    let mut chunks = Vec::<Vec<char>>::new();
    let mut chunk_start = 0;
    let mut search_from = 1;
    while let Some(relative) = members
        .get(search_from..)
        .and_then(|remaining| remaining.iter().position(|character| *character == '-'))
    {
        let hyphen = search_from + relative;
        chunks.push(members[chunk_start..hyphen].to_vec());
        chunk_start = hyphen + 1;
        search_from = hyphen.saturating_add(3);
    }

    if chunk_start < members.len() {
        chunks.push(members[chunk_start..].to_vec());
    } else if let Some(last) = chunks.last_mut() {
        last.push('-');
    }

    for index in (1..chunks.len()).rev() {
        let is_reversed = chunks[index - 1]
            .last()
            .zip(chunks[index].first())
            .is_some_and(|(start, end)| start > end);
        if is_reversed {
            let suffix = chunks[index].iter().skip(1).copied().collect::<Vec<_>>();
            let _removed_endpoint = chunks[index - 1].pop();
            chunks[index - 1].extend(suffix);
            chunks.remove(index);
        }
    }

    let literals = chunks.iter().flatten().copied().collect::<Vec<_>>();
    let ranges = chunks
        .windows(2)
        .filter_map(|pair| pair[0].last().copied().zip(pair[1].first().copied()))
        .collect::<Vec<_>>();
    (literals, ranges)
}

fn duplicate_tree_path() -> ValidationError {
    ValidationError::new("repository tree", ValidationErrorKind::Malformed)
}

#[derive(Clone, Debug)]
pub(crate) struct RepositorySelection {
    paths: Box<[RepoPath]>,
    selection_id: SelectionId,
}

impl RepositorySelection {
    pub(crate) fn paths(&self) -> &[RepoPath] {
        &self.paths
    }

    pub(crate) const fn selection_id(&self) -> &SelectionId {
        &self.selection_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copied_pinned_upstream_filter_cases_match() -> Result<(), Box<dyn std::error::Error>> {
        let files = paths(&[
            "not_hidden.pdf",
            "profile.jpg",
            ".hidden.pdf",
            ".hidden_picture.png",
        ])?;

        assert_selected(
            &files,
            Some(&["*.pdf"]),
            &[],
            &[".hidden.pdf", "not_hidden.pdf"],
        )?;
        assert_selected(&files, Some(&["*.pdf"]), &[".*"], &["not_hidden.pdf"])?;
        assert_selected(
            &files,
            Some(&["*.png", "*.jpg"]),
            &[],
            &[".hidden_picture.png", "profile.jpg"],
        )?;

        let nested = paths(&[
            "file.txt",
            "lfs.bin",
            "path/to/file.txt",
            "path/to/lfs.bin",
            "nested/path/to/file.txt",
            "nested/path/to/lfs.bin",
        ])?;
        assert_selected(
            &nested,
            Some(&["path/to/"]),
            &[],
            &["path/to/file.txt", "path/to/lfs.bin"],
        )?;

        Ok(())
    }

    #[test]
    fn copied_pinned_upstream_empty_case_and_separator_cases()
    -> Result<(), Box<dyn std::error::Error>> {
        let files = paths(&["file.txt", "lfs.bin"])?;
        assert_selected(&files, Some(&[""]), &[], &[])?;
        assert_selected(&files, None, &[""], &["file.txt", "lfs.bin"])?;

        let files = paths(&["unet/config.json", "vae/config.json", "unet/model.bin"])?;
        assert_selected(
            &files,
            Some(&[r"unet\config.json"]),
            &[],
            &["unet/config.json"],
        )?;
        assert_selected(
            &files,
            None,
            &[r"unet\*.json"],
            &["unet/model.bin", "vae/config.json"],
        )?;

        Ok(())
    }

    #[test]
    fn copied_pinned_upstream_matching_is_case_sensitive() -> Result<(), Box<dyn std::error::Error>>
    {
        let files = paths(&["README.md", "notes.MD"])?;
        assert_selected(&files, Some(&["*.MD"]), &[], &["notes.MD"])?;
        assert_selected(&files, Some(&["*.md"]), &[], &["README.md"])?;

        let logs = paths(&["keep.txt", "drop.LOG", "keep.log"])?;
        assert_selected(&logs, None, &["*.LOG"], &["keep.log", "keep.txt"])?;

        Ok(())
    }

    #[test]
    fn hardcoded_python_fnmatchcase_character_class_goldens() {
        let cases = [
            ("a", "[abc]", true),
            ("d", "[!abc]", true),
            ("a", "[!abc]", false),
            ("]", "[]]", true),
            ("a", "[!]]", true),
            ("]", "[!]]", false),
            ("b", "[a-c]", true),
            ("z", "[z-a]", false),
            ("m", "[!z-a]", true),
            ("-", "[-a]", true),
            ("-", "[a-]", true),
            ("-", "[a-b-c]", true),
            ("c", "[a-b-c]", true),
            ("d", "[a-b-c]", false),
            ("c", "[a--c]", true),
            ("a", "[a--c]", false),
            ("[", "[[]", true),
            ("!", "[!!]", false),
            ("a", "[!!]", true),
            ("^", "[^^]", true),
            ("a", "[^^]", false),
            ("&", "[a&&b]", true),
            ("~", "[a~~b]", true),
            ("|", "[a||b]", true),
            ("β", "[α-γ]", true),
            ("δ", "[α-γ]", false),
        ];

        for (path, pattern, expected) in cases {
            assert_eq!(
                Pattern::compile(pattern).matches(path),
                expected,
                "Python fnmatchcase golden differs for {path:?} and {pattern:?}"
            );
        }
    }

    #[test]
    fn python_fnmatchcase_treats_wildcards_as_anchored_and_separator_agnostic() {
        let cases = [
            ("nested/file.json", "*.json", true),
            ("data/nested/file.json", "data/*.json", true),
            ("a/b", "a?b", true),
            ("a/b", "a**b", true),
            ("prefix-a", "a", false),
            ("a-suffix", "a", false),
            ("[abc", "[abc", true),
            (".hidden", "*", true),
        ];

        for (path, pattern, expected) in cases {
            assert_eq!(
                Pattern::compile(pattern).matches(path),
                expected,
                "Python fnmatchcase golden differs for {path:?} and {pattern:?}"
            );
        }
    }

    #[test]
    fn omitted_and_empty_allow_lists_are_distinct() -> Result<(), Box<dyn std::error::Error>> {
        let files = paths(&["config.json", "model.bin"])?;

        assert_selected(&files, None, &[], &["config.json", "model.bin"])?;
        assert_selected(&files, Some(&[]), &[], &[])?;

        Ok(())
    }

    #[test]
    fn ignore_patterns_win_after_allow_patterns() -> Result<(), Box<dyn std::error::Error>> {
        let files = paths(&["config.json", "nested/config.json", "model.bin"])?;

        assert_selected(&files, Some(&["*.json"]), &["nested/*"], &["config.json"])?;

        Ok(())
    }

    #[test]
    fn exact_duplicate_tree_paths_are_rejected_before_filtering()
    -> Result<(), Box<dyn std::error::Error>> {
        let duplicate = RepoPath::parse("duplicate.bin")?;
        let filter = RepositoryFilter::new(Some(&["*.json"]), &[]);

        let error = filter
            .select([duplicate.clone(), duplicate])
            .expect_err("an ignored duplicate tree path was accepted");

        assert_eq!(error.subject(), "repository tree");
        assert!(error.is_malformed());
        Ok(())
    }

    #[test]
    fn portable_collisions_are_checked_after_filtering() -> Result<(), Box<dyn std::error::Error>> {
        let colliding = paths(&["Config.json", "config.json"])?;

        let one = RepositoryFilter::new(Some(&["Config.json"]), &[]).select(colliding.clone())?;
        assert_eq!(path_strings(one.paths()), ["Config.json"]);

        let error = RepositoryFilter::new(None, &[])
            .select(colliding)
            .expect_err("a selected portable collision was accepted");
        assert!(error.is_unsafe_path());

        Ok(())
    }

    #[test]
    fn selected_paths_are_sorted_and_identity_depends_only_on_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let files = paths(&["z/model.bin", "a/config.json", "m/tokenizer.json"])?;
        let first = RepositoryFilter::new(Some(&["*.json", "a/*", "*.json"]), &["nothing-*"])
            .select(files.clone())?;
        let second = RepositoryFilter::new(Some(&["m/*", "a/*"]), &[]).select(files)?;

        assert_eq!(
            path_strings(first.paths()),
            ["a/config.json", "m/tokenizer.json"]
        );
        assert_eq!(first.paths(), second.paths());
        assert_eq!(first.selection_id(), second.selection_id());

        Ok(())
    }

    #[test]
    fn matcher_work_is_polynomial_for_adversarial_star_suffixes() {
        let pattern = format!("{}b", "*a".repeat(512));
        let path = "a".repeat(512);

        assert!(!Pattern::compile(&pattern).matches(&path));
    }

    fn paths(values: &[&str]) -> Result<Vec<RepoPath>, ValidationError> {
        values.iter().map(RepoPath::parse).collect()
    }

    fn assert_selected(
        paths: &[RepoPath],
        allow_patterns: Option<&[&str]>,
        ignore_patterns: &[&str],
        expected: &[&str],
    ) -> Result<(), ValidationError> {
        let filter = RepositoryFilter::new(allow_patterns, ignore_patterns);
        let selected = filter.select(paths.iter().cloned())?;
        assert_eq!(path_strings(selected.paths()), expected);
        Ok(())
    }

    fn path_strings(paths: &[RepoPath]) -> Vec<&str> {
        paths.iter().map(RepoPath::as_str).collect()
    }
}
