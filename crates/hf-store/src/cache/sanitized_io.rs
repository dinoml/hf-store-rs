use std::io;

/// Retains stable I/O classification without retaining potentially secret text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SanitizedIo {
    kind: io::ErrorKind,
}

impl SanitizedIo {
    pub(super) fn new(source: &io::Error) -> Self {
        Self {
            kind: source.kind(),
        }
    }

    pub(super) const fn kind(self) -> io::ErrorKind {
        self.kind
    }
}
