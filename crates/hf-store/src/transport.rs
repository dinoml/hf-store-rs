use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Debug, Display, Formatter};
use std::pin::Pin;

use url::Url;

use crate::AuthToken;

pub(crate) type TransportFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportMethod {
    Get,
    Head,
}

#[derive(Clone)]
pub(crate) struct TransportRequest {
    method: TransportMethod,
    target: Url,
    authorization: Option<AuthToken>,
    range: Option<Box<str>>,
    if_range: Option<Box<str>>,
}

impl TransportRequest {
    pub(crate) fn new(method: TransportMethod, target: Url) -> Result<Self, TransportError> {
        if !matches!(target.scheme(), "http" | "https")
            || target.host_str().is_none()
            || !target.username().is_empty()
            || target.password().is_some()
        {
            return Err(TransportError::protocol());
        }
        Ok(Self {
            method,
            target,
            authorization: None,
            range: None,
            if_range: None,
        })
    }

    pub(crate) const fn method(&self) -> TransportMethod {
        self.method
    }

    pub(crate) const fn target(&self) -> &Url {
        &self.target
    }

    pub(crate) const fn authorization(&self) -> Option<&AuthToken> {
        self.authorization.as_ref()
    }

    pub(crate) fn with_authorization(mut self, authorization: AuthToken) -> Self {
        self.authorization = Some(authorization);
        self
    }

    pub(crate) fn without_authorization(mut self) -> Self {
        self.authorization = None;
        self
    }

    pub(crate) fn with_range(
        mut self,
        range: impl Into<Box<str>>,
        if_range: Option<impl Into<Box<str>>>,
    ) -> Result<Self, TransportError> {
        let range = range.into();
        if !safe_header_value(&range) {
            return Err(TransportError::protocol());
        }
        let if_range = if_range.map(Into::into);
        if if_range
            .as_deref()
            .is_some_and(|value| !safe_header_value(value))
        {
            return Err(TransportError::protocol());
        }
        self.range = Some(range);
        self.if_range = if_range;
        Ok(self)
    }

    pub(crate) fn range(&self) -> Option<&str> {
        self.range.as_deref()
    }

    pub(crate) fn if_range(&self) -> Option<&str> {
        self.if_range.as_deref()
    }
}

impl Debug for TransportRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportRequest")
            .field("method", &self.method)
            .field("has_authorization", &self.authorization.is_some())
            .field("has_range", &self.range.is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Default, Eq, PartialEq)]
pub(crate) struct TransportHeaders(BTreeMap<Box<str>, Box<str>>);

impl TransportHeaders {
    pub(crate) fn new(
        headers: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
    ) -> Result<Self, TransportError> {
        let mut values = BTreeMap::new();
        for (name, value) in headers {
            let name = name.as_ref();
            let value = value.as_ref();
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
                || !safe_header_value(value)
            {
                return Err(TransportError::protocol());
            }
            if values
                .insert(name.to_ascii_lowercase().into(), value.into())
                .is_some()
            {
                return Err(TransportError::protocol());
            }
        }
        Ok(Self(values))
    }

    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.0
            .get(name.to_ascii_lowercase().as_str())
            .map(AsRef::as_ref)
    }
}

impl Debug for TransportHeaders {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportHeaders")
            .field("count", &self.0.len())
            .finish()
    }
}

pub(crate) trait TransportBody: Debug + Send {
    fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>>;
}

pub(crate) struct TransportResponse {
    status: u16,
    headers: TransportHeaders,
    body: Box<dyn TransportBody>,
}

impl TransportResponse {
    pub(crate) fn new(
        status: u16,
        headers: TransportHeaders,
        body: Box<dyn TransportBody>,
    ) -> Result<Self, TransportError> {
        if !(100..=599).contains(&status) {
            return Err(TransportError::protocol());
        }
        Ok(Self {
            status,
            headers,
            body,
        })
    }

    pub(crate) const fn status(&self) -> u16 {
        self.status
    }

    pub(crate) const fn headers(&self) -> &TransportHeaders {
        &self.headers
    }

    pub(crate) fn body_mut(&mut self) -> &mut dyn TransportBody {
        self.body.as_mut()
    }
}

impl Debug for TransportResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportResponse")
            .field("status", &self.status)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

pub(crate) trait Transport: Debug + Send + Sync {
    fn send(
        &self,
        request: TransportRequest,
    ) -> TransportFuture<'_, Result<TransportResponse, TransportError>>;
}

pub(crate) struct TransportError {
    kind: TransportErrorKind,
    backtrace: Backtrace,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TransportErrorKind {
    Unavailable,
    Connection,
    Protocol,
    Body,
}

impl TransportError {
    pub(crate) fn unavailable() -> Self {
        Self::new(TransportErrorKind::Unavailable)
    }

    pub(crate) fn connection() -> Self {
        Self::new(TransportErrorKind::Connection)
    }

    pub(crate) fn protocol() -> Self {
        Self::new(TransportErrorKind::Protocol)
    }

    pub(crate) fn body() -> Self {
        Self::new(TransportErrorKind::Body)
    }

    fn new(kind: TransportErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    pub(crate) fn is_unavailable(&self) -> bool {
        self.kind == TransportErrorKind::Unavailable
    }

    pub(crate) fn is_connection(&self) -> bool {
        self.kind == TransportErrorKind::Connection
    }

    pub(crate) fn is_protocol(&self) -> bool {
        self.kind == TransportErrorKind::Protocol
    }

    pub(crate) fn is_body(&self) -> bool {
        self.kind == TransportErrorKind::Body
    }

    pub(crate) const fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }
}

impl Debug for TransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TransportError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl Display for TransportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self.kind {
            TransportErrorKind::Unavailable => "network backend is unavailable",
            TransportErrorKind::Connection => "transport connection failed",
            TransportErrorKind::Protocol => "transport response violated the protocol",
            TransportErrorKind::Body => "transport response body failed",
        };
        formatter.write_str(message)
    }
}

impl Error for TransportError {}

fn safe_header_value(value: &str) -> bool {
    !value.is_empty()
        && !value
            .bytes()
            .any(|byte| matches!(byte, b'\r' | b'\n' | b'\0'))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::task::{Context, Poll, Waker};

    use super::*;

    const SECRET: &str = "hf_secret_transport_abstraction_sentinel";

    #[derive(Debug)]
    struct MemoryBody(Option<Box<[u8]>>);

    impl TransportBody for MemoryBody {
        fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
            Box::pin(std::future::ready(Ok(self.0.take())))
        }
    }

    #[derive(Debug, Default)]
    struct RecordingTransport(Mutex<Vec<TransportMethod>>);

    impl Transport for RecordingTransport {
        fn send(
            &self,
            request: TransportRequest,
        ) -> TransportFuture<'_, Result<TransportResponse, TransportError>> {
            let result = self
                .0
                .lock()
                .map_err(|_poisoned| TransportError::connection())
                .and_then(|mut methods| {
                    assert_eq!(request.authorization().map(AuthToken::expose), Some(SECRET));
                    assert_eq!(request.range(), Some("bytes=4-"));
                    assert_eq!(request.if_range(), Some("etag"));
                    methods.push(request.method());
                    TransportResponse::new(
                        206,
                        TransportHeaders::new([("content-range", "bytes 4-8/9")])?,
                        Box::new(MemoryBody(Some(Box::from(&b"56789"[..])))),
                    )
                });
            Box::pin(std::future::ready(result))
        }
    }

    #[test]
    fn transport_and_streaming_body_are_object_safe_and_client_independent()
    -> Result<(), Box<dyn Error>> {
        let transport: Box<dyn Transport> = Box::new(RecordingTransport::default());
        let request = TransportRequest::new(
            TransportMethod::Get,
            Url::parse("https://huggingface.co/model.bin?signed=secret")?,
        )?
        .with_authorization(AuthToken::new(SECRET)?)
        .with_range("bytes=4-", Some("etag"))?;
        let mut response = run_ready(transport.send(request))?;
        assert_eq!(response.status(), 206);
        assert_eq!(response.headers().get("content-range"), Some("bytes 4-8/9"));
        assert_eq!(
            run_ready(response.body_mut().next_chunk())?.as_deref(),
            Some(&b"56789"[..])
        );
        assert!(run_ready(response.body_mut().next_chunk())?.is_none());
        Ok(())
    }

    #[test]
    fn diagnostics_and_validation_never_expose_targets_headers_or_tokens()
    -> Result<(), Box<dyn Error>> {
        let request = TransportRequest::new(
            TransportMethod::Head,
            Url::parse(&format!("https://cdn.example/file?{SECRET}"))?,
        )?
        .with_authorization(AuthToken::new(SECRET)?);
        assert!(!format!("{request:?}").contains(SECRET));
        let headers = TransportHeaders::new([("x-signed-location", SECRET)])?;
        assert!(!format!("{headers:?}").contains(SECRET));
        assert!(
            TransportHeaders::new([("location", "ok\r\nAuthorization: secret")])
                .expect_err("accepted a header injection")
                .is_protocol()
        );
        assert!(
            TransportRequest::new(TransportMethod::Get, Url::parse("file:///secret")?)
                .expect_err("accepted a non-HTTP target")
                .is_protocol()
        );
        Ok(())
    }

    fn run_ready<T>(mut future: TransportFuture<'_, T>) -> T {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("test future unexpectedly remained pending"),
        }
    }
}
