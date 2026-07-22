use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_REQUEST_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RecordedRequest {
    method: Box<str>,
    target: Box<str>,
    headers: BTreeMap<Box<str>, Box<str>>,
}

impl RecordedRequest {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn target(&self) -> &str {
        &self.target
    }

    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(name.to_ascii_lowercase().as_str())
            .map(AsRef::as_ref)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ExpectedRequest {
    method: Box<str>,
    target: Box<str>,
    headers: BTreeMap<Box<str>, Box<str>>,
    absent_headers: Box<[Box<str>]>,
}

impl ExpectedRequest {
    pub(crate) fn get(target: &str) -> Self {
        Self {
            method: "GET".into(),
            target: target.into(),
            headers: BTreeMap::new(),
            absent_headers: Box::new([]),
        }
    }

    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .insert(name.to_ascii_lowercase().into(), value.into());
        self
    }

    pub(crate) fn absent_header(mut self, name: &str) -> Self {
        let mut headers = self.absent_headers.into_vec();
        headers.push(name.to_ascii_lowercase().into());
        self.absent_headers = headers.into_boxed_slice();
        self
    }

    fn matches(&self, request: &RecordedRequest) -> bool {
        self.method.as_ref() == request.method()
            && self.target.as_ref() == request.target()
            && self
                .headers
                .iter()
                .all(|(name, value)| request.header(name) == Some(value))
            && self
                .absent_headers
                .iter()
                .all(|name| request.header(name).is_none())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScriptedResponse {
    status: u16,
    headers: Box<[(Box<str>, Box<str>)]>,
    body: Box<[u8]>,
    disconnect_after: Option<usize>,
}

impl ScriptedResponse {
    pub(crate) fn new(status: u16, body: impl Into<Box<[u8]>>) -> Self {
        Self {
            status,
            headers: Box::new([]),
            body: body.into(),
            disconnect_after: None,
        }
    }

    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        let mut headers = self.headers.into_vec();
        headers.push((name.into(), value.into()));
        self.headers = headers.into_boxed_slice();
        self
    }

    pub(crate) const fn disconnect_after(mut self, bytes: usize) -> Self {
        self.disconnect_after = Some(bytes);
        self
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Exchange {
    request: ExpectedRequest,
    response: ScriptedResponse,
}

impl Exchange {
    pub(crate) const fn new(request: ExpectedRequest, response: ScriptedResponse) -> Self {
        Self { request, response }
    }
}

pub(crate) struct ScriptedHub {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    failure: Arc<Mutex<Option<Box<str>>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl ScriptedHub {
    pub(crate) fn start(exchanges: impl IntoIterator<Item = Exchange>) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let failure = Arc::new(Mutex::new(None));
        let stop = Arc::new(AtomicBool::new(false));
        let worker_requests = Arc::clone(&requests);
        let worker_failure = Arc::clone(&failure);
        let worker_stop = Arc::clone(&stop);
        let mut script = exchanges.into_iter().collect::<VecDeque<_>>();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::Acquire) && !script.is_empty() {
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        let Some(exchange) = script.pop_front() else {
                            break;
                        };
                        if let Err(source) =
                            handle_connection(stream, &exchange, worker_requests.as_ref())
                        {
                            record_failure(&worker_failure, source.to_string());
                            break;
                        }
                    }
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                    }
                    Err(source) => {
                        record_failure(&worker_failure, source.to_string());
                        break;
                    }
                }
            }
            if !script.is_empty() && !worker_stop.load(Ordering::Acquire) {
                record_failure(
                    &worker_failure,
                    "fixture stopped before its script completed",
                );
            }
        });
        Ok(Self {
            address,
            requests,
            failure,
            stop,
            worker: Some(worker),
        })
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) fn finish(mut self) -> io::Result<Vec<RecordedRequest>> {
        self.join()?;
        let requests = self
            .requests
            .lock()
            .map_err(|_poisoned| io::Error::other("fixture request log lock poisoned"))?
            .clone();
        Ok(requests)
    }

    fn join(&mut self) -> io::Result<()> {
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_panic| io::Error::other("fixture worker panicked"))?;
        }
        if let Some(message) = self
            .failure
            .lock()
            .map_err(|_poisoned| io::Error::other("fixture failure lock poisoned"))?
            .take()
        {
            return Err(io::Error::other(message.to_string()));
        }
        Ok(())
    }
}

impl Debug for ScriptedHub {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScriptedHub")
            .finish_non_exhaustive()
    }
}

impl Drop for ScriptedHub {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _join_result = self.join();
    }
}

fn handle_connection(
    mut stream: TcpStream,
    exchange: &Exchange,
    requests: &Mutex<Vec<RecordedRequest>>,
) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let request = read_request(&mut stream)?;
    if !exchange.request.matches(&request) {
        return Err(io::Error::other("fixture received an unexpected request"));
    }
    requests
        .lock()
        .map_err(|_poisoned| io::Error::other("fixture request log lock poisoned"))?
        .push(request);
    match write_response(&mut stream, &exchange.response) {
        Err(source)
            if exchange.response.disconnect_after.is_some()
                && expected_disconnect_error(source.kind()) =>
        {
            Ok(())
        }
        result => result,
    }
}

fn read_request(stream: &mut TcpStream) -> io::Result<RecordedRequest> {
    let mut bytes = Vec::new();
    let mut buffer = [0_u8; 1024];
    while !bytes.ends_with(b"\r\n\r\n") {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "request headers ended early",
            ));
        }
        bytes.extend_from_slice(&buffer[..read]);
        if bytes.len() > MAX_REQUEST_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers exceed fixture limit",
            ));
        }
    }
    let text = std::str::from_utf8(&bytes)
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
    let mut lines = text.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| io::Error::other("request line is missing"))?;
    let mut parts = request_line.split(' ');
    let method = parts
        .next()
        .ok_or_else(|| io::Error::other("request method is missing"))?;
    let target = parts
        .next()
        .ok_or_else(|| io::Error::other("request target is missing"))?;
    if parts.next() != Some("HTTP/1.1") || parts.next().is_some() {
        return Err(io::Error::other("fixture requires HTTP/1.1 requests"));
    }
    let mut headers = BTreeMap::new();
    for line in lines.take_while(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| io::Error::other("request header is malformed"))?;
        if headers
            .insert(name.to_ascii_lowercase().into(), value.trim_ascii().into())
            .is_some()
        {
            return Err(io::Error::other("duplicate request header"));
        }
    }
    Ok(RecordedRequest {
        method: method.into(),
        target: target.into(),
        headers,
    })
}

fn write_response(stream: &mut TcpStream, response: &ScriptedResponse) -> io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        206 => "Partial Content",
        302 => "Found",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Fixture Status",
    };
    write!(stream, "HTTP/1.1 {} {reason}\r\n", response.status)?;
    let has_length = response
        .headers
        .iter()
        .any(|(name, _value)| name.eq_ignore_ascii_case("content-length"));
    for (name, value) in &response.headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    if !has_length {
        write!(stream, "Content-Length: {}\r\n", response.body.len())?;
    }
    write!(stream, "Connection: close\r\n\r\n")?;
    let visible = response
        .disconnect_after
        .unwrap_or(response.body.len())
        .min(response.body.len());
    stream.write_all(&response.body[..visible])?;
    stream.flush()?;
    if response.disconnect_after.is_some() {
        if let Err(source) = stream.shutdown(Shutdown::Write) {
            if !expected_disconnect_error(source.kind()) {
                return Err(source);
            }
        }
        stream.set_read_timeout(Some(Duration::from_secs(1)))?;
        let mut drain = [0_u8; 64];
        loop {
            match stream.read(&mut drain) {
                Ok(0) => break,
                Ok(_) => {}
                Err(source)
                    if expected_disconnect_error(source.kind())
                        || matches!(
                            source.kind(),
                            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
                        ) =>
                {
                    break;
                }
                Err(source) => return Err(source),
            }
        }
    }
    Ok(())
}

fn expected_disconnect_error(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
    )
}

fn record_failure(failure: &Mutex<Option<Box<str>>>, message: impl Into<Box<str>>) {
    if let Ok(mut slot) = failure.lock() {
        if slot.is_none() {
            *slot = Some(message.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct RawResponse {
        status: u16,
        headers: BTreeMap<String, String>,
        body: Vec<u8>,
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the single scenario intentionally inventories every required fixture behavior"
    )]
    fn fixture_covers_hub_planning_transfer_and_failure_shapes() -> io::Result<()> {
        let exchanges = vec![
            Exchange::new(
                ExpectedRequest::get("/api/models/org/repo/revision/main")
                    .header("authorization", "Bearer fixture-token"),
                ScriptedResponse::new(
                    200,
                    br#"{"sha":"0123456789abcdef0123456789abcdef01234567"}"#.as_slice(),
                ),
            ),
            Exchange::new(
                ExpectedRequest::get("/api/models/org/repo/tree/commit?cursor=first"),
                ScriptedResponse::new(200, br#"[{"path":"config.json"}]"#.as_slice()).header(
                    "Link",
                    "</api/models/org/repo/tree/commit?cursor=last>; rel=next",
                ),
            ),
            Exchange::new(
                ExpectedRequest::get("/api/models/org/repo/tree/commit?cursor=last"),
                ScriptedResponse::new(200, br#"[{"path":"model.bin"}]"#.as_slice()),
            ),
            Exchange::new(
                ExpectedRequest::get("/org/repo/resolve/commit/model.bin")
                    .header("range", "bytes=4-")
                    .header("if-range", "fixture-etag"),
                ScriptedResponse::new(206, b"56789".as_slice())
                    .header("Content-Range", "bytes 4-8/9")
                    .header("ETag", "fixture-etag"),
            ),
            Exchange::new(
                ExpectedRequest::get("/redirect").header("authorization", "Bearer fixture-token"),
                ScriptedResponse::new(302, Box::<[u8]>::default())
                    .header("Location", "/download?signed=secret"),
            ),
            Exchange::new(
                ExpectedRequest::get("/download?signed=secret").absent_header("authorization"),
                ScriptedResponse::new(200, b"download".as_slice()),
            ),
            Exchange::new(
                ExpectedRequest::get("/gated"),
                ScriptedResponse::new(403, b"gated".as_slice()),
            ),
            Exchange::new(
                ExpectedRequest::get("/missing"),
                ScriptedResponse::new(404, b"missing".as_slice()),
            ),
            Exchange::new(
                ExpectedRequest::get("/retry"),
                ScriptedResponse::new(503, b"retry".as_slice()).header("Retry-After", "1"),
            ),
            Exchange::new(
                ExpectedRequest::get("/retry"),
                ScriptedResponse::new(200, b"recovered".as_slice()),
            ),
            Exchange::new(
                ExpectedRequest::get("/disconnect"),
                ScriptedResponse::new(200, b"incomplete".as_slice())
                    .header("Content-Length", "10")
                    .disconnect_after(3),
            ),
        ];
        let fixture = ScriptedHub::start(exchanges)?;
        let address = fixture.address();
        assert!(fixture.endpoint().starts_with("http://127.0.0.1:"));

        assert_eq!(
            request(
                address,
                "/api/models/org/repo/revision/main",
                &[("Authorization", "Bearer fixture-token")]
            )?
            .status,
            200
        );
        let first_tree = request(
            address,
            "/api/models/org/repo/tree/commit?cursor=first",
            &[],
        )?;
        assert!(first_tree.headers.contains_key("link"));
        assert_eq!(
            request(address, "/api/models/org/repo/tree/commit?cursor=last", &[])?.status,
            200
        );
        let range = request(
            address,
            "/org/repo/resolve/commit/model.bin",
            &[("Range", "bytes=4-"), ("If-Range", "fixture-etag")],
        )?;
        assert_eq!(range.status, 206);
        assert_eq!(
            range.headers.get("content-range").map(String::as_str),
            Some("bytes 4-8/9")
        );
        assert_eq!(
            request(
                address,
                "/redirect",
                &[("Authorization", "Bearer fixture-token")]
            )?
            .status,
            302
        );
        assert_eq!(
            request(address, "/download?signed=secret", &[])?.body,
            b"download"
        );
        assert_eq!(request(address, "/gated", &[])?.status, 403);
        assert_eq!(request(address, "/missing", &[])?.status, 404);
        assert_eq!(request(address, "/retry", &[])?.status, 503);
        assert_eq!(request(address, "/retry", &[])?.body, b"recovered");
        let disconnected = request(address, "/disconnect", &[])?;
        assert_eq!(
            disconnected
                .headers
                .get("content-length")
                .map(String::as_str),
            Some("10")
        );
        assert_eq!(disconnected.body, b"inc");

        let requests = fixture.finish()?;
        assert_eq!(requests.len(), 11);
        Ok(())
    }

    fn request(
        address: SocketAddr,
        target: &str,
        headers: &[(&str, &str)],
    ) -> io::Result<RawResponse> {
        let mut stream = TcpStream::connect(address)?;
        write!(stream, "GET {target} HTTP/1.1\r\nHost: {address}\r\n")?;
        for (name, value) in headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
        write!(stream, "Connection: close\r\n\r\n")?;
        stream.flush()?;
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => bytes.extend_from_slice(&buffer[..read]),
                Err(source)
                    if !bytes.is_empty()
                        && matches!(
                            source.kind(),
                            io::ErrorKind::BrokenPipe
                                | io::ErrorKind::ConnectionAborted
                                | io::ErrorKind::ConnectionReset
                                | io::ErrorKind::UnexpectedEof
                        ) =>
                {
                    break;
                }
                Err(source) => return Err(source),
            }
        }
        let separator = bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .ok_or_else(|| io::Error::other("response headers are incomplete"))?;
        let head = std::str::from_utf8(&bytes[..separator])
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
        let mut lines = head.split("\r\n");
        let status = lines
            .next()
            .and_then(|line| line.split(' ').nth(1))
            .ok_or_else(|| io::Error::other("response status is missing"))?
            .parse::<u16>()
            .map_err(|source| io::Error::new(io::ErrorKind::InvalidData, source))?;
        let mut response_headers = BTreeMap::new();
        for line in lines {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| io::Error::other("response header is malformed"))?;
            response_headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
        }
        Ok(RawResponse {
            status,
            headers: response_headers,
            body: bytes[separator + 4..].to_vec(),
        })
    }
}
