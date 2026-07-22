use std::fmt::{self, Debug, Formatter};

use reqwest::header::{AUTHORIZATION, IF_RANGE, RANGE};

use crate::transport::{
    Transport, TransportBody, TransportError, TransportFuture, TransportHeaders, TransportMethod,
    TransportRequest, TransportResponse,
};

pub(crate) struct ReqwestTransport {
    client: reqwest::Client,
}

impl ReqwestTransport {
    pub(crate) fn build() -> Result<Self, TransportError> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .build()
            .map_err(|_source| TransportError::unavailable())?;
        Ok(Self { client })
    }
}

impl Debug for ReqwestTransport {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestTransport")
            .finish_non_exhaustive()
    }
}

impl Transport for ReqwestTransport {
    fn send(
        &self,
        request: TransportRequest,
    ) -> TransportFuture<'_, Result<TransportResponse, TransportError>> {
        Box::pin(async move {
            let method = match request.method() {
                TransportMethod::Get => reqwest::Method::GET,
                TransportMethod::Head => reqwest::Method::HEAD,
            };
            let mut builder = self.client.request(method, request.target().clone());
            if let Some(token) = request.authorization() {
                builder = builder.header(AUTHORIZATION, format!("Bearer {}", token.expose()));
            }
            if let Some(range) = request.range() {
                builder = builder.header(RANGE, range);
            }
            if let Some(if_range) = request.if_range() {
                builder = builder.header(IF_RANGE, if_range);
            }
            let response = builder
                .send()
                .await
                .map_err(|_source| TransportError::connection())?;
            let status = response.status().as_u16();
            let mut selected = Vec::new();
            for name in [
                "location",
                "link",
                "content-length",
                "content-range",
                "etag",
                "last-modified",
                "retry-after",
                "accept-ranges",
            ] {
                if let Some(value) = response.headers().get(name) {
                    let value = value
                        .to_str()
                        .map_err(|_source| TransportError::protocol())?;
                    selected.push((name, value));
                }
            }
            let headers = TransportHeaders::new(selected)?;
            TransportResponse::new(status, headers, Box::new(ReqwestBody(response)))
        })
    }
}

struct ReqwestBody(reqwest::Response);

impl Debug for ReqwestBody {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReqwestBody")
            .finish_non_exhaustive()
    }
}

impl TransportBody for ReqwestBody {
    fn next_chunk(&mut self) -> TransportFuture<'_, Result<Option<Box<[u8]>>, TransportError>> {
        Box::pin(async move {
            self.0
                .chunk()
                .await
                .map(|chunk| chunk.map(|bytes| Box::<[u8]>::from(bytes.as_ref())))
                .map_err(|_source| TransportError::body())
        })
    }
}
