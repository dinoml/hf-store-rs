use crate::error::HubOperationError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyDisposition {
    Fresh {
        expected_body_bytes: u64,
    },
    Resume {
        offset: u64,
        expected_body_bytes: u64,
    },
    Restart {
        expected_body_bytes: u64,
    },
}

pub(crate) fn validate_file_response(
    status: u16,
    content_length: Option<&str>,
    content_range: Option<&str>,
    requested_offset: Option<u64>,
    expected_size: u64,
) -> Result<BodyDisposition, HubOperationError> {
    let content_length = content_length
        .map(parse_decimal)
        .transpose()?
        .map(|value| value.0);
    match requested_offset {
        None => {
            if status != 200 || content_range.is_some() {
                return Err(HubOperationError::protocol());
            }
            require_matching_length(content_length, expected_size)?;
            Ok(BodyDisposition::Fresh {
                expected_body_bytes: expected_size,
            })
        }
        Some(offset) => {
            if offset == 0 || offset >= expected_size {
                return Err(HubOperationError::protocol());
            }
            match status {
                200 => {
                    if content_range.is_some() {
                        return Err(HubOperationError::protocol());
                    }
                    require_matching_length(content_length, expected_size)?;
                    Ok(BodyDisposition::Restart {
                        expected_body_bytes: expected_size,
                    })
                }
                206 => {
                    let range = content_range
                        .ok_or_else(HubOperationError::protocol)
                        .and_then(parse_content_range)?;
                    let expected_body_bytes = expected_size
                        .checked_sub(offset)
                        .ok_or_else(HubOperationError::protocol)?;
                    if range.start != offset
                        || range.end != expected_size - 1
                        || range.total != expected_size
                    {
                        return Err(HubOperationError::protocol());
                    }
                    require_matching_length(content_length, expected_body_bytes)?;
                    Ok(BodyDisposition::Resume {
                        offset,
                        expected_body_bytes,
                    })
                }
                _ => Err(HubOperationError::protocol()),
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: u64,
}

fn parse_content_range(value: &str) -> Result<ContentRange, HubOperationError> {
    let value = value
        .strip_prefix("bytes ")
        .ok_or_else(HubOperationError::protocol)?;
    let (bounds, total) = value
        .split_once('/')
        .ok_or_else(HubOperationError::protocol)?;
    let (start, end) = bounds
        .split_once('-')
        .ok_or_else(HubOperationError::protocol)?;
    let start = parse_decimal(start)?.0;
    let end = parse_decimal(end)?.0;
    let total = parse_decimal(total)?.0;
    if start > end || end >= total {
        return Err(HubOperationError::protocol());
    }
    Ok(ContentRange { start, end, total })
}

struct Decimal(u64);

fn parse_decimal(value: &str) -> Result<Decimal, HubOperationError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(HubOperationError::protocol());
    }
    value
        .parse::<u64>()
        .map(Decimal)
        .map_err(|_source| HubOperationError::protocol())
}

fn require_matching_length(actual: Option<u64>, expected: u64) -> Result<(), HubOperationError> {
    if actual.is_some_and(|actual| actual != expected) {
        return Err(HubOperationError::protocol());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_and_exact_partial_responses_are_accepted() -> Result<(), HubOperationError> {
        assert_eq!(
            validate_file_response(200, Some("9"), None, None, 9)?,
            BodyDisposition::Fresh {
                expected_body_bytes: 9
            }
        );
        assert_eq!(
            validate_file_response(206, Some("5"), Some("bytes 4-8/9"), Some(4), 9)?,
            BodyDisposition::Resume {
                offset: 4,
                expected_body_bytes: 5
            }
        );
        Ok(())
    }

    #[test]
    fn a_full_response_to_a_range_request_requires_a_safe_restart() -> Result<(), HubOperationError>
    {
        assert_eq!(
            validate_file_response(200, Some("9"), None, Some(4), 9)?,
            BodyDisposition::Restart {
                expected_body_bytes: 9
            }
        );
        Ok(())
    }

    #[test]
    fn malformed_mismatched_and_overrunning_ranges_are_rejected() {
        let cases = [
            (206, Some("5"), None, Some(4), 9),
            (206, Some("5"), Some("items 4-8/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 3-8/9"), Some(4), 9),
            (206, Some("4"), Some("bytes 4-8/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 4-8/10"), Some(4), 9),
            (206, Some("4"), Some("bytes 4-7/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 8-4/9"), Some(4), 9),
            (206, Some("5"), Some("bytes 4-9/9"), Some(4), 9),
            (206, Some("5x"), Some("bytes 4-8/9"), Some(4), 9),
            (200, Some("8"), None, Some(4), 9),
            (200, Some("9"), Some("bytes 0-8/9"), Some(4), 9),
            (206, Some("9"), Some("bytes 0-8/9"), None, 9),
        ];
        for (status, length, range, offset, expected) in cases {
            assert!(
                validate_file_response(status, length, range, offset, expected)
                    .expect_err("accepted an invalid file response")
                    .is_protocol()
            );
        }
    }

    #[test]
    fn invalid_resume_offsets_and_decimal_overflow_are_rejected() {
        for offset in [0, 9, 10] {
            validate_file_response(206, None, Some("bytes 1-8/9"), Some(offset), 9)
                .expect_err("accepted an invalid resume offset");
        }
        validate_file_response(200, Some("18446744073709551616"), None, None, 9)
            .expect_err("accepted an overflowing decimal header");
    }
}
