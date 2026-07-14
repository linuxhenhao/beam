//! Request / input parsing helpers for workflow resume.

#![allow(dead_code)]

use anyhow::{Context, Result};
use serde_json::Value;

use crate::{AttemptResumeRequest, FeishuResumeInput};

/// Parse a `FeishuResumeInput` from a raw JSON [`Value`] read from the effect
/// input sidecar on disk.
pub(crate) fn parse_feishu_resume_input(raw: &Value) -> Result<FeishuResumeInput> {
    serde_json::from_value::<FeishuResumeInput>(raw.clone())
        .context("invalid feishu-im effect input")
}

/// Parse an [`AttemptResumeRequest`] from the raw HTTP request body.
///
/// An empty body is accepted and deserialized as a request with no reason.
pub(crate) fn parse_attempt_resume_request_body(
    body: &[u8],
) -> Result<AttemptResumeRequest, (axum::http::StatusCode, String)> {
    use axum::http::StatusCode;
    if body.is_empty() {
        return Ok(AttemptResumeRequest { reason: None });
    }
    serde_json::from_slice(body).map_err(|_| (StatusCode::BAD_REQUEST, "bad_json".to_string()))
}
