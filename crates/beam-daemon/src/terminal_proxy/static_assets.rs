//! Built-in serving for the browser terminal page assets.
//!
//! The xterm.js assets are vendored into the binary via `include_bytes!` so
//! the terminal works offline with a single daemon binary and no CDN or
//! Node/TypeScript build chain. Assets are public and unauthenticated; the
//! session page itself still requires ticket/cookie auth.

use axum::{
    extract::Path,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

pub(crate) const INDEX_HTML: &str = include_str!("../../assets/terminal/index.html");
const APP_JS: &str = include_str!("../../assets/terminal/app.js");
const TERMINAL_CSS: &str = include_str!("../../assets/terminal/terminal.css");
const XTERM_JS: &[u8] = include_bytes!("../../assets/terminal/vendor/xterm.min.js");
const XTERM_CSS: &str = include_str!("../../assets/terminal/vendor/xterm.css");
const FIT_JS: &str = include_str!("../../assets/terminal/vendor/xterm-addon-fit.min.js");
const WEB_LINKS_JS: &str =
    include_str!("../../assets/terminal/vendor/xterm-addon-web-links.min.js");

fn asset_response(content_type: &'static str, body: impl Into<axum::body::Body>) -> Response {
    let mut response = Response::new(body.into());
    *response.status_mut() = StatusCode::OK;
    if let Ok(value) = content_type.parse() {
        response.headers_mut().insert(header::CONTENT_TYPE, value);
    }
    response
}

/// Serve a vendored terminal asset under `/terminal-static/{*path}`.
pub(crate) async fn handle_terminal_static(Path(path): Path<String>) -> Response {
    match path.as_str() {
        "index.html" => asset_response("text/html; charset=utf-8", INDEX_HTML),
        "app.js" => asset_response("text/javascript; charset=utf-8", APP_JS),
        "terminal.css" => asset_response("text/css; charset=utf-8", TERMINAL_CSS),
        "vendor/xterm.min.js" => asset_response("text/javascript; charset=utf-8", XTERM_JS),
        "vendor/xterm.css" => asset_response("text/css; charset=utf-8", XTERM_CSS),
        "vendor/xterm-addon-fit.min.js" => asset_response("text/javascript; charset=utf-8", FIT_JS),
        "vendor/xterm-addon-web-links.min.js" => {
            asset_response("text/javascript; charset=utf-8", WEB_LINKS_JS)
        }
        _ => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vendored_assets_are_present() {
        assert!(INDEX_HTML.contains("xterm.min.js"));
        assert!(APP_JS.contains("/ws/herdr"));
        assert!(TERMINAL_CSS.contains("terminal-container"));
        assert!(!XTERM_JS.is_empty());
        assert!(XTERM_CSS.contains(".xterm"));
        assert!(FIT_JS.contains("FitAddon"));
        assert!(WEB_LINKS_JS.contains("WebLinksAddon"));
    }

    #[tokio::test]
    async fn static_handler_serves_known_and_rejects_unknown() {
        let ok = handle_terminal_static(Path("app.js".to_string())).await;
        assert_eq!(ok.status(), StatusCode::OK);
        let missing = handle_terminal_static(Path("../secret".to_string())).await;
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}
