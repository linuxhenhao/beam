//! Embedded monospace font so screenshot rendering is deterministic on
//! machines without fontconfig fonts (CI runners, minimal servers).
//! DejaVu Sans Mono is redistributable under the Bitstream Vera license.

use super::FontVec;

pub(crate) fn embedded_mono_font() -> Option<FontVec> {
    FontVec::try_from_vec(include_bytes!("../../assets/DejaVuSansMono.ttf").to_vec()).ok()
}
