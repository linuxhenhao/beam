//! Parse terminal SGR sequences into per-character fg/bg for screenshot rendering.

use image::{ImageBuffer, Rgba};

pub(crate) const BG_COLOR: Rgba<u8> = Rgba([26, 27, 38, 255]);
pub(crate) const FG_COLOR: Rgba<u8> = Rgba([169, 177, 214, 255]);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StyledCell {
    pub ch: char,
    pub fg: Rgba<u8>,
    pub bg: Rgba<u8>,
    pub underline: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct Sgr {
    fg: Option<[u8; 3]>,
    bg: Option<[u8; 3]>,
    bold: bool,
    dim: bool,
    reverse: bool,
    underline: bool,
}

impl Sgr {
    fn apply(&mut self, params: &[u16]) {
        if params.is_empty() {
            *self = Self::default();
            return;
        }
        let mut index = 0;
        while index < params.len() {
            let code = params[index];
            match code {
                0 => *self = Self::default(),
                1 => self.bold = true,
                2 => self.dim = true,
                4 => self.underline = true,
                7 => self.reverse = true,
                22 => {
                    self.bold = false;
                    self.dim = false;
                }
                24 => self.underline = false,
                27 => self.reverse = false,
                30..=37 => self.fg = Some(ansi_16((code - 30) as u8)),
                40..=47 => self.bg = Some(ansi_16((code - 40) as u8)),
                90..=97 => self.fg = Some(ansi_16((code - 90 + 8) as u8)),
                100..=107 => self.bg = Some(ansi_16((code - 100 + 8) as u8)),
                39 => self.fg = None,
                49 => self.bg = None,
                38 | 48 => {
                    let is_fg = code == 38;
                    if let Some((color, consumed)) = parse_extended_color(&params[index + 1..]) {
                        if is_fg {
                            self.fg = Some(color);
                        } else {
                            self.bg = Some(color);
                        }
                        index += consumed;
                    }
                }
                _ => {}
            }
            index += 1;
        }
    }

    fn cell(&self, ch: char) -> StyledCell {
        let mut fg = self.fg.map(rgba).unwrap_or(FG_COLOR);
        let mut bg = self.bg.map(rgba).unwrap_or(BG_COLOR);
        if self.bold {
            fg = brighten(fg);
        }
        if self.dim {
            fg = mix(fg, bg, 0.45);
        }
        if self.reverse {
            std::mem::swap(&mut fg, &mut bg);
        }
        StyledCell {
            ch,
            fg,
            bg,
            underline: self.underline,
        }
    }
}

fn rgba(rgb: [u8; 3]) -> Rgba<u8> {
    Rgba([rgb[0], rgb[1], rgb[2], 255])
}

fn brighten(color: Rgba<u8>) -> Rgba<u8> {
    Rgba([
        color.0[0].saturating_add(40),
        color.0[1].saturating_add(40),
        color.0[2].saturating_add(40),
        255,
    ])
}

fn mix(fg: Rgba<u8>, bg: Rgba<u8>, keep: f32) -> Rgba<u8> {
    let rest = 1.0 - keep;
    Rgba([
        (fg.0[0] as f32 * keep + bg.0[0] as f32 * rest) as u8,
        (fg.0[1] as f32 * keep + bg.0[1] as f32 * rest) as u8,
        (fg.0[2] as f32 * keep + bg.0[2] as f32 * rest) as u8,
        255,
    ])
}

/// Tokyo Night-ish 16-color table, matched to the screenshot canvas.
fn ansi_16(index: u8) -> [u8; 3] {
    const TABLE: [[u8; 3]; 16] = [
        [36, 40, 59],
        [247, 118, 142],
        [158, 206, 106],
        [224, 175, 104],
        [122, 162, 247],
        [187, 154, 247],
        [125, 207, 255],
        [169, 177, 214],
        [86, 95, 137],
        [255, 137, 157],
        [173, 218, 119],
        [244, 196, 117],
        [158, 188, 255],
        [207, 177, 255],
        [157, 224, 255],
        [192, 202, 245],
    ];
    TABLE[index.min(15) as usize]
}

fn ansi_256(index: u8) -> [u8; 3] {
    if index < 16 {
        return ansi_16(index);
    }
    if index >= 232 {
        let value = 8 + (index - 232) * 10;
        return [value, value, value];
    }
    let cube = index - 16;
    let level = |part: u8| -> u8 { if part == 0 { 0 } else { 55 + 40 * part } };
    [level(cube / 36), level((cube / 6) % 6), level(cube % 6)]
}

fn parse_extended_color(params: &[u16]) -> Option<([u8; 3], usize)> {
    match params.first().copied()? {
        5 => {
            let index = *params.get(1)? as u8;
            Some((ansi_256(index), 2))
        }
        2 => {
            let red = *params.get(1)? as u8;
            let green = *params.get(2)? as u8;
            let blue = *params.get(3)? as u8;
            Some(([red, green, blue], 4))
        }
        _ => None,
    }
}

/// Layout the visible screen, applying SGR and dropping other CSI/OSC.
pub(crate) fn parse_ansi_screen(input: &str) -> Vec<Vec<StyledCell>> {
    let mut rows: Vec<Vec<StyledCell>> = vec![Vec::new()];
    let mut sgr = Sgr::default();
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' {
            match chars.peek().copied() {
                Some('[') => {
                    chars.next();
                    let (params, final_byte) = take_csi(&mut chars);
                    if final_byte == Some('m') {
                        sgr.apply(&params);
                    }
                }
                Some(']') => {
                    chars.next();
                    skip_osc(&mut chars);
                }
                Some(_) => {
                    chars.next();
                }
                None => {}
            }
            continue;
        }
        if ch == '\n' {
            rows.push(Vec::new());
            continue;
        }
        if ch == '\r' {
            continue;
        }
        if let Some(row) = rows.last_mut() {
            row.push(sgr.cell(ch));
        }
    }
    if rows.last().is_some_and(|row| row.is_empty()) && rows.len() > 1 {
        rows.pop();
    }
    if rows.is_empty() {
        rows.push(Vec::new());
    }
    rows
}

fn take_csi(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> (Vec<u16>, Option<char>) {
    let mut raw = String::new();
    let mut final_byte = None;
    while let Some(&next) = chars.peek() {
        let code = next as u32;
        if (0x30..=0x3F).contains(&code) || (0x20..=0x2F).contains(&code) {
            raw.push(next);
            chars.next();
            continue;
        }
        if (0x40..=0x7E).contains(&code) {
            final_byte = Some(next);
            chars.next();
        }
        break;
    }
    let params = raw
        .split(';')
        .filter_map(|part| {
            let digits: String = part.chars().filter(|ch| ch.is_ascii_digit()).collect();
            if digits.is_empty() {
                Some(0)
            } else {
                digits.parse().ok()
            }
        })
        .collect();
    (params, final_byte)
}

pub(crate) struct GlyphPaint<'a> {
    pub image: &'a mut ImageBuffer<Rgba<u8>, Vec<u8>>,
    pub x: f32,
    pub y: f32,
    pub cell_w: f32,
    pub cell_h: f32,
    pub width: u32,
    pub height: u32,
}

impl GlyphPaint<'_> {
    pub(crate) fn fill_bg(&mut self, bg: Rgba<u8>) {
        if bg == BG_COLOR {
            return;
        }
        let x0 = self.x.max(0.0) as u32;
        let y0 = self.y.max(0.0) as u32;
        let x1 = ((self.x + self.cell_w).ceil() as u32).min(self.width);
        let y1 = ((self.y + self.cell_h).ceil() as u32).min(self.height);
        for py in y0..y1 {
            for px in x0..x1 {
                self.image.put_pixel(px, py, bg);
            }
        }
    }

    pub(crate) fn underline(&mut self, fg: Rgba<u8>) {
        let x0 = self.x.max(0.0) as u32;
        let x1 = ((self.x + self.cell_w).ceil() as u32).min(self.width);
        let py = ((self.y + self.cell_h - 2.0).round() as i32)
            .clamp(0, self.height.saturating_sub(1) as i32) as u32;
        for px in x0..x1 {
            self.image.put_pixel(px, py, fg);
        }
    }
}

fn skip_osc(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    while let Some(ch) = chars.next() {
        if ch == '\x07' {
            break;
        }
        if ch == '\x1b' && chars.peek() == Some(&'\\') {
            chars.next();
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_uses_default_colors() {
        let rows = parse_ansi_screen("ab");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].ch, 'a');
        assert_eq!(rows[0][0].fg, FG_COLOR);
        assert_eq!(rows[0][0].bg, BG_COLOR);
        assert_eq!(rows[0][1].ch, 'b');
    }

    #[test]
    fn sgr_red_then_reset() {
        let rows = parse_ansi_screen("\x1b[31mA\x1b[0mB");
        assert_eq!(rows[0][0].ch, 'A');
        assert_eq!(rows[0][0].fg, rgba(ansi_16(1)));
        assert_eq!(rows[0][1].ch, 'B');
        assert_eq!(rows[0][1].fg, FG_COLOR);
    }

    #[test]
    fn truecolor_and_256_and_dim() {
        let rows = parse_ansi_screen("\x1b[38;2;10;20;30mC\x1b[38;5;196mD\x1b[2mE");
        assert_eq!(rows[0][0].fg, Rgba([10, 20, 30, 255]));
        assert_eq!(rows[0][1].fg, rgba(ansi_256(196)));
        assert_ne!(rows[0][2].fg, rows[0][1].fg);
    }

    #[test]
    fn strips_non_sgr_and_cr() {
        let rows = parse_ansi_screen("\x1b[?25l\x1b[H\x1b[2Ja\r\nb");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0].ch, 'a');
        assert_eq!(rows[1][0].ch, 'b');
    }

    #[test]
    fn reverse_swaps_colors() {
        let rows = parse_ansi_screen("\x1b[31;42;7mX");
        assert_eq!(rows[0][0].fg, rgba(ansi_16(2)));
        assert_eq!(rows[0][0].bg, rgba(ansi_16(1)));
    }

    #[test]
    fn screenshot_visual_hash_differs_when_sgr_color_differs() {
        let plain =
            crate::worker_runtime::screenshot::screenshot_visual_hash("hello").expect("plain");
        let red = crate::worker_runtime::screenshot::screenshot_visual_hash("\x1b[31mhello\x1b[0m")
            .expect("red");
        let reset_only = crate::worker_runtime::screenshot::screenshot_visual_hash("\x1b[0mhello")
            .expect("reset");
        assert_ne!(plain, red, "SGR fg color must change the rendered PNG");
        assert_eq!(plain, reset_only, "bare reset should match default colors");
    }

    #[test]
    fn render_text_screenshot_png_paints_sgr_foreground() {
        let png = crate::worker_runtime::screenshot::render_text_screenshot_png(
            "\x1b[38;2;255;0;0mX\x1b[0m",
        )
        .expect("png");
        let image = image::load_from_memory(&png).expect("decode").to_rgba8();
        let has_red = image
            .pixels()
            .any(|px| px.0[0] > 180 && px.0[1] < 80 && px.0[2] < 80);
        assert!(has_red, "truecolor red SGR should appear in the PNG");
    }
}
