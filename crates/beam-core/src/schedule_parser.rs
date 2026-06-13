use chrono::{DateTime, NaiveDateTime, Utc};

use crate::schedule_store::{ParsedSchedule, ParsedScheduleKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedNaturalSchedule {
    pub parsed: ParsedSchedule,
    pub prompt: String,
    pub name: String,
}

fn cron_ps(expr: String, display: String) -> ParsedSchedule {
    ParsedSchedule {
        kind: ParsedScheduleKind::Cron,
        run_at: None,
        minutes: None,
        expr: Some(expr),
        display,
    }
}

fn parse_time_hm(input: &str) -> Option<(u32, u32, String)> {
    let s = input.trim_start();
    if let Some(idx) = s.find([':', '：']) {
        let hour = s[..idx].parse().ok()?;
        let mut end = idx + 1;
        for (offset, ch) in s[end..].char_indices() {
            if !ch.is_ascii_digit() {
                break;
            }
            end = idx + 1 + offset + ch.len_utf8();
        }
        let minute_str = &s[idx + 1..end];
        if minute_str.is_empty() {
            return None;
        }
        let minute = minute_str.parse().ok()?;
        return Some((hour, minute, s[end..].to_string()));
    }

    if let Some(idx) = s.find('点') {
        let hour = s[..idx].parse().ok()?;
        let mut start = idx + '点'.len_utf8();
        let mut minute_end = start;
        for (offset, ch) in s[start..].char_indices() {
            if !ch.is_ascii_digit() {
                break;
            }
            minute_end = start + offset + ch.len_utf8();
        }
        if minute_end > start {
            let minute = s[start..minute_end].parse().ok()?;
            start = minute_end;
            if s[start..].starts_with('分') {
                start += '分'.len_utf8();
            }
            return Some((hour, minute, s[start..].to_string()));
        }
        return Some((hour, 0, s[start..].to_string()));
    }

    None
}

fn parse_chinese_schedule(input: &str) -> Option<(ParsedSchedule, String)> {
    let s = input.trim();
    let norm = s.replace(' ', "");

    if let Some(rest) = norm.strip_prefix("每个工作日") {
        if let Some((hour, minute, tail)) = parse_time_hm(rest) {
            return Some((
                cron_ps(
                    format!("{minute} {hour} * * 1-5"),
                    format!("工作日 {hour}:{minute:02}"),
                ),
                tail,
            ));
        }
    }
    if let Some(rest) = norm.strip_prefix("工作日每天") {
        if let Some((hour, minute, tail)) = parse_time_hm(rest) {
            return Some((
                cron_ps(
                    format!("{minute} {hour} * * 1-5"),
                    format!("工作日 {hour}:{minute:02}"),
                ),
                tail,
            ));
        }
    }
    if let Some(rest) = norm
        .strip_prefix("每天")
        .or_else(|| norm.strip_prefix("每日"))
    {
        if let Some((hour, minute, tail)) = parse_time_hm(rest) {
            return Some((
                cron_ps(
                    format!("{minute} {hour} * * *"),
                    format!("每天 {hour}:{minute:02}"),
                ),
                tail,
            ));
        }
    }
    if let Some(rest) = norm.strip_prefix("每周") {
        let mut chars = rest.chars();
        if let Some(day) = chars.next() {
            let weekday = match day {
                '一' => 1,
                '二' => 2,
                '三' => 3,
                '四' => 4,
                '五' => 5,
                '六' => 6,
                '日' | '天' => 0,
                _ => return None,
            };
            let tail = chars.as_str();
            if let Some((hour, minute, tail2)) = parse_time_hm(tail) {
                return Some((
                    cron_ps(
                        format!("{minute} {hour} * * {weekday}"),
                        format!("每周{day} {hour}:{minute:02}"),
                    ),
                    tail2,
                ));
            }
        }
    }
    if let Some(rest) = norm.strip_prefix("每月") {
        let mut digits = String::new();
        let mut idx = 0usize;
        for ch in rest.chars() {
            if ch.is_ascii_digit() {
                digits.push(ch);
                idx += ch.len_utf8();
            } else {
                break;
            }
        }
        if !digits.is_empty() {
            let day: u32 = digits.parse().ok()?;
            let tail = &rest[idx..];
            let tail = tail
                .strip_prefix('号')
                .or_else(|| tail.strip_prefix('日'))?;
            if let Some((hour, minute, tail2)) = parse_time_hm(tail) {
                return Some((
                    cron_ps(
                        format!("{minute} {hour} {day} * *"),
                        format!("每月{day}号 {hour}:{minute:02}"),
                    ),
                    tail2,
                ));
            }
        }
    }
    if let Some(rest) = norm.strip_prefix("每小时") {
        return Some((
            cron_ps("0 * * * *".to_string(), "每小时".to_string()),
            rest.to_string(),
        ));
    }
    if let Some(rest) = norm.strip_prefix("每") {
        if let Some(idx) = rest.find('小') {
            let (n, tail) = rest.split_at(idx);
            let tail = tail.trim_start_matches("小时");
            if let Ok(hours) = n.parse::<u64>() {
                let expr = if hours == 1 {
                    "0 * * * *".to_string()
                } else {
                    format!("0 */{hours} * * *")
                };
                return Some((cron_ps(expr, format!("每 {hours} 小时")), tail.to_string()));
            }
        }
        if let Some(idx) = rest.find('分') {
            let (n, tail) = rest.split_at(idx);
            let tail = tail.trim_start_matches("分钟");
            if let Ok(minutes) = n.parse::<u64>() {
                return Some((
                    cron_ps(format!("*/{minutes} * * * *"), format!("每 {minutes} 分钟")),
                    tail.to_string(),
                ));
            }
        }
    }
    if let Some(rest) = norm.strip_suffix("分钟后") {
        if let Ok(minutes) = rest.parse::<u64>() {
            let run_at = (Utc::now() + chrono::Duration::minutes(minutes as i64)).to_rfc3339();
            return Some((
                ParsedSchedule {
                    kind: ParsedScheduleKind::Once,
                    run_at: Some(run_at),
                    minutes: None,
                    expr: None,
                    display: format!("{minutes} 分钟后"),
                },
                String::new(),
            ));
        }
    }
    if let Some(rest) = norm.strip_suffix("小时后") {
        if let Ok(hours) = rest.parse::<u64>() {
            let run_at = (Utc::now() + chrono::Duration::hours(hours as i64)).to_rfc3339();
            return Some((
                ParsedSchedule {
                    kind: ParsedScheduleKind::Once,
                    run_at: Some(run_at),
                    minutes: None,
                    expr: None,
                    display: format!("{hours} 小时后"),
                },
                String::new(),
            ));
        }
    }
    if let Some(rest) = norm.strip_prefix("明天") {
        if let Some((hour, minute, tail)) = parse_time_hm(rest) {
            let tomorrow = Utc::now().date_naive().succ_opt()?;
            let naive =
                NaiveDateTime::new(tomorrow, chrono::NaiveTime::from_hms_opt(hour, minute, 0)?);
            let run_at = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc).to_rfc3339();
            return Some((
                ParsedSchedule {
                    kind: ParsedScheduleKind::Once,
                    run_at: Some(run_at),
                    minutes: None,
                    expr: None,
                    display: format!("明天 {hour}:{minute:02}"),
                },
                tail,
            ));
        }
    }
    None
}

fn parse_duration(input: &str) -> Option<ParsedSchedule> {
    let s = input.trim();
    let split = s.split_whitespace().collect::<Vec<_>>();
    if split.len() == 1 {
        let token = split[0];
        let mut digits = String::new();
        let mut suffix = String::new();
        for ch in token.chars() {
            if ch.is_ascii_digit() {
                digits.push(ch);
            } else {
                suffix.push(ch);
            }
        }
        if digits.is_empty() || suffix.is_empty() {
            return None;
        }
        let minutes = duration_to_minutes(&digits, &suffix)?;
        let run_at = (Utc::now() + chrono::Duration::minutes(minutes as i64)).to_rfc3339();
        return Some(ParsedSchedule {
            kind: ParsedScheduleKind::Once,
            run_at: Some(run_at),
            minutes: None,
            expr: None,
            display: format!("once in {s}"),
        });
    }

    if split.len() >= 2 && split[0].eq_ignore_ascii_case("every") {
        let num = split[1];
        let unit = split.get(2).copied().unwrap_or("m");
        if let Some(minutes) = duration_to_minutes(num, unit) {
            return Some(ParsedSchedule {
                kind: ParsedScheduleKind::Interval,
                run_at: None,
                minutes: Some(minutes),
                expr: None,
                display: format!("every {minutes}m"),
            });
        }
    }
    None
}

fn duration_to_minutes(num_str: &str, unit: &str) -> Option<u64> {
    let n = num_str.parse::<u64>().ok()?;
    let u = unit.to_ascii_lowercase();
    let mult = match u.chars().next()? {
        'm' => 1,
        'h' => 60,
        'd' => 1440,
        _ => return None,
    };
    Some(n * mult)
}

fn parse_cron(input: &str) -> Option<ParsedSchedule> {
    let s = input.trim();
    let parts: Vec<_> = s.split_whitespace().collect();
    if parts.len() == 5
        && parts.iter().all(|p| {
            p.chars()
                .all(|c| c.is_ascii_digit() || matches!(c, '*' | '-' | ',' | '/'))
        })
    {
        return Some(cron_ps(s.to_string(), s.to_string()));
    }
    None
}

fn parse_iso(input: &str) -> Option<ParsedSchedule> {
    let s = input.trim();
    if !s.starts_with("20") && !s.starts_with("19") {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(ParsedSchedule {
            kind: ParsedScheduleKind::Once,
            run_at: Some(dt.with_timezone(&Utc).to_rfc3339()),
            minutes: None,
            expr: None,
            display: format!("once at {}", dt.format("%Y-%m-%d %H:%M:%S")),
        });
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M") {
        let dt = DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc);
        return Some(ParsedSchedule {
            kind: ParsedScheduleKind::Once,
            run_at: Some(dt.to_rfc3339()),
            minutes: None,
            expr: None,
            display: format!("once at {}", dt.format("%Y-%m-%d %H:%M:%S")),
        });
    }
    None
}

pub fn parse_schedule(input: &str) -> Result<ParsedSchedule, String> {
    let s = input.trim();
    if s.is_empty() {
        return Err("empty schedule".to_string());
    }
    if let Some((parsed, _rest)) = parse_chinese_schedule(s) {
        return Ok(parsed);
    }
    if let Some(parsed) = parse_duration(s) {
        return Ok(parsed);
    }
    if let Some(parsed) = parse_cron(s) {
        return Ok(parsed);
    }
    if let Some(parsed) = parse_iso(s) {
        return Ok(parsed);
    }
    Err(format!(
        "invalid schedule '{}'. Use '30m' / 'every 2h' / '0 9 * * *' / '2026-05-01T10:00' / 每日17:50",
        input
    ))
}

pub fn parse_natural_schedule(input: &str) -> Option<ParsedNaturalSchedule> {
    let s = input.trim();
    let (parsed, rest) = parse_chinese_schedule(s)?;
    let mut prompt = rest.trim().trim_start_matches(['给', '帮']);
    prompt = prompt.trim_start_matches('我').trim();
    let prompt = prompt.trim_matches(['"', '\'', '「', '」']);
    if prompt.is_empty() {
        return None;
    }
    let name = if prompt.chars().count() > 20 {
        let mut out = String::new();
        for ch in prompt.chars().take(20) {
            out.push(ch);
        }
        out.push_str("...");
        out
    } else {
        prompt.to_string()
    };
    Some(ParsedNaturalSchedule {
        parsed,
        prompt: prompt.to_string(),
        name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cron_schedule() {
        let parsed = parse_schedule("0 9 * * *").expect("cron");
        assert_eq!(parsed.kind, ParsedScheduleKind::Cron);
        assert_eq!(parsed.expr.as_deref(), Some("0 9 * * *"));
    }

    #[test]
    fn parse_chinese_schedule_prompt() {
        let parsed = parse_natural_schedule("每日17:50 帮我看看AI新闻").expect("natural");
        assert_eq!(parsed.parsed.kind, ParsedScheduleKind::Cron);
        assert!(!parsed.prompt.is_empty());
    }
}
