use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders};
use xet_runtime::console::model::{
    DownloadGroupState, FileDownloadState, FileUploadState, MonitorSnapshot, SessionState, ShardState, TermState,
    UploadCommitState, XorbState,
};

/// A monitor that has never carried traffic: no permits out, no bytes, no
/// model samples, no limit adjustments. (Upload monitors during a pure
/// download, and vice versa.)
pub fn monitor_is_idle(m: &MonitorSnapshot) -> bool {
    m.active_permits == 0
        && m.bytes_sent == 0
        && m.success.is_none()
        && m.latency.is_none()
        && m.limit_history.is_empty()
}

pub fn humanize_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if n < 1024 {
        return format!("{n} B");
    }
    let mut v = n as f64;
    let mut unit = 0;
    while v >= 1024.0 && unit < UNITS.len() - 1 {
        v /= 1024.0;
        unit += 1;
    }
    format!("{v:.1} {}", UNITS[unit])
}

pub fn humanize_rate(bps: Option<f64>) -> String {
    match bps {
        Some(v) if v.is_finite() && v > 0.0 => format!("{}/s", humanize_bytes(v as u64)),
        _ => "–".to_string(),
    }
}

pub fn short_hash(h: &str) -> String {
    if h.len() > 6 {
        format!("{}…", &h[..6])
    } else {
        h.to_string()
    }
}

pub fn percent(done: u64, total: u64) -> u16 {
    done.saturating_mul(100).checked_div(total).unwrap_or(0).min(100) as u16
}

/// One style language for all lifecycle states across pages.
pub fn state_style(label: &str) -> Style {
    match label {
        "complete" | "completed" | "uploaded" | "finished" | "consumed" => Style::default().fg(Color::Green),
        "failed" | "aborted" => Style::default().fg(Color::Red),
        "queued" | "enqueued" | "staging" | "formed" => Style::default().fg(Color::DarkGray),
        _ => Style::default().fg(Color::Cyan), // any in-flight state
    }
}

// snake_case labels matching the wire encoding, so the UI vocabulary matches
// what agents see over HTTP.
pub fn upload_state_label(s: FileUploadState) -> &'static str {
    match s {
        FileUploadState::Queued => "queued",
        FileUploadState::Chunking => "chunking",
        FileUploadState::Processed => "processed",
        FileUploadState::AwaitingXorbs => "awaiting_xorbs",
        FileUploadState::AwaitingShard => "awaiting_shard",
        FileUploadState::Complete => "complete",
        FileUploadState::Failed => "failed",
        FileUploadState::Aborted => "aborted",
    }
}

pub fn commit_state_label(s: UploadCommitState) -> &'static str {
    match s {
        UploadCommitState::Active => "active",
        UploadCommitState::Committing => "committing",
        UploadCommitState::Completed => "completed",
        UploadCommitState::Aborted => "aborted",
    }
}

pub fn xorb_state_label(s: XorbState) -> &'static str {
    match s {
        XorbState::Formed => "formed",
        XorbState::Queued => "queued",
        XorbState::Uploading => "uploading",
        XorbState::Uploaded => "uploaded",
        XorbState::Failed => "failed",
    }
}

pub fn shard_state_label(s: ShardState) -> &'static str {
    match s {
        ShardState::Staging => "staging",
        ShardState::Uploading => "uploading",
        ShardState::Uploaded => "uploaded",
    }
}

pub fn download_state_label(s: FileDownloadState) -> &'static str {
    match s {
        FileDownloadState::Queued => "queued",
        FileDownloadState::Reconstructing => "reconstructing",
        FileDownloadState::Complete => "complete",
        FileDownloadState::Failed => "failed",
        FileDownloadState::Aborted => "aborted",
    }
}

pub fn group_state_label(s: DownloadGroupState) -> &'static str {
    match s {
        DownloadGroupState::Active => "active",
        DownloadGroupState::Finished => "finished",
        DownloadGroupState::Aborted => "aborted",
    }
}

pub fn term_state_label(s: TermState) -> &'static str {
    match s {
        TermState::Enqueued => "enqueued",
        TermState::Fetching => "fetching",
        TermState::Fetched => "fetched",
        TermState::Consumed => "consumed",
    }
}

pub fn session_state_label(s: SessionState) -> &'static str {
    match s {
        SessionState::Active => "active",
        SessionState::Ended => "ended",
    }
}

/// Middle-truncates `s` to at most `max` display characters.
///
/// If `s` fits within `max`, it is returned unchanged. Otherwise the result is
/// `first…last` where `first` is `max - 9` chars and `last` is 8 chars,
/// joined by a single `…` (1 char), totalling `max` chars.
///
/// `max` must be at least 10; values below that fall back to returning `s`
/// unchanged to avoid panics.
pub fn truncate_middle(s: &str, max: usize) -> String {
    if max < 10 || s.chars().count() <= max {
        return s.to_string();
    }
    let keep_start = max - 9; // max - 8 (tail) - 1 (ellipsis)
    let chars: Vec<char> = s.chars().collect();
    let n = chars.len();
    let head: String = chars[..keep_start].iter().collect();
    let tail: String = chars[n - 8..].iter().collect();
    format!("{head}…{tail}")
}

/// Humanizes a duration given in milliseconds to a compact human-readable string.
///
/// - `< 1000 ms` → `"850ms"`
/// - `< 60 s` → `"3.2s"`
/// - `< 60 min` → `"2m 05s"`
/// - `≥ 60 min` → `"1h 03m"`
pub fn humanize_duration_ms(ms: u64) -> String {
    if ms < 1_000 {
        return format!("{ms}ms");
    }
    let secs = ms / 1_000;
    let frac_secs = ms % 1_000;
    if secs < 60 {
        // One decimal place: e.g. "3.2s"
        let tenths = frac_secs / 100;
        return format!("{secs}.{tenths}s");
    }
    let mins = secs / 60;
    let rem_secs = secs % 60;
    if mins < 60 {
        return format!("{mins}m {:02}s", rem_secs);
    }
    let hours = mins / 60;
    let rem_mins = mins % 60;
    format!("{hours}h {:02}m", rem_mins)
}

/// Bordered pane; the focused pane gets a highlighted title so tab-cycling is visible.
pub fn pane_block(title: String, focused: bool) -> Block<'static> {
    let block = Block::default().borders(Borders::ALL).title(title);
    if focused {
        block.border_style(Style::default().fg(Color::Cyan))
    } else {
        block
    }
}

/// Flattens a TestBackend buffer to a newline-joined string for content asserts.
#[cfg(test)]
pub fn buffer_text(backend: &ratatui::backend::TestBackend) -> String {
    let buffer = backend.buffer();
    let area = buffer.area;
    let mut out = String::new();
    for y in 0..area.height {
        for x in 0..area.width {
            out.push_str(buffer[(x, y)].symbol());
        }
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn humanize_bytes_picks_sane_units() {
        assert_eq!(humanize_bytes(0), "0 B");
        assert_eq!(humanize_bytes(1023), "1023 B");
        assert_eq!(humanize_bytes(1024), "1.0 KiB");
        assert_eq!(humanize_bytes(4 * 1024 * 1024), "4.0 MiB");
        assert_eq!(humanize_bytes(5_368_709_120), "5.0 GiB");
    }

    #[test]
    fn humanize_rate_handles_none() {
        assert_eq!(humanize_rate(None), "–");
        assert_eq!(humanize_rate(Some(412_000_000.0)), "392.9 MiB/s");
    }

    #[test]
    fn short_hash_truncates_with_ellipsis() {
        assert_eq!(short_hash("f3ab129944aa"), "f3ab12…");
        assert_eq!(short_hash("abc"), "abc");
    }

    #[test]
    fn percent_is_safe_on_zero_total() {
        assert_eq!(percent(0, 0), 0);
        assert_eq!(percent(50, 200), 25);
    }

    #[test]
    fn truncate_middle_fits_unchanged() {
        assert_eq!(truncate_middle("hello", 20), "hello");
        assert_eq!(truncate_middle("exactly20charshere!!", 20), "exactly20charshere!!");
    }

    #[test]
    fn truncate_middle_truncates_with_ellipsis() {
        // 70-char hex name → should be truncated
        let long = "a".repeat(30) + "b".repeat(8).as_str();
        let result = truncate_middle(&long, 20);
        assert_eq!(result.chars().count(), 20);
        assert!(result.contains('…'), "must contain ellipsis");
        assert!(result.ends_with(&"b".repeat(8)));
    }

    #[test]
    fn truncate_middle_very_short_max_returns_unchanged() {
        // max < 10 → never truncate (guard against panic)
        let s = "abcdefghijklmnop";
        assert_eq!(truncate_middle(s, 5), s);
    }

    #[test]
    fn humanize_duration_ms_milliseconds() {
        assert_eq!(humanize_duration_ms(0), "0ms");
        assert_eq!(humanize_duration_ms(850), "850ms");
        assert_eq!(humanize_duration_ms(999), "999ms");
    }

    #[test]
    fn humanize_duration_ms_seconds() {
        assert_eq!(humanize_duration_ms(1_000), "1.0s");
        assert_eq!(humanize_duration_ms(3_200), "3.2s");
        assert_eq!(humanize_duration_ms(59_900), "59.9s");
    }

    #[test]
    fn humanize_duration_ms_minutes() {
        assert_eq!(humanize_duration_ms(60_000), "1m 00s");
        assert_eq!(humanize_duration_ms(125_000), "2m 05s");
        assert_eq!(humanize_duration_ms(192_000), "3m 12s");
    }

    #[test]
    fn humanize_duration_ms_hours() {
        assert_eq!(humanize_duration_ms(3_780_000), "1h 03m");
        assert_eq!(humanize_duration_ms(7_200_000), "2h 00m");
    }
}
