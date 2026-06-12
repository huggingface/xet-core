use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};
use xet_runtime::console::model::{DownloadGroupDetail, DownloadGroupKind, SnapshotResponse, TermState};

use crate::app::{App, Pane};
use crate::poll::now_ms;
use crate::ui::widgets::*;

fn selected_group<'a>(app: &App, snap: &'a SnapshotResponse) -> Option<&'a DownloadGroupDetail> {
    let s = snap.sessions.get(app.session_idx)?;
    let live = &s.download_group_details;
    if app.group_idx < live.len() {
        live.get(app.group_idx)
    } else {
        s.detail.ended_download_groups.get(app.group_idx - live.len())
    }
}

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(g) = selected_group(app, snap) else {
        f.render_widget(Paragraph::new("no such download group — esc to go back"), area);
        return;
    };

    // Header occupies 3 lines (title + stats + caption); rest goes to content.
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);

    draw_header(f, rows[0], g);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(rows[1]);
    draw_files(f, cols[0], app, g);

    let side = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(cols[1]);
    draw_terms(f, side[0], app, g);
    draw_prefetch(f, side[1], app, g);
}

// ---- header ----

fn draw_header(f: &mut Frame, area: Rect, g: &DownloadGroupDetail) {
    let kind = match g.kind {
        DownloadGroupKind::Files => "files",
        DownloadGroupKind::Stream => "stream",
    };
    let endpoint = g.endpoint.as_deref().unwrap_or("?");

    // Line 1: identity
    let line1 = format!(
        " ▼ group #{} [{}] kind {kind} · endpoint {endpoint}",
        g.id,
        group_state_label(g.state)
    );

    // Line 2: progress stats
    let line2 = match &g.progress {
        Some(p) => {
            let done = p.bytes_completed;
            let total = p.total_bytes;
            let pct = percent(done, total);
            let rate = humanize_rate(p.rate_bps);

            let eta = eta_string(total, done, p.rate_bps);

            let wire = humanize_bytes(p.transfer_bytes_completed);
            let retry_note = if p.transfer_bytes_completed > p.bytes_completed && p.bytes_completed > 0 {
                let overhead = (p.transfer_bytes_completed as u128 * 100)
                    .checked_div(p.bytes_completed as u128)
                    .unwrap_or(100)
                    .saturating_sub(100);
                format!(" (+{overhead}% retries)")
            } else {
                String::new()
            };

            let completed_files = g.file_counts.completed;
            let in_flight = g.file_counts.in_flight;

            format!(
                " {done_h} / {total_h} ({pct}%) · {rate} · ETA {eta} · wire {wire}{retry_note} · files {completed_files}/{total_files}",
                done_h = humanize_bytes(done),
                total_h = humanize_bytes(total),
                total_files = completed_files as usize + in_flight,
            )
        },
        None => " – / – (–%) · – · ETA – · wire – · files –/–".to_string(),
    };

    // Line 3: caption
    let line3 = " files are reconstructed from CAS term blocks; the prefetcher schedules blocks ahead of the writer";

    let text = format!("{line1}\n{line2}\n{line3}");
    f.render_widget(
        Paragraph::new(text)
            .style(Style::default().add_modifier(Modifier::BOLD))
            // Override caption line to DarkGray — we do it per-line below
            ,
        area,
    );

    // Re-render just the caption line in DarkGray (line index 2 = y+2).
    let caption_area = Rect {
        y: area.y + 2,
        height: 1,
        ..area
    };
    f.render_widget(
        Paragraph::new(line3).style(Style::default().fg(Color::DarkGray)),
        caption_area,
    );
}

// ---- files table ----

fn draw_files(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let mut rows: Vec<Row> = Vec::new();
    for file in &g.files {
        let done_total = format!(
            "{}/{}",
            humanize_bytes(file.bytes_completed),
            humanize_bytes(file.total_bytes)
        );
        rows.push(Row::new(vec![
            Cell::from(truncate_middle(&file.name, 34)),
            Cell::from(download_state_label(file.state)).style(state_style(download_state_label(file.state))),
            Cell::from(format!("{:>3}%", percent(file.bytes_completed, file.total_bytes.max(1)))),
            Cell::from(done_total),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
        ]));
    }
    for (_, file) in &g.completed_files {
        rows.push(Row::new(vec![
            Cell::from(truncate_middle(&file.name, 34)),
            Cell::from(download_state_label(file.state)).style(state_style(download_state_label(file.state))),
            Cell::from("100%"),
            Cell::from(humanize_bytes(file.total_bytes)),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
        ]));
    }
    let n = rows.len();
    let mut tstate = TableState::default();
    if n > 0 {
        tstate.select(Some(app.main_row.min(n - 1)));
    }
    let table = Table::new(
        rows,
        [
            Constraint::Min(34),
            Constraint::Length(16),
            Constraint::Length(5),
            Constraint::Length(16),
            Constraint::Length(9),
        ],
    )
    .header(
        Row::new(vec!["name", "state", "prog", "done/total", "hash"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(pane_block(format!(" files ({}/{} active) ", g.file_counts.in_flight, n), app.pane == Pane::Main))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(table, area, &mut tstate);
}

// ---- side-pane file resolution ----

/// Resolve which in-flight file (if any) should be shown in the side panes.
///
/// The files table shows `g.files` (in-flight) followed by `g.completed_files`.
/// `app.main_row` can point anywhere in that combined range. We clamp to the
/// last real row, then return `Some(file)` only when the clamped index falls
/// inside `g.files`; a completed row returns `None` paired with
/// `completed = true` so callers can emit the right message.
fn side_pane_file<'a>(
    app: &App,
    g: &'a DownloadGroupDetail,
) -> (Option<&'a xet_runtime::console::model::FileDownloadSnapshot>, bool) {
    let total = g.files.len() + g.completed_files.len();
    if total == 0 {
        return (None, false);
    }
    let clamped = app.main_row.min(total - 1);
    if clamped < g.files.len() {
        (g.files.get(clamped), false)
    } else {
        (None, true)
    }
}

// ---- term blocks pane ----

fn draw_terms(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let (file, completed) = side_pane_file(app, g);
    let now = now_ms();
    let mut lines: Vec<Line> = Vec::new();
    if let Some(file) = file {
        for b in &file.term_blocks {
            let size = humanize_bytes(b.byte_range.1.saturating_sub(b.byte_range.0));
            let detail = match b.state {
                TermState::Enqueued => {
                    let age = humanize_duration_ms(now.saturating_sub(b.created_at));
                    format!("waiting {age}")
                },
                TermState::Fetching => {
                    let age = humanize_duration_ms(now.saturating_sub(b.created_at));
                    format!("resolving… {age}")
                },
                TermState::Fetched => {
                    let dur = humanize_duration_ms(
                        b.fetched_at
                            .unwrap_or(now)
                            .saturating_sub(b.created_at),
                    );
                    format!("{} terms in {dur}", b.terms.len())
                },
                TermState::Consumed => {
                    // Consumed blocks are typically removed from the list, but
                    // handle gracefully if present.
                    "consumed".to_string()
                },
            };
            lines.push(Line::styled(
                format!(
                    "#{} {} {}–{} ({size}) · {detail}",
                    b.block_id,
                    term_state_label(b.state),
                    humanize_bytes(b.byte_range.0),
                    humanize_bytes(b.byte_range.1),
                ),
                state_style(term_state_label(b.state)),
            ));
        }
        lines.push(Line::from(format!("consumed {} blocks total", file.consumed_blocks)));
    } else if completed {
        lines.push(Line::from("file complete — no live blocks"));
    } else {
        lines.push(Line::from("no in-flight file selected"));
    }
    f.render_widget(
        Paragraph::new(lines)
            .block(pane_block(" term blocks — fetch queue (selected file) ".into(), app.pane == Pane::SideTop)),
        area,
    );
}

// ---- prefetch / readahead pane ----

fn draw_prefetch(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let (file, completed) = side_pane_file(app, g);
    let now = now_ms();
    let lines: Vec<Line> = if let Some(fl) = file {
        if let Some(p) = &fl.prefetch {
            let file_total = fl.total_bytes.max(1);
            let active_pct = percent(p.active_byte_position, file_total);
            let prefetched_pct = percent(p.prefetched_byte_position, file_total);
            let readahead = p.prefetched_byte_position.saturating_sub(p.active_byte_position);
            let rate = humanize_rate(p.completion_rate_bps);
            let eta = eta_string(file_total, fl.bytes_completed, p.completion_rate_bps);
            let elapsed = humanize_duration_ms(now.saturating_sub(fl.created_at));
            vec![
                Line::from(format!("writer    @ {} ({active_pct}%)", humanize_bytes(p.active_byte_position))),
                Line::from(format!("scheduled @ {} ({prefetched_pct}%)", humanize_bytes(p.prefetched_byte_position))),
                Line::from(format!("readahead {} · {} block(s) in flight", humanize_bytes(readahead), p.queue_depth)),
                Line::from(format!("block rate {rate} · file ETA {eta}")),
                Line::from(format!("elapsed {elapsed}")),
            ]
        } else {
            vec![Line::from("no prefetch state")]
        }
    } else if completed {
        vec![Line::from("file complete — no prefetch state")]
    } else {
        vec![Line::from("no prefetch state")]
    };
    f.render_widget(
        Paragraph::new(lines)
            .block(pane_block(" readahead (selected file) ".into(), app.pane == Pane::SideBottom)),
        area,
    );
}

// ---- shared helpers ----

/// Returns ETA as a human-readable duration string, or "–" when unavailable.
fn eta_string(total: u64, done: u64, rate_bps: Option<f64>) -> String {
    if done >= total {
        return "–".to_string();
    }
    match rate_bps {
        Some(r) if r.is_finite() && r > 0.0 => {
            let remaining_bytes = total - done;
            let ms = ((remaining_bytes as f64 / r) * 1000.0) as u64;
            humanize_duration_ms(ms)
        },
        _ => "–".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::{App, Page};
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    #[test]
    fn download_page_shows_files_terms_and_prefetch() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App {
            page: Page::Download,
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(160, 40)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("group #3"), "{text}");
        assert!(text.contains("out.bin"), "{text}");
        assert!(text.contains("reconstructing"), "{text}");
        assert!(text.contains("term blocks — fetch queue"), "{text}");
        assert!(text.contains("resolving…"), "{text}"); // block 4 is Fetching
        assert!(text.contains("consumed 5 blocks"), "{text}");
        // readahead pane
        assert!(text.contains("readahead"), "{text}");
        assert!(text.contains("2 block(s) in flight"), "{text}");
        assert!(text.contains("writer"), "{text}");
        assert!(text.contains("scheduled"), "{text}");
        // header stats
        assert!(text.contains("ETA"), "{text}");
        // caption
        assert!(text.contains("files are reconstructed from CAS term blocks"), "{text}");
    }

    /// The fixture group has 1 in-flight file (index 0) and 0 completed files
    /// (total table length = 1). With main_row = 5, the full-table clamp lands
    /// on index 0 (the only real row), which is in-flight — so the side panes
    /// must show that file's live data rather than going blank or silently
    /// tracking an unrelated row.
    #[test]
    fn out_of_range_row_clamps_to_last_real_row() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App {
            page: Page::Download,
            main_row: 5,
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(160, 40)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        // The clamped row (0) is in-flight, so live block data must appear.
        assert!(text.contains("consumed 5 blocks"), "side panes must show clamped in-flight file: {text}");
        assert!(text.contains("2 block(s) in flight"), "readahead must show clamped in-flight file: {text}");
        // Must not show the completed-row placeholder (no completed files exist).
        assert!(
            !text.contains("file complete — no live blocks"),
            "must not show completed-row message when clamped row is in-flight: {text}"
        );
    }
}
