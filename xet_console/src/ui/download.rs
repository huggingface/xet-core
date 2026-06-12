use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};
use xet_runtime::console::model::{DownloadGroupDetail, DownloadGroupKind, SnapshotResponse};

use crate::app::{App, Pane};
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

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(3)])
        .split(area);

    let (pct, rate) = match &g.progress {
        Some(p) => (percent(p.bytes_completed, p.total_bytes), humanize_rate(p.rate_bps)),
        None => (0, "–".to_string()),
    };
    let kind = match g.kind {
        DownloadGroupKind::Files => "files",
        DownloadGroupKind::Stream => "stream",
    };
    f.render_widget(
        Paragraph::new(format!(
            " ▼ group #{} [{}] kind {kind}  {pct}%  {rate}",
            g.id,
            group_state_label(g.state)
        ))
        .style(Style::default().add_modifier(Modifier::BOLD)),
        rows[0],
    );

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

fn draw_files(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let mut rows: Vec<Row> = Vec::new();
    for file in &g.files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(download_state_label(file.state))
                .style(state_style(download_state_label(file.state))),
            Cell::from(format!("{:>3}%", percent(file.bytes_completed, file.total_bytes.max(1)))),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
        ]));
    }
    for (_, file) in &g.completed_files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(download_state_label(file.state))
                .style(state_style(download_state_label(file.state))),
            Cell::from("100%"),
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
            Constraint::Min(24),
            Constraint::Length(16),
            Constraint::Length(5),
            Constraint::Length(9),
        ],
    )
    .header(
        Row::new(vec!["name", "state", "prog", "hash"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(pane_block(
        format!(" files ({}/{} active) ", g.file_counts.in_flight, n),
        app.pane == Pane::Main,
    ))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(table, area, &mut tstate);
}

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

/// Term blocks of the file selected in the files pane (in-flight files only;
/// completed files have no live blocks).
fn draw_terms(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let (file, completed) = side_pane_file(app, g);
    let mut lines: Vec<Line> = Vec::new();
    if let Some(file) = file {
        for b in &file.term_blocks {
            lines.push(Line::styled(
                format!(
                    "block {} [{}] {}–{} ({} terms)",
                    b.block_id,
                    term_state_label(b.state),
                    humanize_bytes(b.byte_range.0),
                    humanize_bytes(b.byte_range.1),
                    b.terms.len(),
                ),
                state_style(term_state_label(b.state)),
            ));
        }
        lines.push(Line::from(format!("consumed {}", file.consumed_blocks)));
    } else if completed {
        lines.push(Line::from("file complete — no live blocks"));
    } else {
        lines.push(Line::from("no in-flight file selected"));
    }
    f.render_widget(
        Paragraph::new(lines).block(pane_block(" term blocks ".into(), app.pane == Pane::SideTop)),
        area,
    );
}

fn draw_prefetch(f: &mut Frame, area: Rect, app: &App, g: &DownloadGroupDetail) {
    let (file, completed) = side_pane_file(app, g);
    let lines: Vec<Line> = if let Some(p) = file.and_then(|fl| fl.prefetch.as_ref()) {
        vec![
            Line::from(format!("queue {}", p.queue_depth)),
            Line::from(format!("prefetched @ {}", humanize_bytes(p.prefetched_byte_position))),
            Line::from(format!("active     @ {}", humanize_bytes(p.active_byte_position))),
            Line::from(format!("rate {}", humanize_rate(p.completion_rate_bps))),
        ]
    } else if completed {
        vec![Line::from("file complete — no prefetch state")]
    } else {
        vec![Line::from("no prefetch state")]
    };
    f.render_widget(
        Paragraph::new(lines)
            .block(pane_block(" prefetch ".into(), app.pane == Pane::SideBottom)),
        area,
    );
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
        let app = App { page: Page::Download, ..Default::default() };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("group #3"), "{text}");
        assert!(text.contains("out.bin"), "{text}");
        assert!(text.contains("reconstructing"), "{text}");
        assert!(text.contains("term blocks"), "{text}");
        assert!(text.contains("fetching"), "{text}");
        assert!(text.contains("consumed 5"), "{text}");
        assert!(text.contains("queue 2"), "{text}");
        assert!(text.contains("prefetch"), "{text}");
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
        let app = App { page: Page::Download, main_row: 5, ..Default::default() };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        // The clamped row (0) is in-flight, so live block data must appear.
        assert!(text.contains("consumed 5"), "side panes must show clamped in-flight file: {text}");
        assert!(text.contains("queue 2"), "prefetch must show clamped in-flight file: {text}");
        // Must not show the completed-row placeholder (no completed files exist).
        assert!(
            !text.contains("file complete — no live blocks"),
            "must not show completed-row message when clamped row is in-flight: {text}"
        );
    }
}
