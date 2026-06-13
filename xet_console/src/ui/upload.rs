use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Cell, Paragraph, Row, Table, TableState};
use xet_runtime::console::model::{SnapshotResponse, UploadCommitDetail};

use crate::app::{App, Pane};
use crate::ui::widgets::*;

/// Selected commit for the page: live details first, then ended (same order
/// as overview_entries builds rows).
fn selected_commit<'a>(app: &App, snap: &'a SnapshotResponse) -> Option<&'a UploadCommitDetail> {
    let s = snap.sessions.get(app.session_idx)?;
    let live = &s.upload_commit_details;
    if app.commit_idx < live.len() {
        live.get(app.commit_idx)
    } else {
        s.detail.ended_upload_commits.get(app.commit_idx - live.len())
    }
}

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(c) = selected_commit(app, snap) else {
        f.render_widget(Paragraph::new("no such commit — esc to go back"), area);
        return;
    };

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(3), Constraint::Length(1)])
        .split(area);

    draw_header(f, rows[0], c);

    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(rows[1]);
    draw_files(f, cols[0], app, c);

    // Shards are usually 1–3 entries (the live staging accumulator plus any
    // flushed/uploaded shards), so size that pane to its content rather than a
    // fixed fraction — the dead space goes to the busy xorbs pane instead.
    // +2 for the border; clamp so an empty pane still shows its title and a
    // many-shard commit can't crowd out xorbs.
    let shard_height = (c.shards.len() as u16 + 2).clamp(3, 9);
    let side = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(shard_height)])
        .split(cols[1]);
    draw_xorbs(f, side[0], app, c);
    draw_shards(f, side[1], app, c);

    draw_recent(f, rows[2], c);
}

fn draw_header(f: &mut Frame, area: Rect, c: &UploadCommitDetail) {
    let (pct, done, total, rate) = match &c.progress {
        Some(p) => (
            percent(p.bytes_completed, p.total_bytes),
            humanize_bytes(p.bytes_completed),
            humanize_bytes(p.total_bytes),
            humanize_rate(p.rate_bps),
        ),
        None => (0, "?".into(), "?".into(), "–".into()),
    };
    let d = &c.dedup;
    let dedup_pct = percent(d.deduped_bytes, d.total_bytes);
    let lines = vec![
        Line::from(
            format!(" ▲ commit #{} [{}]  {done} / {total} ({pct}%)  {rate}", c.id, commit_state_label(c.state),),
        ),
        Line::from(format!(
            " dedup {dedup_pct}%: {} deduped ({} global) · {} new · xorb↑ {} · shard↑ {}",
            humanize_bytes(d.deduped_bytes),
            humanize_bytes(d.deduped_bytes_by_global_dedup),
            humanize_bytes(d.new_bytes),
            humanize_bytes(d.xorb_bytes_uploaded),
            humanize_bytes(d.shard_bytes_uploaded),
        )),
    ];
    f.render_widget(Paragraph::new(lines).style(Style::default().add_modifier(Modifier::BOLD)), area);
}

fn draw_files(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    // In-flight rows first, then the recent-completions ring (oldest first).
    let mut rows: Vec<Row> = Vec::new();
    for file in &c.files {
        let pct = file.size.map(|s| percent(file.bytes_chunked, s)).unwrap_or(0);
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(upload_state_label(file.state)).style(state_style(upload_state_label(file.state))),
            Cell::from(format!("{pct:>3}%")),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
            Cell::from(if file.shard_uploaded { "✓" } else { "–" }),
        ]));
    }
    for (_, file) in &c.completed_files {
        rows.push(Row::new(vec![
            Cell::from(file.name.clone()),
            Cell::from(upload_state_label(file.state)).style(state_style(upload_state_label(file.state))),
            Cell::from("100%"),
            Cell::from(file.file_hash.as_deref().map(short_hash).unwrap_or_else(|| "–".into())),
            Cell::from(if file.shard_uploaded { "✓" } else { "–" }),
        ]));
    }
    let n = rows.len();
    let mut tstate = TableState::default();
    if n > 0 {
        tstate.select(Some(app.main_row.min(n - 1)));
    }
    let focused = app.pane == Pane::Main;
    let table = Table::new(
        rows,
        [
            Constraint::Min(24),
            Constraint::Length(15),
            Constraint::Length(5),
            Constraint::Length(9),
            Constraint::Length(5),
        ],
    )
    .header(
        Row::new(vec!["name", "state", "prog", "hash", "shard"]).style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(pane_block(format!(" files ({}/{} active) ", c.file_counts.in_flight, n), focused))
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    f.render_stateful_widget(table, area, &mut tstate);
}

fn draw_xorbs(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    let mut lines: Vec<Line> = Vec::new();
    for x in &c.xorbs.in_flight {
        let pct = percent(x.bytes_transferred, x.serialized_bytes.max(1));
        lines.push(Line::styled(
            format!("{} {} {pct}%", short_hash(&x.hash), xorb_state_label(x.state)),
            state_style(xorb_state_label(x.state)),
        ));
    }
    lines.push(Line::from(format!(
        "done {} · failed {} · formed {}",
        c.xorbs.counts.uploaded, c.xorbs.counts.failed, c.xorbs.counts.formed
    )));
    for (_, x) in c.xorbs.recent.iter().rev().take(3) {
        lines.push(Line::from(format!("recent {} {}", short_hash(&x.hash), xorb_state_label(x.state))));
    }
    let focused = app.pane == Pane::SideTop;
    f.render_widget(
        Paragraph::new(lines).block(pane_block(format!(" xorbs ({} in-flight) ", c.xorbs.in_flight.len()), focused)),
        area,
    );
}

fn draw_shards(f: &mut Frame, area: Rect, app: &App, c: &UploadCommitDetail) {
    let lines: Vec<Line> = c
        .shards
        .iter()
        .map(|s| {
            Line::styled(
                format!(
                    "{} {} — {} xorbs, {}",
                    s.hash.as_deref().map(short_hash).unwrap_or_else(|| "#live".into()),
                    shard_state_label(s.state),
                    s.n_xorbs,
                    humanize_bytes(s.size),
                ),
                state_style(shard_state_label(s.state)),
            )
        })
        .collect();
    let focused = app.pane == Pane::SideBottom;
    f.render_widget(Paragraph::new(lines).block(pane_block(format!(" shards ({}) ", c.shards.len()), focused)), area);
}

fn draw_recent(f: &mut Frame, area: Rect, c: &UploadCommitDetail) {
    let last_file = c.completed_files.last().map(|(_, fl)| fl.name.clone());
    let last_xorb = c.xorbs.recent.last().map(|(_, x)| short_hash(&x.hash));
    let line = match (last_file, last_xorb) {
        (Some(fl), Some(x)) => format!(" recent: {fl} done · xorb {x} ↑"),
        (Some(fl), None) => format!(" recent: {fl} done"),
        (None, Some(x)) => format!(" recent: xorb {x} ↑"),
        (None, None) => " recent: —".to_string(),
    };
    f.render_widget(Paragraph::new(line).style(Style::default().fg(ratatui::style::Color::DarkGray)), area);
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

    fn render(app: &App) -> String {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, app, &state)).unwrap();
        buffer_text(terminal.backend())
    }

    #[test]
    fn upload_page_shows_layout_a_panes() {
        let app = App {
            page: Page::Upload,
            ..Default::default()
        };
        let text = render(&app);
        // header strip
        assert!(text.contains("commit #7"), "{text}");
        assert!(text.contains("dedup"), "{text}");
        // files table (left)
        assert!(text.contains("model-00001.safetensors"), "{text}");
        assert!(text.contains("chunking"), "{text}");
        assert!(text.contains("tokenizer.json"), "{text}");
        assert!(text.contains("awaiting_shard"), "{text}");
        assert!(text.contains("9c01dd…"), "short hash shown: {text}");
        // completed file folded into the table after in-flight rows
        assert!(text.contains("config.json"), "{text}");
        // xorbs pane (right top)
        assert!(text.contains("xorbs"), "{text}");
        assert!(text.contains("f3ab12…"), "{text}");
        assert!(text.contains("done 46"), "{text}");
        assert!(text.contains("failed 1"), "{text}");
        // shards pane (right bottom)
        assert!(text.contains("shards"), "{text}");
        assert!(text.contains("staging"), "{text}");
        assert!(text.contains("uploaded"), "{text}");
    }

    #[test]
    fn upload_page_handles_missing_commit_gracefully() {
        let app = App {
            page: Page::Upload,
            commit_idx: 99,
            ..Default::default()
        };
        let text = render(&app);
        assert!(text.contains("no such commit"), "{text}");
    }
}
