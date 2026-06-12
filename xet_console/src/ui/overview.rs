use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use xet_runtime::console::model::{SessionSummary, SnapshotResponse};

use crate::app::{App, OverviewEntry, overview_entries};
use crate::ui::widgets::*;

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse, ended_sessions: &[SessionSummary]) {
    let (list_area, footer_area) = if ended_sessions.is_empty() {
        (area, None)
    } else {
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(3), Constraint::Length(1)])
            .split(area);
        (split[0], Some(split[1]))
    };

    let entries = overview_entries(snap);
    let items: Vec<ListItem> = entries.iter().map(|e| ListItem::new(entry_line(e, snap))).collect();

    let mut state = ListState::default();
    if !entries.is_empty() {
        state.select(Some(app.overview_row.min(entries.len() - 1)));
    }
    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title(format!(
            " sessions ({}) — as_of {} ",
            snap.sessions.len(),
            snap.as_of
        )))
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED))
        .highlight_symbol("▸ ");
    f.render_stateful_widget(list, list_area, &mut state);

    if let Some(footer) = footer_area {
        let ids = ended_sessions
            .iter()
            .map(|s| s.id[..8.min(s.id.len())].to_string())
            .collect::<Vec<_>>()
            .join(", ");
        f.render_widget(
            Paragraph::new(format!(" ended sessions: {ids}"))
                .style(Style::default().fg(ratatui::style::Color::DarkGray)),
            footer,
        );
    }
}

fn entry_line(e: &OverviewEntry, snap: &SnapshotResponse) -> String {
    match *e {
        OverviewEntry::Session { session_idx } => {
            let s = &snap.sessions[session_idx];
            let monitors = s
                .detail
                .monitors
                .iter()
                .map(|m| format!("{} {}/{}", m.tag, m.active_permits, m.total_permits))
                .collect::<Vec<_>>()
                .join(" · ");
            format!(
                "session {} [{}]  monitors: {}",
                &s.detail.id[..8.min(s.detail.id.len())],
                session_state_label(s.detail.state),
                if monitors.is_empty() {
                    "none".to_string()
                } else {
                    monitors
                }
            )
        },
        OverviewEntry::Commit {
            session_idx,
            commit_idx,
        } => {
            let s = &snap.sessions[session_idx];
            let live = &s.upload_commit_details;
            let (c, ended) = if commit_idx < live.len() {
                (&live[commit_idx], false)
            } else {
                (&s.detail.ended_upload_commits[commit_idx - live.len()], true)
            };
            let pct = c
                .progress
                .as_ref()
                .map(|p| percent(p.bytes_completed, p.total_bytes))
                .unwrap_or(0);
            let rate = humanize_rate(c.progress.as_ref().and_then(|p| p.rate_bps));
            format!(
                "  ▲ commit #{} [{}] {}% {} files {}/{}{}",
                c.id,
                commit_state_label(c.state),
                pct,
                rate,
                c.file_counts.completed,
                c.file_counts.completed + c.file_counts.in_flight as u64,
                if ended { "  (ended)" } else { "" }
            )
        },
        OverviewEntry::Group { session_idx, group_idx } => {
            let s = &snap.sessions[session_idx];
            let live = &s.download_group_details;
            let (g, ended) = if group_idx < live.len() {
                (&live[group_idx], false)
            } else {
                (&s.detail.ended_download_groups[group_idx - live.len()], true)
            };
            let pct = g
                .progress
                .as_ref()
                .map(|p| percent(p.bytes_completed, p.total_bytes))
                .unwrap_or(0);
            format!(
                "  ▼ group #{} [{}] {}% {} files in flight{}",
                g.id,
                group_state_label(g.state),
                pct,
                g.file_counts.in_flight,
                if ended { "  (ended)" } else { "" }
            )
        },
    }
}

#[cfg(test)]
mod tests {
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    use crate::app::App;
    use crate::fixtures::sample_snapshot;
    use crate::poll::PollState;
    use crate::ui;
    use crate::ui::widgets::buffer_text;

    #[test]
    fn overview_lists_sessions_commits_groups_monitors_and_ended_sessions() {
        use xet_runtime::console::model::{SessionState, SessionSummary};
        let snap = sample_snapshot();
        let state = PollState {
            snapshot: Some(snap),
            ended_sessions: vec![SessionSummary {
                id: "41bcdead-0000-0000-0000-000000000000".into(),
                state: SessionState::Ended,
                created_at: 0,
                n_upload_commits: 2,
                n_download_groups: 0,
                n_monitors: 0,
            }],
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App::default();
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("9f3c0000"), "session id shown:\n{text}");
        assert!(text.contains("commit #7"), "live commit listed:\n{text}");
        assert!(text.contains("commit #5"), "ended commit listed:\n{text}");
        assert!(text.contains("group #3"), "download group listed:\n{text}");
        assert!(text.contains("upload 14/16"), "monitor one-liner shown:\n{text}");
        assert!(text.contains("ended sessions: 41bcdead"), "ended-session footer:\n{text}");
        assert!(text.contains("[1]overview"), "key bar present:\n{text}");
    }

    #[test]
    fn disconnected_banner_when_no_data() {
        let state = PollState::default();
        let app = App::default();
        let mut terminal = Terminal::new(TestBackend::new(100, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("no data"), "disconnected hint shown:\n{text}");
    }
}
