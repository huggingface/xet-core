use std::collections::HashSet;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{Block, Borders, List, ListItem, ListState, Paragraph};
use xet_runtime::console::model::{SessionSummary, SnapshotResponse};

use crate::app::{App, OverviewEntry, overview_entries};
use crate::ui::widgets::{self, *};

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

    let entries = overview_entries(snap, &app.expanded);
    let items: Vec<ListItem> = entries
        .iter()
        .map(|e| ListItem::new(entry_line(e, snap, &app.expanded)))
        .collect();

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
        .highlight_symbol("❯ ");
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

/// Expand affordance for a commit/group row: a right triangle when collapsed,
/// a down triangle when expanded, and a blank slot (same width) when there are
/// no files to expand — so the caret never promises children that aren't there.
fn expand_caret(has_children: bool, expanded: bool) -> &'static str {
    if !has_children {
        "  "
    } else if expanded {
        "▾ "
    } else {
        "▸ "
    }
}

fn entry_line(e: &OverviewEntry, snap: &SnapshotResponse, expanded: &HashSet<u64>) -> String {
    match *e {
        OverviewEntry::Session { session_idx } => {
            let s = &snap.sessions[session_idx];
            let all = &s.detail.monitors;
            let idle_count = all.iter().filter(|m| widgets::monitor_is_idle(m)).count();
            let active: Vec<_> = all.iter().filter(|m| !widgets::monitor_is_idle(m)).collect();
            let monitors_str = if all.is_empty() {
                "none".to_string()
            } else if active.is_empty() {
                // Every monitor is idle.
                format!("{} idle", all.len())
            } else {
                let mut parts = active
                    .iter()
                    .map(|m| format!("{} {}/{}", m.tag, m.active_permits, m.total_permits))
                    .collect::<Vec<_>>()
                    .join(" · ");
                if idle_count > 0 {
                    parts.push_str(&format!(" +{idle_count} idle"));
                }
                parts
            };
            format!(
                "session {} [{}]  monitors: {}",
                &s.detail.id[..8.min(s.detail.id.len())],
                session_state_label(s.detail.state),
                monitors_str,
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
            let has_files = !c.files.is_empty() || !c.completed_files.is_empty();
            let caret = expand_caret(has_files, expanded.contains(&c.id));
            format!(
                "{caret}↑ commit #{} [{}] {}% {} files {}/{}{}",
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
            let has_files = !g.files.is_empty() || !g.completed_files.is_empty();
            let caret = expand_caret(has_files, expanded.contains(&g.id));
            format!(
                "{caret}↓ group #{} [{}] {}% {} files in flight{}",
                g.id,
                group_state_label(g.state),
                pct,
                g.file_counts.in_flight,
                if ended { "  (ended)" } else { "" }
            )
        },
        OverviewEntry::UploadFile {
            session_idx,
            commit_idx,
            table_row,
        } => {
            let s = &snap.sessions[session_idx];
            let commit = if commit_idx < s.upload_commit_details.len() {
                &s.upload_commit_details[commit_idx]
            } else {
                &s.detail.ended_upload_commits[commit_idx - s.upload_commit_details.len()]
            };
            let n_live = commit.files.len();
            if table_row < n_live {
                let file = &commit.files[table_row];
                let pct = file.size.map(|s| percent(file.bytes_chunked, s)).unwrap_or(0);
                format!("      · {}  [{}]  {}%", file.name, upload_state_label(file.state), pct,)
            } else {
                let ring_pos = table_row - n_live;
                let (_, file) = &commit.completed_files[ring_pos];
                format!("      · {}  [{}]  100%", file.name, upload_state_label(file.state),)
            }
        },
        OverviewEntry::DownloadFile {
            session_idx,
            group_idx,
            table_row,
        } => {
            let s = &snap.sessions[session_idx];
            let group = if group_idx < s.download_group_details.len() {
                &s.download_group_details[group_idx]
            } else {
                &s.detail.ended_download_groups[group_idx - s.download_group_details.len()]
            };
            let n_live = group.files.len();
            if table_row < n_live {
                let file = &group.files[table_row];
                let pct = percent(file.bytes_completed, file.total_bytes.max(1));
                format!("      · {}  [{}]  {}%", file.name, download_state_label(file.state), pct,)
            } else {
                let ring_pos = table_row - n_live;
                let (_, file) = &group.completed_files[ring_pos];
                format!("      · {}  [{}]  100%", file.name, download_state_label(file.state),)
            }
        },
        OverviewEntry::More {
            session_idx,
            kind_commit,
            idx,
        } => {
            // Compute how many additional completed entries are hidden.
            let s = &snap.sessions[session_idx];
            let k = if kind_commit {
                let commit = if idx < s.upload_commit_details.len() {
                    &s.upload_commit_details[idx]
                } else {
                    &s.detail.ended_upload_commits[idx - s.upload_commit_details.len()]
                };
                commit.completed_files.len().saturating_sub(5)
            } else {
                let group = if idx < s.download_group_details.len() {
                    &s.download_group_details[idx]
                } else {
                    &s.detail.ended_download_groups[idx - s.download_group_details.len()]
                };
                group.completed_files.len().saturating_sub(5)
            };
            format!("      … +{k} more (⏎ for detail)")
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
        assert!(text.contains("upload 14/16"), "active monitor shown:\n{text}");
        assert!(text.contains("+1 idle"), "idle count suffix shown:\n{text}");
        assert!(text.contains("ended sessions: 41bcdead"), "ended-session footer:\n{text}");
        assert!(text.contains("[1]overview"), "key bar present:\n{text}");
    }

    /// Expanding commit id 7 (the fixture's live commit) shows its in-flight
    /// and completed-ring file names indented under the commit row.
    /// The collapsed default must NOT show those file names on the overview.
    #[test]
    fn expanded_commit_shows_file_names_collapsed_does_not() {
        use std::collections::HashSet;
        let snap = sample_snapshot();

        // --- collapsed ---
        let state_collapsed = PollState {
            snapshot: Some(snap.clone()),
            ..Default::default()
        };
        let app_collapsed = App::default();
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app_collapsed, &state_collapsed)).unwrap();
        let text_collapsed = buffer_text(terminal.backend());
        assert!(
            !text_collapsed.contains("model-00001.safetensors"),
            "collapsed should NOT show in-flight file name:\n{text_collapsed}"
        );
        assert!(
            !text_collapsed.contains("config.json"),
            "collapsed should NOT show completed-ring file name:\n{text_collapsed}"
        );

        // --- expanded (id 7 in expanded set) ---
        let state_expanded = PollState {
            snapshot: Some(snap),
            ..Default::default()
        };
        let mut app_expanded = App::default();
        app_expanded.expanded.insert(7);
        let mut terminal2 = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal2.draw(|f| ui::draw(f, &app_expanded, &state_expanded)).unwrap();
        let text_expanded = buffer_text(terminal2.backend());
        assert!(
            text_expanded.contains("model-00001.safetensors"),
            "expanded should show in-flight file:\n{text_expanded}"
        );
        assert!(text_expanded.contains("config.json"), "expanded should show completed-ring file:\n{text_expanded}");
    }

    /// Transfer direction uses up/down arrows; the expand state uses
    /// right/down triangles; the selection cursor is a chevron, not a triangle.
    /// Rows with no files to expand (the ended commit #5) get no caret.
    #[test]
    fn direction_uses_arrows_and_expand_uses_triangles() {
        let snap = sample_snapshot();

        // --- collapsed ---
        let state = PollState {
            snapshot: Some(snap.clone()),
            ..Default::default()
        };
        let app = App::default();
        let mut terminal = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("▸ ↑ commit #7"), "collapsed upload commit: caret + up arrow:\n{text}");
        assert!(text.contains("▸ ↓ group #3"), "collapsed download group: caret + down arrow:\n{text}");
        assert!(!text.contains('▲'), "no upload triangle remains:\n{text}");
        assert!(!text.contains('▼'), "no download triangle remains:\n{text}");
        // Ended commit #5 has no files, so it must show no expand caret.
        assert!(text.contains("↑ commit #5"), "ended commit still shows up arrow:\n{text}");
        assert!(!text.contains("▸ ↑ commit #5"), "ended commit (no files) has no caret:\n{text}");
        assert!(!text.contains("▾ ↑ commit #5"), "ended commit (no files) has no caret:\n{text}");
        // Selection cursor is the chevron, on the default-selected session row.
        assert!(text.contains("❯ session"), "chevron cursor on selected row:\n{text}");

        // --- expanded commit 7 ---
        let mut app_expanded = App::default();
        app_expanded.expanded.insert(7);
        let state_expanded = PollState {
            snapshot: Some(snap),
            ..Default::default()
        };
        let mut terminal2 = Terminal::new(TestBackend::new(120, 30)).unwrap();
        terminal2.draw(|f| ui::draw(f, &app_expanded, &state_expanded)).unwrap();
        let text2 = buffer_text(terminal2.backend());
        assert!(text2.contains("▾ ↑ commit #7"), "expanded commit: down triangle + up arrow:\n{text2}");
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
