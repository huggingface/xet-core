use std::collections::HashSet;

use crossterm::event::KeyCode;
use xet_runtime::console::model::SnapshotResponse;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Page {
    #[default]
    Overview,
    Upload,
    Download,
    Concurrency,
}

/// Pane focus within a detail page (layout A): Main = files table,
/// SideTop = xorbs / term blocks, SideBottom = shards / prefetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Pane {
    #[default]
    Main,
    SideTop,
    SideBottom,
}

/// One selectable row on the overview page, in render order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverviewEntry {
    Session { session_idx: usize },
    Commit { session_idx: usize, commit_idx: usize },
    Group { session_idx: usize, group_idx: usize },
    /// In-flight or completed-ring file under an expanded commit.
    /// `table_row` is the row index in the upload detail page's combined files
    /// table (in-flight first, then completed ring) so Enter can preset
    /// `main_row` consistently with upload.rs rendering.
    UploadFile { session_idx: usize, commit_idx: usize, table_row: usize },
    /// File under an expanded download group; `table_row` same convention.
    DownloadFile { session_idx: usize, group_idx: usize, table_row: usize },
    /// "… +K more" sentinel when completed_files.len() > 5; Enter drills to
    /// the parent detail page.
    More { session_idx: usize, kind_commit: bool, idx: usize },
}

const MAX_INLINE_COMPLETED: usize = 5;

/// Flatten the snapshot into overview rows: each session header, then its
/// commits (live details first, then ended), then its groups (live then ended).
/// When a commit/group id is in `expanded`, emit its file rows immediately
/// after the parent row.
/// Render and key handling share this so selection always matches the screen.
pub fn overview_entries(snapshot: &SnapshotResponse, expanded: &HashSet<u64>) -> Vec<OverviewEntry> {
    let mut out = Vec::new();
    for (si, s) in snapshot.sessions.iter().enumerate() {
        out.push(OverviewEntry::Session { session_idx: si });

        let n_commits = s.upload_commit_details.len() + s.detail.ended_upload_commits.len();
        for ci in 0..n_commits {
            out.push(OverviewEntry::Commit { session_idx: si, commit_idx: ci });

            // Resolve which commit detail this is (live-then-ended order).
            let commit = if ci < s.upload_commit_details.len() {
                &s.upload_commit_details[ci]
            } else {
                &s.detail.ended_upload_commits[ci - s.upload_commit_details.len()]
            };

            if expanded.contains(&commit.id) {
                // In-flight files first.
                for (fi, _file) in commit.files.iter().enumerate() {
                    out.push(OverviewEntry::UploadFile {
                        session_idx: si,
                        commit_idx: ci,
                        table_row: fi,
                    });
                }
                // Up to 5 most-recent completed-ring entries (end of the ring = most recent).
                let n_completed = commit.completed_files.len();
                let skip = n_completed.saturating_sub(MAX_INLINE_COMPLETED);
                for (ring_pos, _) in commit.completed_files.iter().enumerate().skip(skip) {
                    out.push(OverviewEntry::UploadFile {
                        session_idx: si,
                        commit_idx: ci,
                        table_row: commit.files.len() + ring_pos,
                    });
                }
                // "… +K more" row when there are additional completed entries.
                if n_completed > MAX_INLINE_COMPLETED {
                    out.push(OverviewEntry::More {
                        session_idx: si,
                        kind_commit: true,
                        idx: ci,
                    });
                }
            }
        }

        let n_groups = s.download_group_details.len() + s.detail.ended_download_groups.len();
        for gi in 0..n_groups {
            out.push(OverviewEntry::Group { session_idx: si, group_idx: gi });

            let group = if gi < s.download_group_details.len() {
                &s.download_group_details[gi]
            } else {
                &s.detail.ended_download_groups[gi - s.download_group_details.len()]
            };

            if expanded.contains(&group.id) {
                for (fi, _file) in group.files.iter().enumerate() {
                    out.push(OverviewEntry::DownloadFile {
                        session_idx: si,
                        group_idx: gi,
                        table_row: fi,
                    });
                }
                let n_completed = group.completed_files.len();
                let skip = n_completed.saturating_sub(MAX_INLINE_COMPLETED);
                for (ring_pos, _) in group.completed_files.iter().enumerate().skip(skip) {
                    out.push(OverviewEntry::DownloadFile {
                        session_idx: si,
                        group_idx: gi,
                        table_row: group.files.len() + ring_pos,
                    });
                }
                if n_completed > MAX_INLINE_COMPLETED {
                    out.push(OverviewEntry::More {
                        session_idx: si,
                        kind_commit: false,
                        idx: gi,
                    });
                }
            }
        }
    }
    out
}

#[derive(Debug, Default)]
pub struct App {
    pub page: Page,
    pub pane: Pane,
    pub show_help: bool,
    pub paused: bool,
    pub should_quit: bool,
    pub session_idx: usize,
    pub commit_idx: usize,
    pub group_idx: usize,
    pub overview_row: usize,
    pub main_row: usize, // files table row on detail pages
    pub monitor_row: usize,
    pub show_idle_monitors: bool,
    /// Set of commit/group ids whose file lists are expanded on the overview.
    pub expanded: HashSet<u64>,
}

impl App {
    pub fn handle_key(&mut self, code: KeyCode, entries: &[OverviewEntry]) {
        if self.show_help {
            // Any key dismisses help except q which still quits.
            if code == KeyCode::Char('q') {
                self.should_quit = true;
            }
            self.show_help = false;
            return;
        }
        match code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Char('?') => self.show_help = true,
            KeyCode::Char('p') => self.paused = !self.paused,
            KeyCode::Char('1') => self.page = Page::Overview,
            KeyCode::Char('2') => self.enter_detail(Page::Upload),
            KeyCode::Char('3') => self.enter_detail(Page::Download),
            KeyCode::Char('4') => self.page = Page::Concurrency,
            KeyCode::Esc => {
                self.page = Page::Overview;
                self.pane = Pane::Main;
            },
            KeyCode::Tab => {
                if matches!(self.page, Page::Upload | Page::Download) {
                    self.pane = match self.pane {
                        Pane::Main => Pane::SideTop,
                        Pane::SideTop => Pane::SideBottom,
                        Pane::SideBottom => Pane::Main,
                    };
                }
            },
            KeyCode::Char('a') if self.page == Page::Concurrency => {
                self.show_idle_monitors = !self.show_idle_monitors;
            },
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1, entries.len()),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1, entries.len()),
            KeyCode::Char(' ') if self.page == Page::Overview => {
                // Toggle expanded state for a commit or group row.
                if let Some(entry) = entries.get(self.overview_row) {
                    // We need the snapshot to resolve the entity id, but
                    // handle_key only has entries — the caller must pass the
                    // id directly via the entry payload. We use a separate
                    // method that takes the snapshot so the caller can do the
                    // lookup. For now, the toggle is driven by
                    // toggle_expanded_for_entry which takes the snapshot.
                    let _ = entry; // handled via toggle_expanded_for_entry
                }
            },
            KeyCode::Enter => {
                if self.page == Page::Overview
                    && let Some(entry) = entries.get(self.overview_row)
                {
                    match *entry {
                        OverviewEntry::Session { session_idx } => self.session_idx = session_idx,
                        OverviewEntry::Commit { session_idx, commit_idx } => {
                            self.session_idx = session_idx;
                            self.commit_idx = commit_idx;
                            self.enter_detail(Page::Upload);
                        },
                        OverviewEntry::Group { session_idx, group_idx } => {
                            self.session_idx = session_idx;
                            self.group_idx = group_idx;
                            self.enter_detail(Page::Download);
                        },
                        OverviewEntry::UploadFile { session_idx, commit_idx, table_row } => {
                            self.session_idx = session_idx;
                            self.commit_idx = commit_idx;
                            self.enter_detail(Page::Upload);
                            self.main_row = table_row;
                        },
                        OverviewEntry::DownloadFile { session_idx, group_idx, table_row } => {
                            self.session_idx = session_idx;
                            self.group_idx = group_idx;
                            self.enter_detail(Page::Download);
                            self.main_row = table_row;
                        },
                        OverviewEntry::More { session_idx, kind_commit, idx } => {
                            self.session_idx = session_idx;
                            if kind_commit {
                                self.commit_idx = idx;
                                self.enter_detail(Page::Upload);
                            } else {
                                self.group_idx = idx;
                                self.enter_detail(Page::Download);
                            }
                        },
                    }
                }
            },
            _ => {},
        }
    }

    /// Toggle expand/collapse for the commit or group at the current overview
    /// row. Takes the snapshot to resolve entity ids. Called from the run loop
    /// (and tests) after acquiring the snapshot lock, instead of inside
    /// handle_key which doesn't have access to the snapshot.
    pub fn toggle_expanded(&mut self, entries: &[OverviewEntry], snapshot: &SnapshotResponse) {
        let Some(entry) = entries.get(self.overview_row) else { return };
        let id: Option<u64> = match *entry {
            OverviewEntry::Commit { session_idx, commit_idx } => {
                let s = &snapshot.sessions[session_idx];
                let commit = if commit_idx < s.upload_commit_details.len() {
                    &s.upload_commit_details[commit_idx]
                } else {
                    &s.detail.ended_upload_commits[commit_idx - s.upload_commit_details.len()]
                };
                Some(commit.id)
            },
            OverviewEntry::Group { session_idx, group_idx } => {
                let s = &snapshot.sessions[session_idx];
                let group = if group_idx < s.download_group_details.len() {
                    &s.download_group_details[group_idx]
                } else {
                    &s.detail.ended_download_groups[group_idx - s.download_group_details.len()]
                };
                Some(group.id)
            },
            _ => None,
        };
        if let Some(id) = id {
            if self.expanded.contains(&id) {
                self.expanded.remove(&id);
            } else {
                self.expanded.insert(id);
            }
        }
    }

    fn enter_detail(&mut self, page: Page) {
        self.page = page;
        self.pane = Pane::Main;
        self.main_row = 0;
    }

    fn move_selection(&mut self, delta: i64, overview_len: usize) {
        let bump = |v: &mut usize, max: usize| {
            let next = (*v as i64 + delta).max(0) as usize;
            *v = if max == 0 { 0 } else { next.min(max - 1) };
        };
        match self.page {
            // Detail-pane and monitor rows are clamped against live data at
            // render time; here they only need the saturating move.
            Page::Overview => bump(&mut self.overview_row, overview_len),
            Page::Concurrency => bump(&mut self.monitor_row, usize::MAX),
            Page::Upload | Page::Download => match self.pane {
                Pane::Main => bump(&mut self.main_row, usize::MAX),
                // v1: pane focus is visual; side panes render whole and have
                // no independent selection. (Per-pane scrolling is deferred.)
                Pane::SideTop | Pane::SideBottom => {},
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::sample_snapshot;

    fn entries() -> Vec<OverviewEntry> {
        overview_entries(
            &{
                // Minimal inline snapshot: one session, one commit, one group.
                // For tests that don't need the fixture snapshot we keep this
                // so existing assertions over row indices stay unchanged.
                use xet_runtime::console::model::*;
                SnapshotResponse {
                    as_of: 0,
                    process: ProcessInfo {
                        as_of: 0,
                        pid: 1,
                        argv: vec![],
                        start_time_ms: 0,
                        version: "0".into(),
                        n_active_sessions: 1,
                    },
                    sessions: vec![SessionFull {
                        detail: SessionDetail {
                            as_of: 0,
                            id: "test-session".into(),
                            state: SessionState::Active,
                            created_at: 0,
                            config: vec![],
                            monitors: vec![],
                            upload_commits: vec![],
                            ended_upload_commits: vec![],
                            download_groups: vec![],
                            ended_download_groups: vec![],
                        },
                        upload_commit_details: vec![UploadCommitDetail {
                            as_of: 0,
                            id: 1,
                            state: UploadCommitState::Active,
                            created_at: 0,
                            endpoint: None,
                            progress: None,
                            dedup: DedupSnapshot::default(),
                            files: vec![],
                            completed_files: vec![],
                            file_counts: FileCounts::default(),
                            xorbs: XorbsSnapshot::default(),
                            shards: vec![],
                        }],
                        download_group_details: vec![DownloadGroupDetail {
                            as_of: 0,
                            id: 2,
                            kind: DownloadGroupKind::Files,
                            state: DownloadGroupState::Active,
                            created_at: 0,
                            endpoint: None,
                            progress: None,
                            files: vec![],
                            completed_files: vec![],
                            file_counts: FileCounts::default(),
                        }],
                    }],
                }
            },
            &HashSet::new(),
        )
    }

    #[test]
    fn quit_help_pause_toggle() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('?'), &entries());
        assert!(app.show_help);
        app.handle_key(KeyCode::Char('?'), &entries());
        assert!(!app.show_help);
        app.handle_key(KeyCode::Char('p'), &entries());
        assert!(app.paused);
        app.handle_key(KeyCode::Char('q'), &entries());
        assert!(app.should_quit);
    }

    #[test]
    fn overview_drill_into_commit_and_back() {
        let mut app = App::default();
        assert_eq!(app.page, Page::Overview);
        app.handle_key(KeyCode::Char('j'), &entries()); // row 1 = the commit
        app.handle_key(KeyCode::Enter, &entries());
        assert_eq!(app.page, Page::Upload);
        assert_eq!(app.session_idx, 0);
        assert_eq!(app.commit_idx, 0);
        app.handle_key(KeyCode::Esc, &entries());
        assert_eq!(app.page, Page::Overview);
    }

    #[test]
    fn overview_drill_into_group() {
        let mut app = App::default();
        app.overview_row = 2;
        app.handle_key(KeyCode::Enter, &entries());
        assert_eq!(app.page, Page::Download);
        assert_eq!(app.group_idx, 0);
    }

    #[test]
    fn number_keys_jump_pages_and_tab_cycles_panes() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('4'), &entries());
        assert_eq!(app.page, Page::Concurrency);
        app.handle_key(KeyCode::Char('2'), &entries());
        assert_eq!(app.page, Page::Upload);
        assert_eq!(app.pane, Pane::Main);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::SideTop);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::SideBottom);
        app.handle_key(KeyCode::Tab, &entries());
        assert_eq!(app.pane, Pane::Main);
        app.handle_key(KeyCode::Char('1'), &entries());
        assert_eq!(app.page, Page::Overview);
    }

    #[test]
    fn toggle_idle_monitors_only_on_concurrency_page() {
        let mut app = App::default();
        // On Overview page 'a' is a no-op.
        assert_eq!(app.page, Page::Overview);
        app.handle_key(KeyCode::Char('a'), &entries());
        assert!(!app.show_idle_monitors);
        // Switch to Concurrency; 'a' toggles on.
        app.page = Page::Concurrency;
        app.handle_key(KeyCode::Char('a'), &entries());
        assert!(app.show_idle_monitors);
        // Second press toggles back off.
        app.handle_key(KeyCode::Char('a'), &entries());
        assert!(!app.show_idle_monitors);
        // On Upload page 'a' is still a no-op.
        app.page = Page::Upload;
        app.handle_key(KeyCode::Char('a'), &entries());
        assert!(!app.show_idle_monitors);
    }

    #[test]
    fn selection_moves_saturate_at_zero() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('k'), &entries());
        assert_eq!(app.overview_row, 0);
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries()); // clamped to last entry (2)
        assert_eq!(app.overview_row, 2);
    }

    /// SPACE on a commit row toggles the commit id (7 in fixture) into
    /// `expanded`, and the next `overview_entries` call emits file rows
    /// immediately after the commit row.
    #[test]
    fn space_expands_commit_and_emits_file_rows() {
        let snap = sample_snapshot();
        let mut app = App::default();
        // Fixture: row 0 = session, row 1 = live commit (id 7), row 2 = ended
        // commit (id 5), row 3 = download group.
        let collapsed_entries = overview_entries(&snap, &app.expanded);
        // Sanity: collapsed has exactly 4 rows (session + 2 commits + 1 group).
        assert_eq!(collapsed_entries.len(), 4);

        // Move to row 1 (the live commit).
        app.overview_row = 1;
        app.toggle_expanded(&collapsed_entries, &snap);

        // id 7 should now be in expanded.
        assert!(app.expanded.contains(&7), "expanded should contain commit id 7");

        // Re-compute entries with the now-expanded set.
        let expanded_entries = overview_entries(&snap, &app.expanded);
        // Fixture commit id 7 has 2 in-flight files and 1 completed-ring file
        // (≤ 5, so no More row). Extra rows = 3; total = 4 + 3 = 7.
        assert_eq!(expanded_entries.len(), 7, "file rows injected: {expanded_entries:?}");

        // Row 2 should be an UploadFile (first in-flight file, table_row 0).
        assert_eq!(
            expanded_entries[2],
            OverviewEntry::UploadFile { session_idx: 0, commit_idx: 0, table_row: 0 }
        );
        // Row 4 should be the completed-ring file (table_row = files.len() + ring_pos = 2 + 0).
        assert_eq!(
            expanded_entries[4],
            OverviewEntry::UploadFile { session_idx: 0, commit_idx: 0, table_row: 2 }
        );

        // Toggle again: should collapse.
        app.toggle_expanded(&expanded_entries, &snap);
        assert!(!app.expanded.contains(&7), "should be collapsed again");
        let re_collapsed = overview_entries(&snap, &app.expanded);
        assert_eq!(re_collapsed.len(), 4);
    }

    /// Enter on an UploadFile entry drills to Page::Upload with main_row set
    /// to the entry's table_row.
    #[test]
    fn enter_on_upload_file_row_sets_main_row() {
        let snap = sample_snapshot();
        let mut app = App::default();
        // Expand commit id 7 so file rows are present.
        app.expanded.insert(7);
        let expanded_entries = overview_entries(&snap, &app.expanded);

        // Row 2 is the first in-flight UploadFile with table_row 0.
        // Row 3 is the second in-flight UploadFile with table_row 1.
        // Pick table_row 1 (row 3).
        app.overview_row = 3;
        let entry = &expanded_entries[3];
        assert_eq!(*entry, OverviewEntry::UploadFile { session_idx: 0, commit_idx: 0, table_row: 1 });
        app.handle_key(KeyCode::Enter, &expanded_entries);
        assert_eq!(app.page, Page::Upload);
        assert_eq!(app.session_idx, 0);
        assert_eq!(app.commit_idx, 0);
        assert_eq!(app.main_row, 1);
    }
}
