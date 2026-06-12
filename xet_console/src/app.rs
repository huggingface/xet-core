#![allow(dead_code)]

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
}

/// Flatten the snapshot into overview rows: each session header, then its
/// commits (live details first, then ended), then its groups (live then ended).
/// Render and key handling share this so selection always matches the screen.
pub fn overview_entries(snapshot: &SnapshotResponse) -> Vec<OverviewEntry> {
    let mut out = Vec::new();
    for (si, s) in snapshot.sessions.iter().enumerate() {
        out.push(OverviewEntry::Session { session_idx: si });
        let n_commits = s.upload_commit_details.len() + s.detail.ended_upload_commits.len();
        for ci in 0..n_commits {
            out.push(OverviewEntry::Commit { session_idx: si, commit_idx: ci });
        }
        let n_groups = s.download_group_details.len() + s.detail.ended_download_groups.len();
        for gi in 0..n_groups {
            out.push(OverviewEntry::Group { session_idx: si, group_idx: gi });
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
            KeyCode::Char('j') | KeyCode::Down => self.move_selection(1, entries.len()),
            KeyCode::Char('k') | KeyCode::Up => self.move_selection(-1, entries.len()),
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
                    }
                }
            },
            _ => {},
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

    fn entries() -> Vec<OverviewEntry> {
        vec![
            OverviewEntry::Session { session_idx: 0 },
            OverviewEntry::Commit { session_idx: 0, commit_idx: 0 },
            OverviewEntry::Group { session_idx: 0, group_idx: 0 },
        ]
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
    fn selection_moves_saturate_at_zero() {
        let mut app = App::default();
        app.handle_key(KeyCode::Char('k'), &entries());
        assert_eq!(app.overview_row, 0);
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries());
        app.handle_key(KeyCode::Down, &entries()); // clamped to last entry (2)
        assert_eq!(app.overview_row, 2);
    }
}
