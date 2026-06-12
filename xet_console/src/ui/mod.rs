pub mod widgets;

pub mod concurrency;
pub mod download;
pub mod overview;
pub mod upload;

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use crate::app::{App, Page};
use crate::poll::{PollState, now_ms};

pub fn draw(f: &mut Frame, app: &App, state: &PollState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1), Constraint::Length(1)])
        .split(f.area());

    draw_header(f, chunks[0], app, state);

    match &state.snapshot {
        None => {
            let msg = match &state.last_error {
                Some(e) => format!("no data — {e}"),
                None => "no data — waiting for first poll…".to_string(),
            };
            f.render_widget(Paragraph::new(msg).style(Style::default().fg(Color::Red)), chunks[1]);
        },
        Some(snap) => match app.page {
            Page::Overview => overview::draw(f, chunks[1], app, snap, &state.ended_sessions),
            Page::Upload => upload::draw(f, chunks[1], app, snap),
            Page::Download => download::draw(f, chunks[1], app, snap),
            Page::Concurrency => concurrency::draw(f, chunks[1], app, snap),
        },
    }

    draw_key_bar(f, chunks[2]);

    if app.show_help {
        draw_help(f);
    }
}

fn draw_header(f: &mut Frame, area: Rect, app: &App, state: &PollState) {
    let age = state.last_success_ms.map(|t| now_ms().saturating_sub(t));
    let status = if app.paused {
        "PAUSED".to_string()
    } else if let Some(e) = &state.last_error {
        format!("DISCONNECTED: {e}")
    } else {
        match age {
            Some(ms) if ms > 3000 => format!("stale {}s", ms / 1000),
            Some(_) => "live".to_string(),
            None => "connecting".to_string(),
        }
    };
    let pid = state.snapshot.as_ref().map(|s| s.process.pid).unwrap_or(0);
    let line = format!(" xet-console — pid {pid} — {status}");
    let style = if app.paused || state.last_error.is_some() {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().add_modifier(Modifier::BOLD)
    };
    f.render_widget(Paragraph::new(line).style(style), area);
}

fn draw_key_bar(f: &mut Frame, area: Rect) {
    f.render_widget(
        Paragraph::new(
            " [1]overview [2]upload [3]download [4]concurrency  ↑↓/jk ⏎ drill  tab pane  esc back  p pause  ? help  q quit",
        )
        .style(Style::default().fg(Color::DarkGray)),
        area,
    );
}

fn draw_help(f: &mut Frame) {
    let area = centered_rect(60, 14, f.area());
    f.render_widget(Clear, area);
    let text = "\
  1/2/3/4   jump to page
  ↑↓ or jk  move selection
  ⏎         drill into selected item
  tab       cycle pane focus (detail pages)
  esc       back to overview
  p         pause/resume polling
  q         quit

  any key closes this help";
    f.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title(" help ")),
        area,
    );
}

fn centered_rect(width: u16, height: u16, parent: Rect) -> Rect {
    let w = width.min(parent.width);
    let h = height.min(parent.height);
    Rect {
        x: parent.x + (parent.width - w) / 2,
        y: parent.y + (parent.height - h) / 2,
        width: w,
        height: h,
    }
}
