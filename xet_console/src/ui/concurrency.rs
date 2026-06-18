use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Sparkline};
use xet_runtime::console::model::{AdjustmentRecommendation, MonitorSnapshot, SnapshotResponse};

use crate::app::App;
use crate::ui::widgets::*;

pub fn draw(f: &mut Frame, area: Rect, app: &App, snap: &SnapshotResponse) {
    let Some(session) = snap.sessions.get(app.session_idx) else {
        f.render_widget(Paragraph::new("no session selected — esc to go back"), area);
        return;
    };
    let monitors = &session.detail.monitors;
    if monitors.is_empty() {
        f.render_widget(
            Paragraph::new("no live concurrency monitors (they exist only while a commit/group's client is alive)"),
            area,
        );
        return;
    }

    let shown: Vec<&MonitorSnapshot> = monitors
        .iter()
        .filter(|m| app.show_idle_monitors || !monitor_is_idle(m))
        .collect();
    let hidden = monitors.len() - shown.len();

    // Reserve the last line for the footer when there is something to say.
    let has_footer = hidden > 0 || (app.show_idle_monitors && monitors.iter().any(monitor_is_idle));
    let (cards_area, footer_area) = if has_footer && area.height > 1 {
        let split = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(1), Constraint::Length(1)])
            .split(area);
        (split[0], Some(split[1]))
    } else {
        (area, None)
    };

    if shown.is_empty() {
        // All monitors are idle and hidden.
        f.render_widget(Paragraph::new("all monitors idle"), cards_area);
    } else {
        // One fixed-height card per monitor, scrolled by monitor_row.
        // 2 border rows + 6 content rows (permits / success / latency /
        // history-label / sparkline[2]).
        const CARD: u16 = 8;
        let visible = (cards_area.height / CARD).max(1) as usize;
        let first = app
            .monitor_row
            .min(shown.len().saturating_sub(1))
            .saturating_sub(visible.saturating_sub(1));
        let mut y = cards_area.y;
        for m in shown.iter().skip(first).take(visible) {
            let card = Rect {
                x: cards_area.x,
                y,
                width: cards_area.width,
                height: CARD.min(cards_area.y + cards_area.height - y),
            };
            draw_monitor(f, card, m);
            y += CARD;
            if y >= cards_area.y + cards_area.height {
                break;
            }
        }
    }

    if let Some(footer) = footer_area {
        let (msg, color) = if app.show_idle_monitors && monitors.iter().any(monitor_is_idle) {
            (" showing idle monitors — press a to hide".to_string(), Color::DarkGray)
        } else {
            (format!(" {hidden} idle monitor(s) hidden — press a to show"), Color::DarkGray)
        };
        f.render_widget(Paragraph::new(msg).style(Style::default().fg(color)), footer);
    }
}

fn draw_monitor(f: &mut Frame, area: Rect, m: &MonitorSnapshot) {
    // Outer bordered block titled with the tag.
    let block = Block::default().borders(Borders::ALL).title(format!(" {} ", m.tag));
    let inner = block.inner(area);
    f.render_widget(block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // permits/bounds/adj/sent
            Constraint::Length(1), // success model
            Constraint::Length(1), // latency model
            Constraint::Length(1), // history label (allowed-permit scale)
            Constraint::Length(2), // sparkline
        ])
        .split(inner);

    f.render_widget(
        Paragraph::new(format!(
            " permits {}/{} ({} free) — bounds {}..{} — adj {} — sent {}",
            m.active_permits,
            m.total_permits,
            m.available_permits,
            m.bounds.min,
            m.bounds.max,
            if m.adjustment_enabled { "auto" } else { "fixed" },
            humanize_bytes(m.bytes_sent),
        ))
        .style(Style::default().add_modifier(Modifier::BOLD)),
        rows[0],
    );

    let success = match &m.success {
        Some(s) => format!(
            " success {:.2} (↑>{:.2} ↓<{:.2}) rec {}",
            s.success_ratio,
            s.thresholds.increase,
            s.thresholds.decrease,
            match s.recommended_adjustment {
                AdjustmentRecommendation::Increase => "increase",
                AdjustmentRecommendation::Hold => "hold",
                AdjustmentRecommendation::Decrease => "decrease",
            }
        ),
        None => " success: no samples yet".to_string(),
    };
    f.render_widget(Paragraph::new(success), rows[1]);

    let latency = match &m.latency {
        Some(l) => format!(
            " rtt {}ms ±{} · bw {}",
            l.predicted_max_rtt_ms.map(|v| format!("{v:.1}")).unwrap_or_else(|| "–".into()),
            l.rtt_standard_error_ms.map(|v| format!("{v:.0}")).unwrap_or_else(|| "–".into()),
            humanize_rate(l.predicted_bandwidth_bps),
        ),
        None => " latency: no samples yet".to_string(),
    };
    f.render_widget(Paragraph::new(latency), rows[2]);

    // The sparkline shows the allowed-permit limit over the adjustment history.
    // It auto-scales with no axis, so name the series and print the scale
    // (current / observed range / hard cap) alongside it.
    let now = m.total_permits;
    let cap = m.bounds.max;
    let n = m.limit_history.len();
    let lo = m.limit_history.iter().map(|(_, v)| *v).min().unwrap_or(now);
    let hi = m.limit_history.iter().map(|(_, v)| *v).max().unwrap_or(now);
    f.render_widget(
        Paragraph::new(Line::styled(
            format!(" allowed permits · now {now} · min {lo} / max {hi} · cap {cap} ({n} adj)"),
            Style::default().fg(Color::DarkGray),
        )),
        rows[3],
    );
    let data: Vec<u64> = m.limit_history.iter().map(|(_, v)| *v as u64).collect();
    f.render_widget(Sparkline::default().data(&data).style(Style::default().fg(Color::Green)), rows[4]);
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
    fn concurrency_page_shows_monitor_models_and_history() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App {
            page: Page::Concurrency,
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("upload"), "{text}");
        assert!(text.contains("permits 14/16"), "{text}");
        assert!(text.contains("bounds 1..64"), "{text}");
        assert!(text.contains("success 0.94"), "{text}");
        assert!(text.contains("increase"), "recommendation shown: {text}");
        assert!(text.contains("rtt 412.5ms"), "{text}");
        // Sparkline is labeled "allowed permits" with a readable scale, and the
        // utilization gauge (and its percentage label) is gone.
        assert!(text.contains("allowed permits"), "sparkline labeled: {text}");
        assert!(text.contains("now 16"), "current allowed shown: {text}");
        assert!(text.contains("min 8 / max 16"), "limit range shown: {text}");
        assert!(text.contains("cap 64"), "hard cap shown: {text}");
        assert!(!text.contains("limit history"), "old label gone: {text}");
        assert!(!text.contains("88%") && !text.contains("87%"), "utilization gauge removed: {text}");
        // Idle "download" monitor must be hidden by default.
        assert!(text.contains("idle monitor"), "idle footer present: {text}");
        assert!(!text.contains(" download "), "idle card must be absent: {text}");
    }

    #[test]
    fn concurrency_page_shows_idle_when_toggled() {
        let state = PollState {
            snapshot: Some(sample_snapshot()),
            last_success_ms: Some(0),
            ..Default::default()
        };
        let app = App {
            page: Page::Concurrency,
            show_idle_monitors: true,
            ..Default::default()
        };
        let mut terminal = Terminal::new(TestBackend::new(120, 40)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        // Idle card must now be visible.
        assert!(text.contains(" download "), "idle card title shown: {text}");
        // Footer prompts to hide.
        assert!(text.contains("press a to hide"), "hide footer present: {text}");
    }
}
