use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Gauge, Paragraph, Sparkline};
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
    // One fixed-height card per monitor, scrolled by monitor_row.
    const CARD: u16 = 7;
    let visible = (area.height / CARD).max(1) as usize;
    let first = app.monitor_row.min(monitors.len().saturating_sub(1)).saturating_sub(visible.saturating_sub(1));
    let mut y = area.y;
    for m in monitors.iter().skip(first).take(visible) {
        let card = Rect { x: area.x, y, width: area.width, height: CARD.min(area.y + area.height - y) };
        draw_monitor(f, card, m);
        y += CARD;
        if y >= area.y + area.height {
            break;
        }
    }
}

fn draw_monitor(f: &mut Frame, area: Rect, m: &MonitorSnapshot) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // title
            Constraint::Length(1), // permits gauge
            Constraint::Length(1), // success model
            Constraint::Length(1), // latency model
            Constraint::Length(1), // history label
            Constraint::Length(2), // sparkline
        ])
        .split(area);

    f.render_widget(
        Paragraph::new(format!(
            " {} — permits {}/{} ({} free) — bounds {}..{} — adj {} — sent {}",
            m.tag,
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

    let ratio = if m.total_permits == 0 { 0.0 } else { m.active_permits as f64 / m.total_permits as f64 };
    f.render_widget(
        Gauge::default().ratio(ratio.clamp(0.0, 1.0)).gauge_style(Style::default().fg(Color::Cyan)),
        rows[1],
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
    f.render_widget(Paragraph::new(success), rows[2]);

    let latency = match &m.latency {
        Some(l) => format!(
            " rtt {}ms ±{} · bw {}",
            l.predicted_max_rtt_ms.map(|v| format!("{v:.1}")).unwrap_or_else(|| "–".into()),
            l.rtt_standard_error_ms.map(|v| format!("{v:.0}")).unwrap_or_else(|| "–".into()),
            humanize_rate(l.predicted_bandwidth_bps),
        ),
        None => " latency: no samples yet".to_string(),
    };
    f.render_widget(Paragraph::new(latency), rows[3]);

    f.render_widget(
        Paragraph::new(Line::styled(
            format!(" limit history ({} adjustments)", m.limit_history.len()),
            Style::default().fg(Color::DarkGray),
        )),
        rows[4],
    );
    let data: Vec<u64> = m.limit_history.iter().map(|(_, v)| *v as u64).collect();
    f.render_widget(Sparkline::default().data(&data).style(Style::default().fg(Color::Green)), rows[5]);
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
        let app = App { page: Page::Concurrency, ..Default::default() };
        let mut terminal = Terminal::new(TestBackend::new(120, 32)).unwrap();
        terminal.draw(|f| ui::draw(f, &app, &state)).unwrap();
        let text = buffer_text(terminal.backend());
        assert!(text.contains("upload"), "{text}");
        assert!(text.contains("permits 14/16"), "{text}");
        assert!(text.contains("bounds 1..64"), "{text}");
        assert!(text.contains("success 0.94"), "{text}");
        assert!(text.contains("increase"), "recommendation shown: {text}");
        assert!(text.contains("rtt 412.5ms"), "{text}");
        assert!(text.contains("limit history"), "{text}");
    }
}
