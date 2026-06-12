mod app;
mod client;
mod poll;
mod ui;
#[cfg(test)]
mod fixtures;

use std::io;
use std::time::Duration;

use clap::Parser;
use client::ConsoleClient;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

const DEFAULT_BASE: &str = "http://127.0.0.1:6660";

/// Terminal UI for the xet-console live observability API.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Base URL (http://host:port), bare host:port, or bare port of the console server.
    target: Option<String>,
    /// Port on 127.0.0.1 (overridden by `target` if given).
    #[arg(long)]
    port: Option<u16>,
    /// Poll interval (e.g. 500ms, 2s).
    #[arg(long, default_value = "500ms", value_parser = humantime::parse_duration)]
    interval: Duration,
}

fn resolve_base(target: Option<&str>, port: Option<u16>) -> String {
    if let Some(t) = target {
        let t = t.trim().trim_end_matches('/');
        if t.starts_with("http://") || t.starts_with("https://") {
            return t.to_string();
        }
        if let Ok(p) = t.parse::<u16>() {
            return format!("http://127.0.0.1:{p}");
        }
        return format!("http://{t}");
    }
    if let Some(p) = port {
        return format!("http://127.0.0.1:{p}");
    }
    DEFAULT_BASE.to_string()
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let base = resolve_base(args.target.as_deref(), args.port);

    // Restore the terminal even if we panic mid-draw.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let client = ConsoleClient::new(base.clone());
    let result = run(&mut terminal, &client, args.interval);

    disable_raw_mode()?;
    execute!(io::stdout(), LeaveAlternateScreen)?;
    result
}

fn run(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    client: &ConsoleClient,
    _interval: Duration,
) -> anyhow::Result<()> {
    loop {
        terminal.draw(|f| {
            use ratatui::widgets::Paragraph;
            f.render_widget(
                Paragraph::new(format!("xet-console — connecting to {}  (q to quit)", client.base())),
                f.area(),
            );
        })?;
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
            && key.code == KeyCode::Char('q')
        {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_resolution_rules() {
        assert_eq!(resolve_base(None, None), "http://127.0.0.1:6660");
        assert_eq!(resolve_base(None, Some(7000)), "http://127.0.0.1:7000");
        assert_eq!(resolve_base(Some("8123"), None), "http://127.0.0.1:8123");
        assert_eq!(
            resolve_base(Some("http://10.0.0.5:6660/"), Some(1)),
            "http://10.0.0.5:6660"
        );
        assert_eq!(resolve_base(Some("myhost:6660"), None), "http://myhost:6660");
    }
}
