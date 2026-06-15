mod app;
mod client;
#[cfg(test)]
mod fixtures;
mod poll;
#[cfg(test)]
mod smoke;
mod ui;

use std::io::{self, IsTerminal};
use std::time::Duration;

use clap::Parser;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode};
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

/// Restores the terminal on drop; best-effort (errors ignored — we're exiting).
struct TerminalRestore;

impl Drop for TerminalRestore {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let base = resolve_base(args.target.as_deref(), args.port);

    let shared = std::sync::Arc::new(poll::Shared::default());
    let shutdown = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let poller =
        poll::spawn_poller(client::ConsoleClient::new(base.clone()), shared.clone(), args.interval, shutdown.clone());

    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        default_hook(info);
    }));

    if !io::stdout().is_terminal() {
        anyhow::bail!("xet-console needs an interactive terminal (stdout is not a tty)");
    }

    enable_raw_mode()?;
    execute!(io::stdout(), EnterAlternateScreen)?;
    let _restore = TerminalRestore;
    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))?;

    let result = run(&mut terminal, &shared);

    drop(_restore);
    shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = poller.join();
    result
}

fn run(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, shared: &poll::Shared) -> anyhow::Result<()> {
    let mut app = app::App::default();
    loop {
        {
            let state = shared.lock();
            terminal.draw(|f| ui::draw(f, &app, &state))?;
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
            && key.kind == KeyEventKind::Press
        {
            let state = shared.lock();
            let entries = state
                .snapshot
                .as_ref()
                .map(|s| app::overview_entries(s, &app.expanded))
                .unwrap_or_default();
            // Expand/collapse keys resolve entity ids, so they need the
            // snapshot and are handled here rather than in handle_key.
            match key.code {
                KeyCode::Char(' ') => {
                    if let Some(snap) = &state.snapshot {
                        app.toggle_expanded(&entries, snap);
                    }
                },
                KeyCode::Right if app.page == app::Page::Overview => {
                    if let Some(snap) = &state.snapshot {
                        app.expand_current(&entries, snap);
                    }
                },
                KeyCode::Left if app.page == app::Page::Overview => {
                    if let Some(snap) = &state.snapshot {
                        app.collapse_current(&entries, snap);
                    }
                },
                _ => app.handle_key(key.code, &entries),
            }
            drop(state);
            shared.lock().paused = app.paused;
        }
        if app.should_quit {
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
        assert_eq!(resolve_base(Some("http://10.0.0.5:6660/"), Some(1)), "http://10.0.0.5:6660");
        assert_eq!(resolve_base(Some("myhost:6660"), None), "http://myhost:6660");
    }
}
