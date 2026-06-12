use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::widgets::Paragraph;
use xet_runtime::console::model::SnapshotResponse;

use crate::app::App;

pub fn draw(f: &mut Frame, area: Rect, _app: &App, _snap: &SnapshotResponse) {
    f.render_widget(Paragraph::new("(page arrives in a later task)"), area);
}
