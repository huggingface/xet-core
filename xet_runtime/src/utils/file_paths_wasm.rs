/// On wasm, path templates and expansion are not supported. The stub keeps the
/// surface needed by the config system (`new`, `template_string`) so the
/// `system_monitor` config group compiles on both targets; `evaluate()`
/// returns the input path unchanged.
#[derive(Debug, Clone)]
pub struct TemplatedPathBuf {
    template: std::path::PathBuf,
}

impl TemplatedPathBuf {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { template: path.into() }
    }

    pub fn evaluate(path: impl Into<std::path::PathBuf>) -> std::path::PathBuf {
        path.into()
    }

    pub fn template_string(&self) -> String {
        self.template.to_string_lossy().into_owned()
    }
}
