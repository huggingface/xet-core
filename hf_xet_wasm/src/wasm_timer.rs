use web_sys::console;

static PROFILING: bool = false;

// when dropped the time since the timer is created until it is dropped
// is logged to the browser console.
pub struct ConsoleTimer {
    name: String,
}

impl ConsoleTimer {
    pub fn new(name: impl AsRef<str>) -> ConsoleTimer {
        if PROFILING {
            Self::new_enforce_report(name)
        } else {
            ConsoleTimer { name: "".to_owned() }
        }
    }

    pub fn new_enforce_report(name: impl AsRef<str>) -> ConsoleTimer {
        let name = name.as_ref();
        console::time_with_label(name);
        ConsoleTimer { name: name.to_owned() }
    }
}

impl Drop for ConsoleTimer {
    fn drop(&mut self) {
        if !self.name.is_empty() {
            console::time_end_with_label(&self.name);
        }
    }
}
