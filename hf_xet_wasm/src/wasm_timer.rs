use web_sys::console;

static PROFILING: bool = false;

pub struct Timer {
    name: String,
}

impl Timer {
    pub fn new(name: impl AsRef<str>) -> Timer {
        if PROFILING {
            Self::new_enforce_report(name)
        } else {
            Timer { name: "".to_owned() }
        }
    }

    pub fn new_enforce_report(name: impl AsRef<str>) -> Timer {
        let name = name.as_ref();
        console::time_with_label(name);
        Timer { name: name.to_owned() }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        if !self.name.is_empty() {
            console::time_end_with_label(&self.name);
        }
    }
}
