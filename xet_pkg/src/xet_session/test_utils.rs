//! Shared test-only helpers for `xet_session`'s unit tests.

#[cfg(all(test, not(target_family = "wasm")))]
pub(crate) mod stack_regression {
    //! Regression-test helper guarding against a large, generic future crossing an
    //! `.await`-spanning function boundary by value without indirection — the compiler
    //! then bakes the full, un-erased type into every generator frame it passes through,
    //! which can silently balloon a state machine's size until it overflows a thread's
    //! default stack (small on some platforms, e.g. ~524 KiB for non-main threads on
    //! macOS).
    //!
    //! A stack overflow aborts the whole process (it can't be caught with
    //! `catch_unwind`), so [`run`] executes `build_in_small_stack` in a *child process*
    //! with an explicitly small stack: if it regresses, only the child aborts and the
    //! caller's `#[test]` fails cleanly, instead of taking down the entire test binary.

    const MARKER_ENV: &str = "XET_STACK_REGRESSION_CHILD";
    // 128 KiB is tight enough to actually catch the regression on macOS/Linux (verified:
    // fails at this size without the fix, passes with it). Windows needs more baseline
    // stack for the same tokio runtime + thread setup regardless of this bug, so it gets
    // a separate, larger bound.
    #[cfg(not(windows))]
    const STACK_SIZE: usize = 128 * 1024;
    #[cfg(windows)]
    const STACK_SIZE: usize = 512 * 1024;

    /// Call from a `#[test]` fn. `test_name` must be that test's own (unique) name —
    /// used to re-invoke just this test in a child process. `build_in_small_stack`
    /// performs the actual risky call and only ever runs inside the child, on a thread
    /// with an explicit small stack.
    pub(crate) fn run(test_name: &'static str, build_in_small_stack: impl FnOnce() + Send + 'static) {
        if std::env::var_os(MARKER_ENV).is_some() {
            let t = std::thread::Builder::new()
                .stack_size(STACK_SIZE)
                .spawn(build_in_small_stack)
                .unwrap();
            t.join().unwrap();
            return;
        }

        let exe = std::env::current_exe().unwrap();
        let output = std::process::Command::new(exe)
            .arg(test_name)
            .arg("--test-threads=1")
            .env(MARKER_ENV, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "child process exited abnormally ({:?}) - likely a stack overflow.\n{stdout}",
            output.status
        );
        // libtest exits 0 even when a filter matches no tests, so a typo'd or renamed
        // `test_name` would otherwise make this helper silently pass without the risky
        // call ever running. Require exactly the one expected test to have run.
        assert!(
            stdout.contains("1 passed"),
            "expected the child process to run exactly one test matching {test_name:?}, but it \
             didn't (the test name filter may not have matched anything, or matched more than \
             one test).\n{stdout}"
        );
    }
}
