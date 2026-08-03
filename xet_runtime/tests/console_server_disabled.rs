#![cfg(feature = "console")]

#[test]
fn invalid_port_disables_console() {
    unsafe { std::env::set_var("XET_CONSOLE_PORT", "not-a-number") };
    xet_runtime::console::server::ensure_started();
    assert!(xet_runtime::console::server::bound_addr().is_none());
}
