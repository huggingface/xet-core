mod data;

fn main() {
    #[cfg(target_arch = "wasm32")]
    {
        console_log::init().unwrap();
        console_error_panic_hook::set_once();
    }

    #[cfg(not(target_arch = "wasm32"))]
    env_logger::init_from_env(env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"));

    log::info!("Starting init wasm_xet...");

    log::info!("Done");
}
