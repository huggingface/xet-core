// fn main() {
//     let stdin = io::stdin();
//     let mut handle = stdin.lock();

//     let file = "output.txt";
//     let mut file = std::fs::File::options()
//         .create(true)
//         .write(true)
//         .append(true)
//         .open(file)
//         .unwrap();

//     let mut buffer = String::new();
//     while handle.read_line(&mut buffer).is_ok() {
//         if buffer.is_empty() {
//             break;
//         }
//         file.write_all(buffer.as_bytes()).unwrap();
//         file.flush().unwrap();
//         buffer.clear();
//         println!("{{ }}");
//     }
//     drop(file);
// }

use std::io::Write;

use anyhow::Result;
use clap::Parser;
use futures::stream::StreamExt;
use git_xet::app::XetApp;
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;

async fn handle_signals(mut signals: Signals) {
    let pid = std::process::id();
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(format!("signallog.{}.txt", pid))
        .unwrap();

    while let Some(signal) = signals.next().await {
        match signal {
            SIGTERM | SIGINT | SIGQUIT | SIGABRT | SIGKILL => {
                file.write_all(
                    format!("[{}:{}] signal: {signal}\n", std::process::id(), chrono::Local::now().to_rfc3339())
                        .as_bytes(),
                )
                .unwrap();
                break;
            },
            _ => unreachable!(),
        }
    }
    file.write_all(
        format!("[{}:{}] quit on signal\n", std::process::id(), chrono::Local::now().to_rfc3339()).as_bytes(),
    )
    .unwrap();
}

#[tokio::main]
async fn main() -> Result<()> {
    let signals = Signals::new(&[SIGTERM, SIGINT, SIGQUIT, SIGABRT])?;
    let handle = signals.handle();

    let signals_task = tokio::spawn(handle_signals(signals));

    let app = XetApp::parse();

    app.run().await?;

    handle.close();
    signals_task.await?;

    Ok(())
}
