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

use anyhow::Result;
use clap::Parser;
use hf_xet_git_integration::app::XetApp;

#[tokio::main]
async fn main() -> Result<()> {
    let app = XetApp::parse();

    app.run().await?;

    Ok(())
}
