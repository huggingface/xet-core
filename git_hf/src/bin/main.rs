use std::process::ExitCode;

use git_hf::GitHfApp;

fn main() -> ExitCode {
    let app = GitHfApp::parse_args();
    match app.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("git-hf: {error}");
            ExitCode::FAILURE
        },
    }
}
