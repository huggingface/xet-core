use std::process::ExitCode;

use git_hf::{GIT_HF_PROGRAM, GitHfApp};

fn main() -> ExitCode {
    let app = GitHfApp::parse_args();
    match app.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{GIT_HF_PROGRAM}: {error}");
            ExitCode::FAILURE
        },
    }
}
