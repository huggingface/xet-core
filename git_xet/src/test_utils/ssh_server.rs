use std::io;
use std::sync::Arc;

use anyhow::anyhow;
use rand_core::OsRng;
use russh::keys::{Certificate, *};
use russh::server::{Msg, Server as _, Session};
use russh::*;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use crate::auth::{GitLFSAuthentationResponseHeader, GitLFSAuthenticateResponse};

/// Starts a lightweight SSH server intended for tests and local/manual debugging.
///
/// The server:
/// - binds to the given port (use `Some(0)` or `None` to let the OS pick a free port),
/// - accepts public-key and OpenSSH certificate authentication,
/// - handles `exec` requests and specifically responds to the `git-lfs-authenticate <repo> <operation>` command with a
///   small JSON payload and then closes the channel,
/// - runs on the tokio runtime and returns a JoinHandle for the spawned server task so the caller can abort or await it
///   when finished.
///
/// Arguments:
/// - `port`: Option<u16> — Some(port) to bind to that port, None (or Some(0)) to bind to an OS-assigned port.
///
/// Returns:
/// - io::Result<(u16, JoinHandle<io::Result<()>>)> where the first element is the actual port the listener is bound to
///   and the second is a handle to the background task running the server.
///
/// Example (async context):
/// ```ignore
/// 
/// // Start server on any free port
/// let (port, server_task) = start_local_ssh_server(None).await?;
/// println!("Test SSH server listening on port {}", port);
///
/// // ... run test client actions against localhost:port ...
///
/// // Stop the server: abort the background task and optionally await it.
/// server_task.abort();
/// let _ = server_task.await;
/// ```
pub async fn start_local_ssh_server(port: Option<u16>) -> io::Result<(u16, JoinHandle<io::Result<()>>)> {
    let config = russh::server::Config {
        inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
        auth_rejection_time: std::time::Duration::from_secs(3),
        auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
        keys: vec![russh::keys::PrivateKey::random(&mut OsRng, russh::keys::Algorithm::Ed25519).unwrap()],
        preferred: Preferred { ..Preferred::default() },
        ..Default::default()
    };
    let config = Arc::new(config);
    let mut sh = ServerImpl;

    let socket = TcpListener::bind(("0.0.0.0", port.unwrap_or_default())).await.unwrap();
    let port = socket.local_addr()?.port();

    Ok((
        port,
        tokio::spawn(async move {
            let server = sh.run_on_socket(config, &socket);

            server.await
        }),
    ))
}

#[derive(Clone)]
struct ServerImpl;

impl server::Server for ServerImpl {
    type Handler = Self;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self {
        self.clone()
    }

    fn handle_session_error(&mut self, _error: <Self::Handler as russh::server::Handler>::Error) {
        eprintln!("Session error: {_error:#?}");
    }
}

impl server::Handler for ServerImpl {
    type Error = russh::Error;

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        _session: &mut Session,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        _term: &str,
        _col_width: u32,
        _row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        session.close(channel)?;
        Ok(())
    }

    async fn shell_request(&mut self, channel: ChannelId, session: &mut Session) -> Result<(), Self::Error> {
        session.channel_failure(channel)?;
        session.close(channel)?;
        Ok(())
    }

    async fn auth_none(&mut self, _: &str) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::Accept)
    }

    async fn auth_publickey(&mut self, _: &str, _key: &ssh_key::PublicKey) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::Accept)
    }

    async fn auth_openssh_certificate(
        &mut self,
        _user: &str,
        _certificate: &Certificate,
    ) -> Result<server::Auth, Self::Error> {
        Ok(server::Auth::Accept)
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let request = String::from_utf8_lossy(data);
        let request: Vec<_> = request.split_ascii_whitespace().collect();
        let response = if let Some(command) = request.first() {
            match *command {
                "git-lfs-authenticate" => self.git_lfs_authenticate(request).unwrap_or_else(|e| e.to_string()),
                _ => "invalid command".into(),
            }
        } else {
            "invalid request".into()
        };

        let data = CryptoVec::from(response);
        session.data(channel, data)?;
        session.close(channel)?;
        Ok(())
    }
}

impl ServerImpl {
    fn git_lfs_authenticate(&self, request: Vec<&str>) -> anyhow::Result<String> {
        let Some(repo_id) = request.get(1) else {
            return Err(anyhow!("invalid request, missing repo id"));
        };
        let Some(operation) = request.get(2) else {
            return Err(anyhow!("invalid request, missing operation"));
        };
        if !matches!(*operation, "upload" | "download") {
            return Err(anyhow!("invalid request, unrecognized operation"));
        }
        let response = GitLFSAuthenticateResponse {
            header: GitLFSAuthentationResponseHeader {
                authorization: "Basic 38vcn391nv==".into(),
            },
            href: format!("https://huggingface.co/{repo_id}.git/info/lfs"),
            expires_in: 3600,
        };

        let json_str = serde_json::to_string(&response)?;

        Ok(json_str)
    }
}

#[cfg(test)]
mod tests {
    use super::start_local_ssh_server;

    #[tokio::test]
    #[ignore = "start an ssh server for manual testing"]
    async fn run_server() {
        let (_port, task) = start_local_ssh_server(Some(2222)).await.unwrap();
        let _ret = task.await;
    }
}
