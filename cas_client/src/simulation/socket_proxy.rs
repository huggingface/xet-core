//! Unix socket proxy for testing Unix domain socket functionality.
//!
//! This module provides a simple proxy that forwards connections from a Unix socket
//! to a TCP server, enabling testing of Unix socket support without modifying
//! the server implementation.

use std::path::PathBuf;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UnixListener, UnixStream};
use tokio::sync::oneshot;

/// A simple Unix socket proxy that forwards connections to a TCP server.
///
/// This proxy listens on a Unix domain socket and forwards all connections
/// to a specified TCP endpoint. It's useful for testing Unix socket functionality
/// without requiring the server to support Unix sockets directly.
///
/// # Example
///
/// ```no_run
/// use std::path::PathBuf;
///
/// use cas_client::simulation::socket_proxy::UnixSocketProxy;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let socket_path = PathBuf::from("/tmp/test_socket.sock");
/// let proxy = UnixSocketProxy::new(socket_path, "127.0.0.1:8080".to_string()).await?;
/// // Proxy is now listening on the Unix socket and forwarding to TCP
/// // Cleanup happens automatically when proxy is dropped
/// # Ok(())
/// # }
/// ```
pub struct UnixSocketProxy {
    socket_path: PathBuf,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl UnixSocketProxy {
    /// Creates a new proxy that listens on a Unix socket and forwards to a TCP endpoint.
    ///
    /// # Arguments
    /// * `socket_path` - Path to the Unix socket file to create
    /// * `tcp_endpoint` - TCP address (host:port) to forward connections to
    ///
    /// # Errors
    /// Returns an error if the socket file cannot be created or bound.
    pub async fn new(socket_path: PathBuf, tcp_endpoint: String) -> Result<Self, Box<dyn std::error::Error>> {
        // Remove socket file if it exists
        if socket_path.exists() {
            std::fs::remove_file(&socket_path)?;
        }

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let listener = UnixListener::bind(&socket_path)?;

        let tcp_endpoint_clone = tcp_endpoint.clone();
        let socket_path_clone = socket_path.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((unix_stream, _)) => {
                                let tcp_endpoint = tcp_endpoint_clone.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = Self::handle_connection(unix_stream, &tcp_endpoint).await {
                                        eprintln!("Error handling connection: {}", e);
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!("Error accepting connection: {}", e);
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
            let _ = std::fs::remove_file(&socket_path_clone);
        });

        Ok(Self {
            socket_path,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    /// Returns the path to the Unix socket file.
    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }

    /// Handles a single connection by proxying data between Unix socket and TCP.
    async fn handle_connection(unix_stream: UnixStream, tcp_endpoint: &str) -> Result<(), Box<dyn std::error::Error>> {
        let tcp_stream = TcpStream::connect(tcp_endpoint).await?;

        // Use tokio::io::split to get owned halves that can be moved into tasks
        let (mut unix_read, mut unix_write) = tokio::io::split(unix_stream);
        let (mut tcp_read, mut tcp_write) = tokio::io::split(tcp_stream);

        let client_to_server = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match unix_read.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if tcp_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    },
                    Err(_) => break,
                }
            }
            let _ = tcp_write.shutdown().await;
            Ok::<(), std::io::Error>(())
        });

        let server_to_client = tokio::spawn(async move {
            let mut buf = vec![0u8; 8192];
            loop {
                match tcp_read.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if unix_write.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    },
                    Err(_) => break,
                }
            }
            let _ = unix_write.shutdown().await;
            Ok::<(), std::io::Error>(())
        });

        let _ = tokio::try_join!(client_to_server, server_to_client);
        Ok(())
    }
}

impl Drop for UnixSocketProxy {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if self.socket_path.exists() {
            let _ = std::fs::remove_file(&self.socket_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, UnixStream};

    use super::*;

    /// Helper function to create a simple TCP echo server.
    async fn create_echo_server(port: u16) -> (tokio::task::JoinHandle<()>, tokio::sync::oneshot::Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
                .await
                .expect("Failed to bind TCP listener");
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((mut stream, _)) => {
                                tokio::spawn(async move {
                                    let mut buf = vec![0u8; 1024];
                                    loop {
                                        match stream.read(&mut buf).await {
                                            Ok(0) => break,
                                            Ok(n) => {
                                                if stream.write_all(&buf[..n]).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Err(_) => break,
                                        }
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });
        (handle, shutdown_tx)
    }

    #[tokio::test]
    async fn test_proxy_creates_socket_file() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_proxy.sock");

        let _proxy = UnixSocketProxy::new(socket_path.clone(), "127.0.0.1:9999".to_string())
            .await
            .expect("Failed to create proxy");

        assert!(socket_path.exists(), "Socket file should be created");
    }

    #[tokio::test]
    async fn test_proxy_removes_existing_socket() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_existing.sock");

        // Create an existing socket file
        std::fs::File::create(&socket_path).unwrap();
        assert!(socket_path.exists());

        let _proxy = UnixSocketProxy::new(socket_path.clone(), "127.0.0.1:9999".to_string())
            .await
            .expect("Failed to create proxy");

        assert!(socket_path.exists(), "Socket file should exist after proxy creation");
    }

    #[tokio::test]
    async fn test_proxy_forwards_data() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_forward.sock");

        // Start an echo server on a random port
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        let tcp_port = tcp_addr.port();
        drop(tcp_listener); // Release the port

        let (echo_handle, shutdown_tx) = create_echo_server(tcp_port).await;

        // Give the echo server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create proxy
        let _proxy = UnixSocketProxy::new(socket_path.clone(), format!("127.0.0.1:{}", tcp_port))
            .await
            .expect("Failed to create proxy");

        // Give proxy a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Connect to Unix socket and send data
        let mut unix_stream = UnixStream::connect(&socket_path)
            .await
            .expect("Failed to connect to Unix socket");

        let test_data = b"Hello, proxy!";
        unix_stream.write_all(test_data).await.unwrap();
        unix_stream.flush().await.unwrap();

        // Read response
        let mut response = vec![0u8; test_data.len()];
        unix_stream.read_exact(&mut response).await.unwrap();

        assert_eq!(&response, test_data, "Proxy should echo data back");

        // Cleanup
        let _ = shutdown_tx.send(());
        echo_handle.abort();
    }

    #[tokio::test]
    async fn test_proxy_handles_multiple_connections() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_multiple.sock");

        // Start an echo server
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_port = tcp_listener.local_addr().unwrap().port();
        drop(tcp_listener); // Release the port
        let (echo_handle, shutdown_tx) = create_echo_server(tcp_port).await;

        tokio::time::sleep(Duration::from_millis(100)).await;

        let _proxy = UnixSocketProxy::new(socket_path.clone(), format!("127.0.0.1:{}", tcp_port))
            .await
            .expect("Failed to create proxy");

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Create multiple connections
        let mut handles = Vec::new();
        for i in 0..5 {
            let socket_path = socket_path.clone();
            let handle = tokio::spawn(async move {
                let mut stream = UnixStream::connect(&socket_path).await.unwrap();
                let data = format!("test-{}", i);
                stream.write_all(data.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();

                let mut response = vec![0u8; data.len()];
                stream.read_exact(&mut response).await.unwrap();
                assert_eq!(response, data.as_bytes());
            });
            handles.push(handle);
        }

        // Wait for all connections to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Cleanup
        let _ = shutdown_tx.send(());
        echo_handle.abort();
    }

    #[tokio::test]
    async fn test_proxy_cleans_up_on_drop() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_cleanup.sock");

        {
            let _proxy = UnixSocketProxy::new(socket_path.clone(), "127.0.0.1:9999".to_string())
                .await
                .expect("Failed to create proxy");
            assert!(socket_path.exists());
        }

        // Give a moment for cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Socket file should be removed (or at least the proxy should have attempted cleanup)
        // Note: On some systems, the socket file might still exist briefly

        assert!(!socket_path.exists());
    }

    #[tokio::test]
    async fn test_proxy_socket_path_accessor() {
        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_path.sock");

        let proxy = UnixSocketProxy::new(socket_path.clone(), "127.0.0.1:9999".to_string())
            .await
            .expect("Failed to create proxy");

        assert_eq!(proxy.socket_path(), &socket_path);
    }

    #[tokio::test]
    async fn test_client_functionality_through_socket_proxy() {
        use std::sync::Arc;

        use tempfile::TempDir;

        use crate::simulation::client_unit_testing::test_client_functionality;
        use crate::simulation::simulation_server::LocalTestServerBuilder;

        let temp_dir = TempDir::new().unwrap();
        let socket_path = temp_dir.path().join("test_socket.sock");

        // Run all client functionality tests through the socket proxy
        test_client_functionality(|| async {
            let server = LocalTestServerBuilder::new()
                .with_socket(socket_path.clone())
                .with_ephemeral_disk()
                .start()
                .await;

            // Verify the socket file exists
            assert!(socket_path.exists(), "Unix socket file should exist");

            // Return the server as a DirectAccessClient
            Arc::new(server) as Arc<dyn crate::simulation::DirectAccessClient>
        })
        .await;
    }
}
