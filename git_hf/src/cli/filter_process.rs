use std::io::{Read, Seek, Write};

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};

use crate::clean_session::CleanSession;
use crate::cleaner::SelectingFileCleaner;
use crate::filter_protocol::{Content, READ_CHUNK_SIZE, flush_packet, headers, write_data};
use crate::{GitHfError, Result};

fn protocol(message: &str) -> GitHfError {
    GitHfError::FilterProtocol(message.into())
}

pub(super) fn run() -> Result<()> {
    let session = CleanSession::new()?;
    let runtime = session.ctx.runtime.clone();
    runtime.bridge_sync(async move {
        let mut input = BufReader::new(tokio::io::stdin());
        let mut output = BufWriter::new(tokio::io::stdout());
        serve(&mut input, &mut output, session).await
    })?
}

async fn serve(
    input: &mut (impl AsyncRead + Unpin),
    output: &mut (impl AsyncWrite + Unpin),
    session: CleanSession,
) -> Result<()> {
    let hello = headers(input).await?.ok_or_else(|| protocol("Missing handshake"))?;
    if hello.first().map(Vec::as_slice) != Some(b"git-filter-client") || !hello.iter().any(|l| l == b"version=2") {
        return Err(protocol("Expected git-filter-client version 2"));
    }
    write_data(output, b"git-filter-server\n").await?;
    write_data(output, b"version=2\n").await?;
    flush_packet(output).await?;
    output.flush().await?;
    let caps = headers(input).await?.ok_or_else(|| protocol("Missing capabilities"))?;
    let clean = caps.iter().any(|l| l == b"capability=clean");
    let smudge = caps.iter().any(|l| l == b"capability=smudge");
    for capability in [b"capability=clean\n".as_slice(), b"capability=smudge\n"] {
        if caps.iter().any(|l| l.as_slice() == &capability[..capability.len() - 1]) {
            write_data(output, capability).await?;
        }
    }
    flush_packet(output).await?;
    output.flush().await?;
    // Finalize only after Git closes stdin normally. On any request or protocol error Git aborts the
    // command without writing the index, so earlier pointers are discarded with it; exiting without
    // finalizing also abandons a failed file's in-flight work instead of racing it. Fully written
    // xorbs stay unreferenced in staging until cleanup.
    serve_requests(input, output, &session, clean, smudge).await?;
    session.finish().await
}

async fn serve_requests(
    input: &mut (impl AsyncRead + Unpin),
    output: &mut (impl AsyncWrite + Unpin),
    session: &CleanSession,
    clean: bool,
    smudge: bool,
) -> Result<()> {
    while let Some(lines) = headers(input).await? {
        let mut command = None;
        let mut pathname = None;
        for line in &lines {
            if let Some(value) = line.strip_prefix(b"command=")
                && command.replace(value).is_some()
            {
                return Err(protocol("Duplicate command"));
            }
            if let Some(value) = line.strip_prefix(b"pathname=")
                && pathname.replace(value).is_some()
            {
                return Err(protocol("Duplicate pathname"));
            }
        }
        let pathname = pathname.ok_or_else(|| protocol("Missing pathname"))?;
        let mut content = Content::new(input);
        let mut response = tempfile::spooled_tempfile(1024 * 1024);
        let result = match command {
            // Identity smudge: return the stored bytes unchanged.
            Some(b"smudge") if smudge => copy_content(&mut content, &mut response).await,
            Some(b"clean") if clean => SelectingFileCleaner::new(session, pathname)
                .clean(&mut content)
                .await
                .and_then(|data| Ok(response.write_all(&data)?)),
            _ => Err(protocol("Unsupported filter command")),
        };
        // Git sends the entire request before reading our response. Drain failures as well.
        content.drain().await?;
        match result {
            Ok(()) => {
                write_data(output, b"status=success\n").await?;
                flush_packet(output).await?;
                response.rewind()?;
                let mut buffer = vec![0; 65516];
                loop {
                    let n = response.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    write_data(output, &buffer[..n]).await?;
                }
                flush_packet(output).await?;
                flush_packet(output).await?;
            },
            Err(error) => {
                // A required filter's error aborts the whole Git command, so serving further
                // requests gains nothing.
                write_data(output, b"status=error\n").await?;
                flush_packet(output).await?;
                output.flush().await?;
                return Err(error);
            },
        }
        output.flush().await?;
    }
    Ok(())
}

async fn copy_content(content: &mut Content<'_, impl AsyncRead + Unpin>, response: &mut impl Write) -> Result<()> {
    let mut buffer = vec![0; READ_CHUNK_SIZE];
    loop {
        let n = content.read(&mut buffer).await?;
        if n == 0 {
            return Ok(());
        }
        response.write_all(&buffer[..n])?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter_protocol::block_on;

    fn write(wire: &mut Vec<u8>, data: &[u8]) {
        block_on(write_data(wire, data)).unwrap();
    }

    fn flush(wire: &mut Vec<u8>) {
        block_on(flush_packet(wire)).unwrap();
    }

    fn read_headers(output: &mut &[u8]) -> Option<Vec<Vec<u8>>> {
        block_on(headers(output)).unwrap()
    }

    fn read_content(output: &mut &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        block_on(Content::new(output).read_up_to(&mut data, usize::MAX)).unwrap();
        data
    }

    /// Serves `wire` with a new session for the current directory, blocking until done.
    fn serve_wire(wire: &[u8], output: &mut Vec<u8>) -> Result<()> {
        let session = CleanSession::new()?;
        let runtime = session.ctx.runtime.clone();
        runtime.handle().block_on(serve(&mut &wire[..], output, session))
    }
    fn handshake() -> Vec<u8> {
        let mut wire = Vec::new();
        for lines in [
            &[b"git-filter-client\n".as_slice(), b"version=2\n"][..],
            &[
                b"capability=clean\n".as_slice(),
                b"capability=smudge\n",
                b"capability=delay\n",
            ][..],
        ] {
            for line in lines {
                write(&mut wire, line);
            }
            flush(&mut wire);
        }
        wire
    }
    fn request(wire: &mut Vec<u8>, data: &[u8]) {
        write(wire, b"command=clean\n");
        write(wire, b"pathname=file\n");
        flush(wire);
        write(wire, data);
        flush(wire);
    }

    // Nothing is finalized after an error: no shards, and the session's scratch is gone.
    // Small inputs never fill a xorb, so no xorb is written either.
    fn assert_nothing_finalized(repo: &git_extension_common::test_utils::TestRepo) -> anyhow::Result<()> {
        let root = git_extension_common::GitRepo::open(repo.path())?
            .common_dir()?
            .join("xet/staging");
        let mut entries = std::fs::read_dir(&root)?
            .map(|entry| Ok(entry?.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort();
        assert_eq!(entries, ["shards", "xorbs"]);
        assert_eq!(std::fs::read_dir(root.join("shards"))?.count(), 0);
        assert_eq!(std::fs::read_dir(root.join("xorbs"))?.count(), 0);
        Ok(())
    }

    #[test]
    #[serial_test::serial(env_var_write_read)]
    fn request_failure_stops_without_finalizing() -> anyhow::Result<()> {
        let repo = git_extension_common::test_utils::TestRepo::new("main")?;
        let _cwd = xet_runtime::utils::CwdGuard::set(repo.path())?;
        let mut wire = handshake();
        request(&mut wire, &vec![0; 100001]);
        request(&mut wire, b"version https://hf.co/spec/xet/v1\n");
        request(&mut wire, &vec![1; 100001]);
        let mut output = Vec::new();
        let error = serve_wire(&wire, &mut output).unwrap_err();
        assert!(matches!(error, GitHfError::InvalidPointer { .. }), "{error:?}");
        // The message names the file and says why it can't be stored.
        assert!(
            error
                .to_string()
                .starts_with("file: this file looks like a Xet pointer but is not valid"),
            "{error}"
        );
        let mut output = output.as_slice();
        read_headers(&mut output);
        read_headers(&mut output);
        assert_eq!(read_headers(&mut output).unwrap(), [b"status=success".to_vec()]);
        let pointer = read_content(&mut output);
        assert!(crate::XetPointer::parse(&pointer)?.is_some());
        assert!(read_headers(&mut output).unwrap().is_empty());
        // The failed request ends the process; the third request is never answered.
        assert_eq!(read_headers(&mut output).unwrap(), [b"status=error".to_vec()]);
        assert!(output.is_empty());
        // Git aborts the command, discarding the first pointer, so its data is not finalized.
        assert_nothing_finalized(&repo)
    }

    #[test]
    #[serial_test::serial(env_var_write_read)]
    fn truncated_request_exits_without_finalizing() -> anyhow::Result<()> {
        let repo = git_extension_common::test_utils::TestRepo::new("main")?;
        let _cwd = xet_runtime::utils::CwdGuard::set(repo.path())?;
        let mut wire = handshake();
        request(&mut wire, &vec![0; 100001]);
        write(&mut wire, b"command=clean\n");
        write(&mut wire, b"pathname=incomplete\n");
        flush(&mut wire);
        write(&mut wire, &vec![1; 200000]);
        wire.extend_from_slice(b"0008ab"); // EOF after ingestion started on the second request.
        assert!(serve_wire(&wire, &mut Vec::new()).is_err());
        assert_nothing_finalized(&repo)
    }

    #[test]
    fn identity_multiple_requests() {
        let mut wire = handshake();
        let payloads = [
            vec![],
            b"version https://hf.co/spec/xet/v1\n".to_vec(),
            vec![255; 2 * 1024 * 1024],
        ];
        for data in &payloads {
            write(&mut wire, b"command=smudge\n");
            write(&mut wire, b"pathname=a=b\n");
            flush(&mut wire);
            write(&mut wire, data);
            flush(&mut wire);
        }
        let mut output = Vec::new();
        serve_wire(&wire, &mut output).unwrap();
        let mut output = output.as_slice();
        assert_eq!(read_headers(&mut output).unwrap(), [b"git-filter-server".to_vec(), b"version=2".to_vec()]);
        assert_eq!(read_headers(&mut output).unwrap(), [b"capability=clean".to_vec(), b"capability=smudge".to_vec()]);
        for expected in payloads {
            assert_eq!(read_headers(&mut output).unwrap(), [b"status=success".to_vec()]);
            assert_eq!(read_content(&mut output), expected);
            assert!(read_headers(&mut output).unwrap().is_empty());
        }
        assert!(output.is_empty());
    }
    #[test]
    fn invalid_handshake_and_unsupported_command() {
        assert!(serve_wire(b"0000", &mut Vec::new()).is_err());
        let mut wire = handshake();
        write(&mut wire, b"command=unknown\n");
        write(&mut wire, b"pathname=a\n");
        flush(&mut wire);
        flush(&mut wire);
        let mut out = Vec::new();
        assert!(serve_wire(&wire, &mut out).is_err());
        assert!(out.ends_with(b"0011status=error\n0000"));
    }
}
