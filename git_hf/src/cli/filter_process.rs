use std::io::{self, Read, Seek, Write};

use crate::clean_session::CleanSession;
use crate::cleaner::SelectingFileCleaner;
use crate::filter_protocol::{Content, flush_packet, headers, write_data};
use crate::{GitHfError, Result};

fn protocol(message: &str) -> GitHfError {
    GitHfError::FilterProtocol(message.into())
}

pub(super) fn run() -> Result<()> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    serve(&mut stdin.lock(), &mut stdout.lock())
}

fn serve(input: &mut impl Read, output: &mut impl Write) -> Result<()> {
    let hello = headers(input)?.ok_or_else(|| protocol("Missing handshake"))?;
    if hello.first().map(Vec::as_slice) != Some(b"git-filter-client") || !hello.iter().any(|l| l == b"version=2") {
        return Err(protocol("Expected git-filter-client version 2"));
    }
    write_data(output, b"git-filter-server\n")?;
    write_data(output, b"version=2\n")?;
    flush_packet(output)?;
    output.flush()?;
    let caps = headers(input)?.ok_or_else(|| protocol("Missing capabilities"))?;
    let clean = caps.iter().any(|l| l == b"capability=clean");
    let smudge = caps.iter().any(|l| l == b"capability=smudge");
    for capability in [b"capability=clean\n".as_slice(), b"capability=smudge\n"] {
        if caps.iter().any(|l| l.as_slice() == &capability[..capability.len() - 1]) {
            write_data(output, capability)?;
        }
    }
    flush_packet(output)?;
    output.flush()?;
    let session = CleanSession::new()?;
    // Finalize only after Git closes stdin normally. On any request or protocol error Git aborts the
    // command without writing the index, so earlier pointers are discarded with it; exiting without
    // finalizing also abandons a failed file's in-flight work instead of racing it. Fully written
    // xorbs stay unreferenced in staging until cleanup.
    serve_requests(input, output, &session, clean, smudge)?;
    session.finish()
}

fn serve_requests(
    input: &mut impl Read,
    output: &mut impl Write,
    session: &CleanSession,
    clean: bool,
    smudge: bool,
) -> Result<()> {
    while let Some(lines) = headers(input)? {
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
            Some(b"smudge") if smudge => io::copy(&mut content, &mut response).map(|_| ()).map_err(GitHfError::from),
            Some(b"clean") if clean => {
                SelectingFileCleaner::new(session, pathname)
                    .clean(&mut content)
                    .and_then(|data| {
                        response.write_all(&data)?;
                        Ok(())
                    })
            },
            _ => Err(protocol("Unsupported filter command")),
        };
        // Git sends the entire request before reading our response. Drain failures as well.
        io::copy(&mut content, &mut io::sink())?;
        match result {
            Ok(()) => {
                write_data(output, b"status=success\n")?;
                flush_packet(output)?;
                response.rewind()?;
                let mut buffer = [0; 65516];
                loop {
                    let n = response.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    write_data(output, &buffer[..n])?;
                }
                flush_packet(output)?;
                flush_packet(output)?;
            },
            Err(error) => {
                // A required filter's error aborts the whole Git command, so serving further
                // requests gains nothing.
                write_data(output, b"status=error\n")?;
                flush_packet(output)?;
                output.flush()?;
                return Err(error);
            },
        }
        output.flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
                write_data(&mut wire, line).unwrap();
            }
            flush_packet(&mut wire).unwrap();
        }
        wire
    }
    fn request(wire: &mut Vec<u8>, data: &[u8]) {
        write_data(wire, b"command=clean\n").unwrap();
        write_data(wire, b"pathname=file\n").unwrap();
        flush_packet(wire).unwrap();
        write_data(wire, data).unwrap();
        flush_packet(wire).unwrap();
    }

    // Nothing is finalized after an error: no shards or marker, and the session's scratch is gone.
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
        assert!(matches!(serve(&mut wire.as_slice(), &mut output), Err(GitHfError::Pointer(_))));
        let mut output = output.as_slice();
        headers(&mut output)?;
        headers(&mut output)?;
        assert_eq!(headers(&mut output)?.unwrap(), [b"status=success".to_vec()]);
        let mut pointer = Vec::new();
        Content::new(&mut output).read_to_end(&mut pointer)?;
        assert!(crate::XetPointer::parse(&pointer)?.is_some());
        assert!(headers(&mut output)?.unwrap().is_empty());
        // The failed request ends the process; the third request is never answered.
        assert_eq!(headers(&mut output)?.unwrap(), [b"status=error".to_vec()]);
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
        write_data(&mut wire, b"command=clean\n")?;
        write_data(&mut wire, b"pathname=incomplete\n")?;
        flush_packet(&mut wire)?;
        write_data(&mut wire, &vec![1; 200000])?;
        wire.extend_from_slice(b"0008ab"); // EOF after ingestion started on the second request.
        assert!(serve(&mut wire.as_slice(), &mut Vec::new()).is_err());
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
            write_data(&mut wire, b"command=smudge\n").unwrap();
            write_data(&mut wire, b"pathname=a=b\n").unwrap();
            flush_packet(&mut wire).unwrap();
            write_data(&mut wire, data).unwrap();
            flush_packet(&mut wire).unwrap();
        }
        let mut output = Vec::new();
        serve(&mut wire.as_slice(), &mut output).unwrap();
        let mut output = output.as_slice();
        assert_eq!(headers(&mut output).unwrap().unwrap(), [b"git-filter-server".to_vec(), b"version=2".to_vec()]);
        assert_eq!(
            headers(&mut output).unwrap().unwrap(),
            [b"capability=clean".to_vec(), b"capability=smudge".to_vec()]
        );
        for expected in payloads {
            assert_eq!(headers(&mut output).unwrap().unwrap(), [b"status=success".to_vec()]);
            let mut actual = Vec::new();
            Content::new(&mut output).read_to_end(&mut actual).unwrap();
            assert_eq!(actual, expected);
            assert!(headers(&mut output).unwrap().unwrap().is_empty());
        }
        assert!(output.is_empty());
    }
    #[test]
    fn invalid_handshake_and_unsupported_command() {
        assert!(serve(&mut &b"0000"[..], &mut Vec::new()).is_err());
        let mut wire = handshake();
        write_data(&mut wire, b"command=unknown\n").unwrap();
        write_data(&mut wire, b"pathname=a\n").unwrap();
        flush_packet(&mut wire).unwrap();
        flush_packet(&mut wire).unwrap();
        let mut out = Vec::new();
        assert!(serve(&mut wire.as_slice(), &mut out).is_err());
        assert!(out.ends_with(b"0011status=error\n0000"));
    }
}
