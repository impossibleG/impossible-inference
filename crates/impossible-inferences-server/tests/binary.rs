//! Adversarial checks against the packaged inference server executable and its real TCP boundary.

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn request(port: u16, bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(bytes)?;
    let mut response = [0_u8; 8192];
    let read = stream.read(&mut response)?;
    Ok(String::from_utf8_lossy(&response[..read]).into_owned())
}

fn wait_until_live(port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if request(
            port,
            b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .is_ok_and(|response| response.starts_with("HTTP/1.1 200"))
        {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(25));
    }
    Err("inference server binary did not become live".into())
}

#[test]
fn real_binary_enforces_public_failures_and_body_deadline() -> Result<(), Box<dyn std::error::Error>>
{
    let reservation = TcpListener::bind(("127.0.0.1", 0))?;
    let port = reservation.local_addr()?.port();
    drop(reservation);
    let child = Command::new(env!("CARGO_BIN_EXE_impossible-inferences-server"))
        .args([
            "serve",
            "--bind",
            &format!("127.0.0.1:{port}"),
            "--max-request-bytes",
            "4",
            "--request-timeout-ms",
            "100",
            "--shutdown-timeout-ms",
            "100",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    let _child = ChildGuard(child);
    wait_until_live(port)?;

    for (wire, status, marker) in [
        (
            b"GET /health/ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".as_slice(),
            "HTTP/1.1 503",
            "component_unavailable",
        ),
        (
            b"GET /missing HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".as_slice(),
            "HTTP/1.1 404",
            "invalid_request",
        ),
        (
            b"POST /status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice(),
            "HTTP/1.1 405",
            "invalid_request",
        ),
        (
            b"GET /status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\n12345".as_slice(),
            "HTTP/1.1 413",
            "invalid_request",
        ),
    ] {
        let response = request(port, wire)?;
        assert!(response.starts_with(status), "unexpected response: {response}");
        assert!(response.contains(marker), "unexpected response: {response}");
    }

    let mut slow = TcpStream::connect(("127.0.0.1", port))?;
    slow.set_read_timeout(Some(Duration::from_secs(2)))?;
    slow.write_all(
        b"GET /status HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nConnection: close\r\n\r\n1",
    )?;
    thread::sleep(Duration::from_millis(200));
    let mut bytes = [0_u8; 8192];
    let read = slow.read(&mut bytes)?;
    let response = String::from_utf8_lossy(&bytes[..read]);
    assert!(
        response.starts_with("HTTP/1.1 504"),
        "unexpected response: {response}"
    );
    assert!(response.contains("deadline_exceeded"));
    Ok(())
}
