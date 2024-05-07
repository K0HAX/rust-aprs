use crate::data::Handshake;
use anyhow::anyhow;
use futures_util::sink::SinkExt;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LinesCodec};

pub struct AprsClient {
    addr: SocketAddr,
    client: Arc<RwLock<Framed<tokio::net::TcpStream, LinesCodec>>>,
    error_count: Arc<RwLock<u64>>,
}

impl AprsClient {
    pub async fn new(hostname: &str, port: u16, callsign: &str) -> Self {
        let addr = tokio::net::lookup_host(format!("{}:{}", hostname, port))
            .await
            .unwrap()
            .next()
            .unwrap();

        // Create the event loop, and initiate the connection to the remote server
        let conn = TcpStream::connect(&addr).await.unwrap();

        let mut client = Framed::new(conn, LinesCodec::new_with_max_length(2048));
        let handshake = Handshake::new(callsign.to_string());
        client
            .send(format!(
                "user {} pass {}\r\n",
                handshake.callsign, handshake.passcode
            ))
            .await
            .unwrap();

        let error_count: u64 = 0;
        AprsClient {
            addr: addr,
            client: Arc::new(RwLock::new(client)),
            error_count: Arc::new(RwLock::new(error_count)),
        }
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn read_line(&self) -> Result<crate::ParsedLine, Box<dyn std::error::Error>> {
        let client_handle = Arc::clone(&self.client);
        let mut client_rw = client_handle.write().unwrap();
        match client_rw.next().await {
            Some(Ok(x)) => match x.as_str().get(..1) {
                Some("#") => {
                    return Err(anyhow!("Server Comment: {}", x).into());
                }
                _ => {
                    let error_count_handle = Arc::clone(&self.error_count);
                    let mut error_count = error_count_handle.write().unwrap();
                    *error_count = 0;
                    match crate::parse_line(&x) {
                        Ok(y) => return Ok(y),
                        Err(y) => return Err(anyhow!("An error: {}; skipped. | {}", y, x).into()),
                    };
                }
            },
            Some(Err(x)) => Err(anyhow!("{}", x).into()),
            None => {
                let error_count_handle = Arc::clone(&self.error_count);
                let mut error_count = error_count_handle.write().unwrap();
                *error_count = *error_count + 1;
                if *error_count > 100 {
                    panic!("client_rw returned None and error_count is > 100!");
                }
                Err(anyhow!("client_rw returned None!").into())
            }
        }
    }
}
