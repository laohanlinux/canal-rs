use std::net::SocketAddr;
use log::{info, trace, warn};
use std::error::Error;
use std::time::Duration;

use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec;

use futures::{future, Sink, SinkExt, Stream, StreamExt};

pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Client {
        Client {
            addr
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        let (r, w) = stream.split();
        let mut canal_read_half = codec::length_delimited::Builder::new().big_endian().length_field_length(4).new_read(r);
//        let cancal = canal_read_half.take(1).await?;

        Ok(())
    }

    pub fn get_with_out_ack(batch_size: usize, timeout: Option<i64>, uints: Option<i32>) {}

    pub fn get(batch_size: usize, timeout: Option<i64>, uints: Option<i64>) {}

    pub fn subscribe(filter: &String) -> Result<(), String> {
        Ok(())
    }

    pub fn unsubscribe() -> Result<(), String> {
        Ok(())
    }

    pub fn ack(batch_id: i64) -> Result<(), String> {
        Ok(())
    }

    pub fn roll_back(batch_id: i64) -> Result<(), String> {
        Ok(())
    }

    pub fn write_header() {}

    pub fn handshake() {}
}