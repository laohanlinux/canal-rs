use std::net::{SocketAddr};
use async_std::prelude::*;
use async_std::io;
use async_std::net::TcpStream;
use async_std::task;
use log::{info, trace, warn};

pub struct Client {
    addr: SocketAddr,
}

impl Client{
    pub fn new(addr: SocketAddr) -> Client {
        Client{
            addr
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        task::block_on(async {
            let mut stream = TcpStream::connect(self.addr).await.unwrap();

        });

        Ok(())
    }

    pub fn get_with_out_ack(batch_size: usize, timeout: Option<i64>, uints: Option<i32>) {

    }

    pub fn get(batch_size: usize, timeout: Option<i64>, uints: Option<i64>) {

    }

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
}