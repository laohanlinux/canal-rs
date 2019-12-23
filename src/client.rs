use std::net::SocketAddr;
use log::{info, trace, warn};
use std::error::Error;
use std::time::Duration;

use tokio::prelude::*;
use tokio::net::TcpStream;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{self, LengthDelimitedCodec, Framed};
use bytes::{Bytes, BytesMut};

use futures::{future, Sink, SinkExt, Stream, StreamExt};

use crate::protobuf::CanalProtocol::{
    Packet,
    PacketType,
    Handshake,
    ClientAuth,
    Ack,
    ClientAuth_oneof_net_read_timeout_present,
    ClientAuth_oneof_net_write_timeout_present};
use protobuf::{Message, parse_from_bytes};

const version: i32 = 1;

pub struct Client {
    addr: SocketAddr,
    db_conf: DbConfig,
    framed: Option<Framed<TcpStream, LengthDelimitedCodec>>,
    connected: bool,
}

pub struct DbConfig{
    user_name: String,
    password: String,
    net_read_timeout_present: ClientAuth_oneof_net_read_timeout_present,
    net_write_timeout_present: ClientAuth_oneof_net_write_timeout_present,
}

impl DbConfig {
    pub fn new(user_name: String, password: String,
               net_read_timeout_present: ClientAuth_oneof_net_read_timeout_present,
               net_write_timeout_present: ClientAuth_oneof_net_write_timeout_present) -> Self{
        DbConfig {
            user_name,
            password,
            net_read_timeout_present,
            net_write_timeout_present,
        }
    }
}

impl Client {
    pub fn new(addr: SocketAddr, conf: DbConfig) -> Client {
        Client {
            addr,
            db_conf:conf,
            framed: None,
            connected: false,
        }
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(&self.addr).await?;
        debug!("Connected canal server: {:?}", self.addr);
        let mut builder = codec::length_delimited::Builder::new().big_endian().length_field_length(4).new_codec();
        let mut framed = Framed::new(stream, builder);
        let buf = framed.next().await.unwrap().unwrap();
        let packet: Packet = protobuf::parse_from_bytes(&buf).unwrap();
        if let Err(e) = self.handle_handshake(&packet) {}
        debug!("Handshaked canal server");
        self.framed = Some(framed);
        if let Err(e) = self.handle_auth().await {

        }
        Ok(())
    }

    pub fn handle_handshake(&self, handshake: &Packet) -> Result<(), String> {
        if handshake.get_version() != version {
            return Err("".to_string());
        }
        if handshake.get_field_type() != PacketType::HANDSHAKE {
            return Err("expect handshake but found other type".to_string());
        }
        // TODO
        // add trace log display handshake detail
        let handshake: Handshake = protobuf::parse_from_bytes(handshake.get_body()).or(Err("expect handshake but found other type".to_string()))?;
        Ok(())
    }

    // TODO may be need to add header size
    async fn handle_auth(&mut self) -> Result<(), Box<dyn Error>> {
        let mut auth = ClientAuth::new();
        auth.username = self.db_conf.user_name.clone();
        auth.password = self.db_conf.password.clone().into_bytes();
        auth.net_read_timeout_present = Some(self.db_conf.net_read_timeout_present.clone());
        auth.net_write_timeout_present = Some(self.db_conf.net_write_timeout_present.clone());
        let auth_buf = auth.write_to_bytes().unwrap();
        let mut packet = Packet::new();
        packet.set_field_type(PacketType::CLIENTAUTHENTICATION);
        packet.set_body(auth_buf);
        let packet_buf = packet.write_to_bytes().unwrap();
        let mut bytes = Bytes::from(packet_buf);
        let framed = self.framed.as_mut().unwrap();
        let ack = framed.next().await.unwrap().unwrap();
        let packet: Packet= protobuf::parse_from_bytes(&ack).unwrap();
        assert_eq!(packet.get_field_type(), PacketType::ACK);
        let ack: Ack = protobuf::parse_from_bytes(packet.get_body()).unwrap();
        assert_eq!(ack.get_error_code(), 0);
        debug!("Pass auth");
        self.connected = true;
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
