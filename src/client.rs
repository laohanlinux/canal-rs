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
use failure::{Error as FailureError, Fail, err_msg};

use crate::protobuf::CanalProtocol::{Packet,
                                     PacketType,
                                     Handshake,
                                     ClientAuth,
                                     Get,
                                     Ack,
                                     Sub,
                                     Unsub,
                                     ClientAck,
                                     Messages,
                                     ClientRollback,
                                     ClientAuth_oneof_net_read_timeout_present,
                                     ClientAuth_oneof_net_write_timeout_present};
use protobuf::{Message, parse_from_bytes};

const version: i32 = 1;

pub struct Client {
    addr: SocketAddr,
    conf: Config,
    framed: Option<Framed<TcpStream, LengthDelimitedCodec>>,
    connected: bool,
}

#[derive(Clone)]
pub struct Config {
    user_name: String,
    password: String,
    client_id: String,
    destinations: String,
    net_read_timeout_present: ClientAuth_oneof_net_read_timeout_present,
    net_write_timeout_present: ClientAuth_oneof_net_write_timeout_present,
}

impl Config {
    pub fn new(user_name: String, password: String, client_id: String, destinations: String,
               net_read_timeout_present: ClientAuth_oneof_net_read_timeout_present,
               net_write_timeout_present: ClientAuth_oneof_net_write_timeout_present) -> Self {
        Config {
            user_name,
            password,
            client_id,
            destinations,
            net_read_timeout_present,
            net_write_timeout_present,
        }
    }
}

impl Client {
    pub fn new(addr: SocketAddr, conf: Config) -> Client {
        Client {
            addr,
            conf: conf,
            framed: None,
            connected: false,
        }
    }

    pub async fn connect(&mut self) -> Result<(), FailureError> {
        let stream = TcpStream::connect(&self.addr).await?;
        let mut builder = codec::length_delimited::Builder::new().big_endian().length_field_length(4).new_codec();
        let mut framed = Framed::new(stream, builder);
        self.framed = Some(framed);
        let packet: Packet = self.read_packet().await?;
        self.handle_handshake(&packet)?;
        self.handle_auth().await
    }

    pub fn handle_handshake(&self, handshake: &Packet) -> Result<(), FailureError> {
        if handshake.get_version() != version {
            bail!("version is not matched:{:?} {:?}", handshake.version_present, handshake.get_version());
        }
        if handshake.get_field_type() != PacketType::HANDSHAKE {
            bail!("expect handshake but found other type");
        }
        protobuf::parse_from_bytes::<Handshake>(handshake.get_body()).map(|_| ()).map_err(|err| err.into())
    }

    async fn handle_auth(&mut self) -> Result<(), FailureError> {
        let mut auth = ClientAuth::new();
        auth.username = self.conf.user_name.clone();
        auth.password = self.conf.password.clone().into_bytes();
        auth.net_read_timeout_present = Some(self.conf.net_read_timeout_present.clone());
        auth.net_write_timeout_present = Some(self.conf.net_write_timeout_present.clone());
        self.write_message(PacketType::CLIENTAUTHENTICATION, auth).await?;
        let packet = self.read_packet().await?;
        assert_eq!(packet.get_field_type(), PacketType::ACK);
        let ack: Ack = protobuf::parse_from_bytes(packet.get_body()).unwrap();
        assert_eq!(ack.get_error_code(), 0);
        self.connected = true;
        Ok(())
    }

    pub async fn get_without_ack(&mut self, batch_size: i32, timeout: Option<i64>, uints: Option<i32>) -> Result<Messages, FailureError> {
        assert!(self.connected);
        let mut get_proto = Get::new();
        get_proto.set_client_id(self.conf.client_id.clone());
        get_proto.set_destination(self.conf.destinations.clone());
        get_proto.set_fetch_size(batch_size);
        let _timeout = timeout.or_else(|| Some(-1));
        get_proto.set_timeout(_timeout.unwrap());
        get_proto.set_unit(uints.or_else(|| Some(-1)).unwrap());
        get_proto.set_auto_ack(false);
        self.write_message(PacketType::GET, get_proto).await?;
        self.read_message().await
    }

    pub async fn get(&mut self, batch_size: i32, timeout: Option<i64>, uints: Option<i32>) -> Result<Messages, FailureError> {
        assert!(self.connected);
        let message = self.get_without_ack(batch_size, timeout, uints).await?;
        self.ack(message.batch_id).await.map(|_| message)
    }

    pub async fn subscribe(&mut self, filter: String) -> Result<(), FailureError> {
        assert!(self.connected);
        let mut sub = Sub::new();
        sub.set_client_id(self.conf.client_id.clone());
        sub.set_destination(self.conf.destinations.clone());
        sub.set_filter(filter);
        self.write_message(PacketType::SUBSCRIPTION, sub).await;
        let packet = self.read_packet().await?;
        assert_eq!(packet.get_field_type(), PacketType::ACK);
        let ack: Ack = protobuf::parse_from_bytes(packet.get_body()).unwrap();
        if ack.get_error_code() != 0 {
            bail!("code: {:?}", ack.get_error_code());
        }
        Ok(())
    }

    pub async fn unsubscribe(&mut self, filter: String) -> Result<(), FailureError> {
        assert!(self.connected);
        let mut unsub = Unsub::new();
        unsub.set_client_id(self.conf.client_id.clone());
        unsub.set_destination(self.conf.destinations.clone());
        unsub.set_filter(filter);
        self.write_message(PacketType::UNSUBSCRIPTION, unsub).await;
        let packet = self.read_packet().await?;
        assert_eq!(packet.get_field_type(), PacketType::ACK);
        match protobuf::parse_from_bytes::<Ack>(packet.get_body()) {
            Ok(ack) => {
                if ack.get_error_code() > 0 {
                    bail!("code: {:?}", ack.get_error_code());
                }
                Ok(())
            }
            Err(e) => bail!("{:?}", e),
        }
    }

    pub async fn ack(&mut self, batch_id: i64) -> Result<(), FailureError> {
        assert!(self.connected);
        let mut ack = ClientAck::new();
        ack.set_client_id(self.conf.client_id.clone());
        ack.set_destination(self.conf.destinations.clone());
        ack.set_batch_id(batch_id);
        self.write_message(PacketType::CLIENTACK, ack).await
    }

    pub async fn roll_back(&mut self, batch_id: i64) -> Result<(), FailureError> {
        assert!(self.connected);
        let mut roll_back = ClientRollback::new();
        roll_back.set_client_id(self.conf.client_id.clone());
        roll_back.set_destination(self.conf.destinations.clone());
        roll_back.set_batch_id(batch_id);
        self.write_message(PacketType::CLIENTROLLBACK, roll_back).await
    }

    pub async fn disconnect(&mut self) -> Result<(), FailureError> {
        assert!(self.connected);
        self.connected = false;
        self.framed.take();
        debug!("Close connection");
        Ok(())
    }

    async fn read_packet(&mut self) -> Result<Packet, FailureError> {
        let framed = self.framed.as_mut().unwrap();
        match framed.next().await {
            Some(Ok(buf)) => {
                let packet: Packet = protobuf::parse_from_bytes(&buf).unwrap();
                Ok(packet)
            }
            Some(Err(e)) => bail!("{:?}", e),
            None => bail!("Connect has close"),
        }
    }

    async fn read_message<M: Message>(&mut self) -> Result<M, FailureError> {
        self.read_packet().await.map(|packet| protobuf::parse_from_bytes(packet.get_body()).unwrap()).map_err(|err| err.into())
    }

    async fn write_packet(&mut self, packet: Packet) -> Result<(), FailureError> {
        let framed = self.framed.as_mut().unwrap();
        framed.send(Bytes::from(packet.write_to_bytes().unwrap())).await.map(|_| ()).map_err(|err| err.into())
    }

    async fn write_message<M: Message>(&mut self, packet_type: PacketType, message: M) -> Result<(), FailureError> {
        let mut packet = Packet::new();
        packet.set_field_type(packet_type);
        packet.set_body(message.write_to_bytes().unwrap());
        self.write_packet(packet).await
    }
}
