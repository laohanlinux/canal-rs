use tokio::codec::{Decoder, Encoder, LengthDelimitedCodec};
use tokio::io;
use crate::protobuf::CanalProtocol::Packet;
use std::io::Bytes;
use bytes::BytesMut;

pub struct CanalCodec {}

impl CanalCodec {
    pub fn new() -> CanalCodec {
        CanalCodec {}
    }
}

impl Decoder for CanalCodec {
    type Item = Packet;
    type Error = io::Error;
    fn decode(&mut self, buf: &mut bytes::BytesMut) -> Result<Option<Packet>, io::Error> {
        Ok(None)
    }
}


impl Encoder for CanalCodec {
    type Item = Packet;
    type Error = io::Error;

    fn encode(&mut self, data: Packet, buf: &mut BytesMut) -> Result<(), io::Error> {
        Ok(())
    }
}