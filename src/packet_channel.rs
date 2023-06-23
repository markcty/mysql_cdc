use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::constants::PACKET_HEADER_SIZE;
use crate::replica_options::ReplicaOptions;

pub struct PacketChannel {
    stream: TcpStream,
}

impl PacketChannel {
    pub async fn connect(options: &ReplicaOptions) -> Result<Self, io::Error> {
        let address: String = format!("{}:{}", options.hostname, options.port);
        let stream = TcpStream::connect(address).await?;
        Ok(Self { stream })
    }

    pub async fn read_packet(&mut self) -> Result<(Vec<u8>, u8), io::Error> {
        let mut header_buffer = [0; PACKET_HEADER_SIZE];

        self.stream.read_exact(&mut header_buffer).await?;
        let packet_size = (&header_buffer[0..3]).read_u24::<LittleEndian>()?;
        let seq_num = header_buffer[3];

        let mut packet: Vec<u8> = vec![0; packet_size as usize];
        self.stream.read_exact(&mut packet).await?;

        Ok((packet, seq_num))
    }

    pub async fn write_packet(&mut self, packet: &[u8], seq_num: u8) -> Result<(), io::Error> {
        let packet_len = packet.len() as u32;
        let mut header = vec![];
        header.write_u24::<LittleEndian>(packet_len)?;
        byteorder::WriteBytesExt::write_u8(&mut header, seq_num)?;
        self.stream.write_all(&header).await?;
        self.stream.write_all(packet).await?;
        Ok(())
    }

    pub fn upgrade_to_ssl(&mut self) {
        unimplemented!();
    }
}
