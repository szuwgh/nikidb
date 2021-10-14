use bytes::{Buf, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub async fn read(&mut self) -> crate::Result<Option<()>> {
        loop {
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
            println!("{:?}", self.buffer);
        }
    }

    pub async fn write(&mut self, src: &[u8]) -> io::Result<()> {
        self.stream.write_all(src).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
