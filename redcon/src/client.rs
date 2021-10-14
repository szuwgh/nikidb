use crate::connection::Connection;
use tokio::net::{TcpStream, ToSocketAddrs};
pub struct Client {
    connection: Connection,
}

pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
    // The `addr` argument is passed directly to `TcpStream::connect`. This
    // performs any asynchronous DNS lookup and attempts to establish the TCP
    // connection. An error at either step returns an error, which is then
    // bubbled up to the caller of `mini_redis` connect.
    let socket = TcpStream::connect(addr).await?;

    // Initialize the connection state. This allocates read/write buffers to
    // perform redis protocol frame parsing.
    let connection = Connection::new(socket);

    Ok(Client { connection })
}

impl Client {
    pub async fn write(&mut self, src: &[u8]) {
        self.connection.write(src).await;
    }
}
