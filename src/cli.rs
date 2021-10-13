// use crate::{Connection, Frame};
// use bytes::Bytes;
// use tokio::net::{TcpStream, ToSocketAddrs};
// pub struct Client {
//     connection: Connection,
// }

// pub async fn connect<T: ToSocketAddrs>(addr: T) -> Option<Client> {
//     let socket = TcpStream::connect(addr).await;
//     let connection = Connection::new(socket);
//     Some(Client { connection })
// }

// impl Client {
//     pub async fn get(&mut self, key: &str) {
//         let mut frame = Frame::array();
//         frame.push_bulk(Bytes::from(key.as_bytes()));
//         self.connection.write_frame(&frame).await;
//     }
// }
