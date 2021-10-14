use redcon::server;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(listener, signal::ctrl_c()).await;
    // loop {
    //     let (socket, _) = listener.accept().await.unwrap();
    //     process(socket).await;
    // }
}

// async fn process(socket: TcpStream) {
//     println!("hello world");
// }
