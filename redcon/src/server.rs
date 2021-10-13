use std::future::Future;
use tokio::net::{TcpListener, TcpStream};

type handler_cmd = fn();

pub async fn run(listener: TcpListener, shutdown: impl Future, handler: handler_cmd) {
    loop {}
}
