use tokio::net::{TcpListener, TcpStream};

type handler_cmd = fn();

pub async fn run(listener: TcpListener, handler: handler_cmd) {}
