use nikidb::db::DB;
use nikidb::option::Options;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::server;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
#[tokio::main]
async fn main() {
    let c = Options::default();
    let db = DB::open(c).unwrap();
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(
        listener,
        signal::ctrl_c(),
        |conn: &Connection, cmd: Command| match cmd {
            Command::Get(cmd) => {
                let entry = db.read(cmd.key.as_bytes()).unwrap();
            }
            Command::Set(cmd) => {}
            Command::Publish(cmd) => {}
            Command::Subscribe(cmd) => {}
            Command::Unknown(cmd) => {}
            Command::Unsubscribe(_) => {}
        },
    )
    .await;
}

// fn handle_event(conn: &Connection, cmd: Command) {
//     println!("handle event");
// }

// async fn process(socket: TcpStream) {
//     println!("hello world");
// }
