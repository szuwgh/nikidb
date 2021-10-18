#![feature(async_closure)]
use bytes::{Buf, Bytes};
use nikidb::db::DbDropGuard;
use nikidb::db::DB;
use nikidb::option::Options;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::frame::Frame;
use redcon::server;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
#[tokio::main]
async fn main() {
    let c = Options::default();
    let db_holder = DbDropGuard::new(c);
    // let thread_db = db_holder.db();
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(listener, signal::ctrl_c(), handle_event).await;
}

async fn handle_event(conn: &mut Connection, cmd: Command) {}

// move |conn: &mut Connection, cmd: Command| async {
//     match cmd {
//         Command::Get(cmd) => {
//             println!("{:?}", cmd);
//             let entry = db_holder.db().get(cmd.key.as_bytes()).unwrap();
//             let resp = Frame::Bulk(Bytes::copy_from_slice(entry.value.as_slice()));
//             conn.write_frame(&resp).await;
//             println!("get");
//         }
//         Command::Set(cmd) => {
//             println!("{:?}", cmd);
//             db_holder.db().put(cmd.key.as_bytes(), &cmd.value).unwrap();
//             let resp = Frame::Simple("ok".to_string());
//             conn.write_frame(&resp).await;
//         }
//         Command::Publish(cmd) => {}
//         Command::Subscribe(cmd) => {}
//         Command::Unknown(cmd) => {}
//         Command::Unsubscribe(_) => {}
//     }
//     .await;
// },

// async fn process(socket: TcpStream) {
//     println!("hello world");
// }
