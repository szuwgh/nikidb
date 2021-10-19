#![feature(async_closure)]
use bytes::{Buf, Bytes};
use nikidb::db::DbDropGuard;
use nikidb::db::DB;
use nikidb::option::Options;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::frame::Frame;
use redcon::server;
use redcon::server::service_fn;
use redcon::Result;
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

async fn handle_event(mut conn: Connection, _cmd: Command) {
    let resp = Frame::Simple("ok".to_string());
    conn.write_frame(&resp).await;
    println!("hello")
}

#[tokio::main]
async fn main() {
    let c = Options::default();
    //let s = Server {};
    let db_holder = DbDropGuard::new(c);
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    // let db1 = &db_holder.clone();
    // let f = async move |conn: &mut Connection, _cmd: Command| match _cmd {
    //     Command::Get(_cmd) => {
    //         // let db = db1.lock().unwrap();
    //         //  let entry = db_holder.db().get(_cmd.key.as_bytes()).unwrap();
    //         // let resp = Frame::Simple("ok".to_string());
    //         // conn.write_frame(&resp).await;
    //     }
    //     Command::Set(_cmd) => {
    //         // db_holder.db().put(cmd.key.as_bytes(), &cmd.value).unwrap();
    //         // let resp = Frame::Simple("ok".to_string());
    //         // conn.write_frame(&resp).await;
    //     }
    //     Command::Publish(_cmd) => {}
    //     Command::Subscribe(_cmd) => {}
    //     Command::Unknown(_cmd) => {}
    //     Command::Unsubscribe(_) => {}
    // };
    //let func = s.handle_event;
    // server::run(listener, signal::ctrl_c(), Arc::new(Box::new(handle_event))).await;

    let s = service_fn(handle_event);
    //server::run(listener, signal::ctrl_c(), Arc::new(Box::new(f))).await;
}

// struct Server {}
// impl Server {
//     async fn handle_event(&self, conn: &mut Connection, cmd: Command) {}
// }

//     match _cmd {
//         Command::Get(_cmd) => {
//             // let db = db1.lock().unwrap();
//             //let entry = db_holder.db().get(_cmd.key.as_bytes()).unwrap();
//             let resp = Frame::Simple("ok".to_string());
//             conn.write_frame(&resp).await;
//         }
//         Command::Set(_cmd) => {
//             // db_holder.db().put(cmd.key.as_bytes(), &cmd.value).unwrap();
//             // let resp = Frame::Simple("ok".to_string());
//             // conn.write_frame(&resp).await;
//         }
//         Command::Publish(_cmd) => {}
//         Command::Subscribe(_cmd) => {}
//         Command::Unknown(_cmd) => {}
//         Command::Unsubscribe(_) => {}
//     };
// }
//println!("Hello");

// async fn process(socket: TcpStream) {
//     println!("hello world");
// }
