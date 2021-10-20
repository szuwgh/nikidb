use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use nikidb::db::DbDropGuard;
use nikidb::db::DB;
use nikidb::option::Options;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::frame::Frame;
use redcon::server;
use redcon::server::AsyncFn;
use redcon::Result;
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

#[tokio::main]
async fn main() {
    let c = Options::default();
    let db_holder = DbDropGuard::new(c);
    let handler = Handler { db_holder };
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    server::run(listener, signal::ctrl_c(), Arc::new(Box::new(handler))).await;
}

#[derive(Clone)]
struct Handler {
    db_holder: DbDropGuard,
}

impl AsyncFn for Handler {
    fn call<'a>(&'a self, _conn: &'a mut Connection, _cmd: Command) -> BoxFuture<'a, ()> {
        Box::pin(async {
            match _cmd {
                Command::Get(_cmd) => {
                    let entry = self.db_holder.db().get(_cmd.key.as_bytes()).unwrap();
                    let resp = Frame::Simple(String::from_utf8(entry.value).unwrap());
                    _conn.write_frame(&resp).await;
                }
                Command::Set(_cmd) => {
                    self.db_holder
                        .db()
                        .put(_cmd.key.as_bytes(), &_cmd.value)
                        .unwrap();
                    let resp = Frame::Simple("OK".to_string());
                    _conn.write_frame(&resp).await;
                }
                Command::Publish(_cmd) => {}
                Command::Subscribe(_cmd) => {}
                Command::Unknown(_cmd) => {}
                Command::Unsubscribe(_) => {}
            };
        })
    }
}
