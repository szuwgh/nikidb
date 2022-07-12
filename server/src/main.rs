use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use nikidb::db::DB;
use nikidb::db::DEFAULT_OPTIONS;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::frame::Frame;
use redcon::server;
use redcon::server::AsyncFn;
use redcon::Result;
use std::cell::RefCell;
use std::fs;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;

#[tokio::main]
async fn main() {
    print_banner();
    // let c = Options::default();
    let db = DB::open("./test.db", DEFAULT_OPTIONS).unwrap();
    // let mut tx = db.begin_rwtx();
    // tx.create_bucket("default".as_bytes()).unwrap();
    // tx.commit();
    let handler = Handler { db };
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("nikidb is running, ready to accept connections.");
    server::run(listener, signal::ctrl_c(), Arc::new(Box::new(handler))).await;
}

fn print_banner() {
    let contents = fs::read_to_string("./resource/banner.txt").unwrap();
    println!("{}", contents);
}

#[derive(Clone)]
struct Handler {
    db: DB,
}

impl AsyncFn for Handler {
    fn call<'a>(&'a self, _conn: &'a mut Connection, _cmd: Command) -> BoxFuture<'a, ()> {
        Box::pin(async {
            match _cmd {
                Command::Get(_cmd) => {
                    let v = {
                        let mut tx = self.db.begin_tx();
                        let b = tx.bucket("default".as_bytes()).unwrap();
                        let x = match b.get(_cmd.key.as_bytes()) {
                            Some(v) => String::from_utf8(v.to_vec()).unwrap(),
                            None => "not found".to_owned(),
                        };
                        tx.rollback().unwrap();
                        x
                    };
                    let resp = Frame::Simple(v);
                    _conn.write_frame(&resp).await.unwrap();
                }
                Command::Set(_cmd) => {
                    {
                        let mut tx = self.db.begin_rwtx();
                        let b = tx.bucket("default".as_bytes()).unwrap();
                        b.put(_cmd.key.as_bytes(), &_cmd.value).unwrap();
                        tx.commit().unwrap();
                    }
                    let resp = Frame::Simple("OK".to_string());
                    _conn.write_frame(&resp).await.unwrap();
                }
                Command::Publish(_cmd) => {}
                Command::Subscribe(_cmd) => {}
                Command::Unknown(_cmd) => {}
                Command::Unsubscribe(_) => {}
            };
        })
    }
}
