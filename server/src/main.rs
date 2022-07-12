use futures::future::BoxFuture;
use nikidb::db::DB;
use nikidb::db::DEFAULT_OPTIONS;
use nikidb::error::{NKError, NKResult};
use nikidb::tx::Tx;
use redcon::cmd::Command;
use redcon::connection::Connection;
use redcon::frame::Frame;
use redcon::server;
use redcon::server::AsyncFn;
use std::fs;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
async fn main() {
    print_banner();
    // let c = Options::default();
    let db = DB::open("./test.db", DEFAULT_OPTIONS).unwrap();
    // let mut tx = db.begin_rwtx();
    // tx.create_bucket("default".as_bytes()).unwrap();
    // tx.commit();

    db.update(Box::new(|tx: &mut Tx| -> NKResult<()> {
        match tx.create_bucket("default".as_bytes()) {
            Ok(_) => println!("create default bucket success"),
            Err(NKError::ErrBucketExists(e)) => println!("{} bucket exist", e),
            Err(e) => panic!("error"),
        }
        Ok(())
    }))
    .unwrap();

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
                        let mut x: Option<String> = None;
                        self.db
                            .view(Box::new(|tx: &mut Tx| -> NKResult<()> {
                                let b = tx.bucket("default".as_bytes())?;
                                x = match b.get(_cmd.key.as_bytes()) {
                                    Some(v) => Some(String::from_utf8(v.to_vec()).unwrap()),
                                    None => Some("not found".to_owned()),
                                };
                                Ok(())
                            }))
                            .unwrap();
                        x
                    };
                    let resp = Frame::Simple(v.unwrap());
                    _conn.write_frame(&resp).await.unwrap();
                }
                Command::Set(_cmd) => {
                    {
                        self.db
                            .update(Box::new(|tx: &mut Tx| -> NKResult<()> {
                                let b = tx.bucket("default".as_bytes())?;
                                b.put(_cmd.key.as_bytes(), &_cmd.value).unwrap();
                                Ok(())
                            }))
                            .unwrap();
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
