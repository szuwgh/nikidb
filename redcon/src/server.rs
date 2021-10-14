use crate::connection::Connection;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{self, Duration};

type HandleCmd = fn();

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let mut server = Server { listener };
    tokio::select! {
        res = server.run() => {
            // If an error is received here, accepting connections from the TCP
            // listener failed multiple times and the server is giving up and
            // shutting down.
            //
            // Errors encountered when handling individual connections do not
            // bubble up to this point.
            if let Err(err) = res {
                //error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // The shutdown signal has been received.
           // info!("shutting down");
        }
    }
}

struct Server {
    listener: TcpListener,
}

impl Server {
    async fn run(&mut self) -> crate::Result<()> {
        loop {
            let socket = self.accept().await?;
            let con = Connection::new(socket);
            let mut handler = Handler { connection: con };
            tokio::spawn(async move {
                // Process the connection. If an error is encountered, log it.
                handler.run().await;
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // Try to accept a few times
        loop {
            // Perform the accept operation. If a socket is successfully
            // accepted, return it. Otherwise, save the error.
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // Accept has failed too many times. Return the error.
                        return Err(err.into());
                    }
                }
            }

            // Pause execution until the back off period elapses.
            time::sleep(Duration::from_secs(backoff)).await;

            // Double the back off
            backoff *= 2;
        }
    }
}

struct Handler {
    connection: Connection,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        loop {
            tokio::select! {
                res = self.connection.read() => res?,
            };
        }
    }
}
