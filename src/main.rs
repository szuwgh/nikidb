use bytes::Bytes;
use redcon::client;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::num::ParseIntError;
use std::time::Duration;

struct Cli {
    command: Command,
}

enum Command {
    /// Get the value of key.
    Get {
        key: String,
    },
    Set {
        key: String,
        value: Bytes,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut cli = client::connect("127.0.0.1:6379").await.unwrap();
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("nikidb> ");
        match readline {
            Ok(line) => {
                let command = parse_redir_command(line);  
                match command {
                    Some(cmd) => match cmd {
                        Command::Get { key } => {
                            let res = cli.get(key.as_str()).await;
                            match res {
                                Ok(Some(e)) => {
                                    println!("{:?}", String::from_utf8_lossy(&e));
                                }
                                Ok(None) => {}
                                Err(_) => {}
                            }
                        }
                        Command::Set {
                            key,
                            value,
                            // expires: Some(expires),
                        } => {
                            let res = cli.set(key.as_str(), value).await;
                            match res {
                                Ok(()) => {
                                    println!("ok");
                                }
                                Err(_) => {
                                    println!("fail");
                                }
                            }
                        }
                    },
                    None => continue,
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    // rl.save_history("history.txt").unwrap();
}

fn parse_redir_command(cmd: String) -> Option<Command> {
    let split_word: Vec<&str> = cmd.split(" ").collect();
    match split_word[0] {
        "get" => {
            return Some(Command::Get {
                key: String::from(split_word[1]),
            })
        }
        "set" => {
            return Some(Command::Set {
                key: String::from(split_word[1]),
                value: Bytes::copy_from_slice(split_word[2].as_bytes()),
            })
        }
        _ => None,
    }
}
