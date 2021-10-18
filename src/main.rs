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
    Get { key: String },
    Set {
        key: String,
        value: Bytes,
        expires: Option<Duration>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // `()` can be used when no completer is required

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
                            println!("{}", key);
                            let res = cli.get(key.as_str()).await;
                            match res {
                                Ok(Some(e)) => {
                                    println!("{:?}", e);
                                }
                                Ok(None) => {}
                                Err(_) => {}
                            }
                        }
                        Command::Set {
                            key,
                            value,
                            expires: Some(expires),
                        } => {
                            cli.set(key.as_str(), value).await;
                        }
                        Command::Set {
                            key,
                            value,
                            expires: None,
                        } => {
                            println!("OK");
                        }
                    },
                    None => continue,
                }
                //  Cli::from_clap();
                // rl.add_history_entry(line.as_str());
                // println!("Line: {}", line);
                // let res = cli.write(line.as_bytes()).await;
                // match res {
                //     Ok(Some(e)) => println!("{:?}", e),
                //     Ok(None) => break,
                //     Err(_) => break,
                // }
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
        _ => None,
    }
}
