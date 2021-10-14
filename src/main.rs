use redcon::client;
use rustyline::error::ReadlineError;
use rustyline::Editor;
#[tokio::main(flavor = "current_thread")]
async fn main() {
    // `()` can be used when no completer is required

    let mut cli = client::connect("127.0.0.1:6379").await.unwrap();
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("nikidb> ");
        match readline {
            Ok(line) => {
                // rl.add_history_entry(line.as_str());
                println!("Line: {}", line);
                cli.write(line.as_bytes()).await;
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
