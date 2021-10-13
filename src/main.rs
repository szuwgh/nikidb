use rustyline::error::ReadlineError;
use rustyline::Editor;
mod cli;
use cli::Client;

fn main() {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new();
    loop {
        let readline = rl.readline("nikidb> ");
        match readline {
            Ok(line) => {
                // rl.add_history_entry(line.as_str());
                println!("Line: {}", line);
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
