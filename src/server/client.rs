//! The module containing NanoDB client instances.

use nom::IResult::*;
use rustyline::Editor;
use rustyline::error::ReadlineError;

use ::parser::statements;
use ::server::Server;

const PROMPT: &'static str = "CMD> ";

/// A NanoDB client which connects to the server and executes user-input commands.
pub struct Client {
    server: Server,
}

impl Client {
    /// Creates a new NanoDB client.
    pub fn new() -> Client {
        Client { server: Server::new() }
    }

    /// Runs the NanoDB client, prompting user for input.
    pub fn run(&mut self) {
        println!("Welcome to NanoDB.  Exit with EXIT or QUIT command.\n");

        let mut rl = Editor::<()>::new();
        rl.load_history(".history").unwrap_or(());
        loop {
            let readline = rl.readline(PROMPT);
            match readline {
                Ok(line) => {
                    rl.add_history_entry(&line);
                    match statements(line.as_bytes()) {
                        Done(_, stmts) => {
                            for stmt in stmts {
                                self.server.handle_command(stmt);
                            }
                        }
                        Error(e) => {
                            println!("Parser Error: {:?}", e);
                        }
                        Incomplete(n) => {
                            println!("Parser Error: {:?}", n);
                        }
                    }
                }
                Err(ReadlineError::Eof) |
                Err(ReadlineError::Interrupted) => {
                    println!("Bye");
                    break;
                }
                Err(e) => {
                    println!("{:?}", e);
                    break;
                }
            }
        }
        rl.save_history(".history").unwrap();
    }
}
