//! The module containing NanoDB server instances.

use std::fs;
use std::path::PathBuf;
use super::super::commands::Command;
use super::super::storage::{FileManager, TableManager};

/// This class provides the entry-point operations for managing the database server, and executing
/// commands against it. While it is certainly possible to implement these operations outside of
/// this class, these implementations are strongly recommended since they include all necessary
/// steps.
pub struct Server {
    /// The server's file manager instance.
    pub file_manager: FileManager,
    /// The server's table manager instance.
    pub table_manager: TableManager,
}

impl Server {
    /// Instantiates a new server instance.
    pub fn new() -> Server {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("datafiles");
        if !path.exists() {
            fs::create_dir(&path).unwrap();
        }
        let file_manager = FileManager::with_directory(&path).unwrap();
        Server {
            file_manager: file_manager,
            table_manager: TableManager {},
        }
    }

    /// Executes a provided command.
    ///
    /// If an error occurs in the command, it is printed to the console.
    pub fn handle_command(&mut self, mut command: Box<Command>) {
        match command.execute(&self) {
            Err(e) => {
                println!("Command error: {:?}", e);
            }
            Ok(_) => {}
        }
    }
}
