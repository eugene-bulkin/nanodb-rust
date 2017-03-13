//! The module containing NanoDB server instances.

use std::fs;
use std::path::{Path, PathBuf};

use ::commands::Command;
use ::storage::{FileManager, TableManager};

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
    /// Instantiates a new server instance, storing data files in the datafiles/ folder.
    pub fn new() -> Server {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("datafiles");
        Server::with_data_path(path)
    }

    /// Instantiates a new server instance with the data files stored at the provided path.
    pub fn with_data_path<P: AsRef<Path>>(path: P) -> Server {
        let path = path.as_ref();
        if !path.exists() {
            fs::create_dir(&path).unwrap();
        }
        let file_manager = FileManager::with_directory(&path).unwrap();
        Server {
            file_manager: file_manager,
            table_manager: TableManager::new(),
        }
    }

    /// Executes a provided command.
    ///
    /// If an error occurs in the command, it is printed to the console.
    pub fn handle_command(&mut self, mut command: Box<Command>) {
        if let Err(e) = command.execute(self) {
            println!("{}", e);
        }
    }
}
