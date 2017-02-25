//! This module contains the classes that represent the commands that the NanoSQL database
//! recognizes.
//!
//! All of the commands implement the [`Command`](trait.Command.html) trait. Commands
//! are grouped into the following categories:
//!
//! # Data Definition Language (DDL) Commands
//!
//! These commands manipulate the contents of the data-dictionary. The supported commands are as
//! follows:
//!
//! - `CREATE TABLE` - [`CreateCommand`](enum.CreateCommand.html#variant.Table)
//! - `DROP TABLE` - [`DropTableCommand`]()
//!
//! # Data Manipulation Language (DML) Commands
//!
//! These commands retrieve and modify the database tables themselves. *Note: In the original
//! NanoDB, these inherit from a `QueryCommand` base class. These may or may not do so in the
//! future.* The supported commands are as follows:
//!
//! - `DELETE ...` - [`DeleteCommand`]()
//! - `INSERT ...` - [`InsertCommand`]()
//! - `SELECT ...` - [`SelectCommand`]()
//! - `UPDATE ...` - [`UpdateCommand`]()
//!
//! # Transaction-Demarcation Commands
//!
//! These commands provide control over when a transaction is started or ended:
//!
//! - `BEGIN [WORK]` | `START TRANSACTION` - [`BeginTransactionCommand`]()
//! - `COMMIT [WORK]` - [`CommitTransactionCommand`]()
//! - `ROLLBACK [WORK]` - [`RollbackTransactionCommand`]()
//!
//! # Utility Commands
//!
//! These commands perform various utility operations:
//!
//! - `ANALYZE ...` - [`AnalyzeCommand`]()
//! - `EXPLAIN ...` - [`ExplainCommand`]()
//! - `EXIT` | `QUIT` - [`ExitCommand`]()

use std::any::Any;

use super::Server;

mod select;
mod show;
mod create;
mod insert;
mod drop;

pub use self::create::CreateCommand;
pub use self::drop::DropCommand;
pub use self::insert::InsertCommand;
pub use self::select::SelectCommand;
pub use self::show::ShowCommand;

use super::expressions::{Expression, ExpressionError};
use super::schema;
use super::storage::{PinError, file_manager};

#[derive(Debug, Clone, PartialEq)]
/// An error that occurred while attempting to execute a command.
pub enum ExecutionError {
    /// Unable to construct a schema given the column information provided.
    CouldNotCreateSchema(schema::Error),
    /// The command tried to open a given table and was unable to.
    CouldNotOpenTable(String),
    /// The table requested does not exist.
    TableDoesNotExist(String),
    /// The column named does not exist.
    ColumnDoesNotExist(String),
    /// The column type does not support the expression passed in.
    CannotStoreExpression(String, Expression),
    /// Parsing the expression resulted in an error.
    ExpressionError(ExpressionError),
    /// The table could not be deleted.
    CouldNotDeleteTable(file_manager::Error),
    /// A pinning error occurred.
    PinError(PinError),
    /// The command has not been fully implemented.
    Unimplemented,
}

impl From<schema::Error> for ExecutionError {
    fn from(error: schema::Error) -> ExecutionError {
        ExecutionError::CouldNotCreateSchema(error)
    }
}

impl From<PinError> for ExecutionError {
    fn from(error: PinError) -> ExecutionError {
        ExecutionError::PinError(error)
    }
}

impl From<ExpressionError> for ExecutionError {
    fn from(error: ExpressionError) -> ExecutionError {
        ExecutionError::ExpressionError(error)
    }
}

/// Trait for all commands that NanoDB supports. Command classes contain both the arguments and
/// configuration details for the command being executed, as well as the code for actually
/// performing the command. Databases tend to have large `switch` statements controlling how
/// various commands are handled, and this really isn't a very pretty way to do things. So, NanoDB
/// uses a class-hierarchy for command representation and execution.
///
/// The command class is subclassed into various command categories that relate to various
/// operations in the database.  For example, the [`QueryCommand`](#) struct represents all
/// `SELECT`, `INSERT`, `UPDATE`, and `DELETE` operations.
pub trait Command: ::std::fmt::Debug + Any {
    /// Actually performs the command.
    ///
    /// # Errors
    ///
    /// If executing the command results in an error, an
    /// [`ExecutionError`](enum.ExecutionError.html) will be returned.
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError>;

    /// Casts the command to Any. Needed to ensure polymorphism.
    fn as_any(&self) -> &Any;
}
