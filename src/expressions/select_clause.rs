//! This module contains tools for using select clauses.

use super::super::parser::select;
use super::super::expressions::{FromClause, Expression};
use ::storage::{FileManager, TableManager};
use ::Schema;
use ::commands::ExecutionError;

/// This class represents a single `SELECT ...` statement or clause. `SELECT` statements can appear
/// as clauses within other expressions, so the class is written to be used easily within other
/// classes.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// The from clause for the `SELECT` query.
    pub from_clause: FromClause,
    /// Whether the row values must be distinct.
    pub distinct: bool,
    /// What select values are desired.
    pub value: select::Value,
    /// An optional limit on the number of rows.
    pub limit: Option<u32>,
    /// An optional starting point at which to start returning rows.
    pub offset: Option<u32>,
    /// The optional where clause.
    pub where_expr: Option<Expression>,
}

impl SelectClause {
    /// Creates a new select clause.
    ///
    /// # Arguments
    /// * table - The name of the table. TODO: This should be an arbitrary `FROM` clause.
    /// * distinct - Whether the values should be distinct.
    /// * value - The select values or wildcard being selected.
    /// * limit - Optionally, how many rows to return.
    /// * offset - Optionally, the index at which to start returning rows.
    /// * where_expr - Optionally, the WHERE clause.
    pub fn new(from_clause: FromClause,
               distinct: bool,
               value: select::Value,
               limit: Option<u32>,
               offset: Option<u32>,
               where_expr: Option<Expression>)
               -> SelectClause {
        SelectClause {
            from_clause: from_clause,
            distinct: distinct,
            value: value,
            limit: limit,
            offset: offset,
            where_expr: where_expr,
        }
    }

    /// Compute the schema for this select clause.
    pub fn compute_schema(&mut self, file_manager: &FileManager, table_manager: &TableManager) -> Result<Schema, ExecutionError> {
        // TODO
        // For now, just return the from clause schema.
        self.from_clause.compute_schema(file_manager, table_manager)
    }
}

impl ::std::fmt::Display for SelectClause {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        try!(write!(f, "SelectClause[\n"));
        if let select::Value::Values(ref values) = self.value {
            try!(write!(f, "\tvalues={:?}\n", values));
        }
        try!(write!(f, "\tfrom={:?}\n", self.from_clause));

        if let Some(ref expr) = self.where_expr {
            try!(write!(f, "\twhere={}\n", expr));
        }

        // TODO: GROUP BY, ORDER BY, HAVING, correlated with?
        write!(f, "]")
    }
}