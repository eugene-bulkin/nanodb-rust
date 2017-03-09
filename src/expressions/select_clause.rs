//! This module contains tools for using select clauses.

use super::super::parser::select;
use super::super::expressions::{FromClause, Expression};

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
}