//! This module contains tools for using select clauses.

use super::super::parser::select;

/// This class represents a single `SELECT ...` statement or clause. `SELECT` statements can appear
/// as clauses within other expressions, so the class is written to be used easily within other
/// classes.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// The name of the table.
    ///
    /// TODO: Support general `FROM` expressions.
    pub table: String,
    /// Whether the row values must be distinct.
    pub distinct: bool,
    /// What select values are desired.
    pub value: select::Value,
    /// An optional limit on the number of rows.
    pub limit: Option<u32>,
    /// An optional starting point at which to start returning rows.
    pub offset: Option<u32>,
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
    pub fn new<S: Into<String>>(table: S,
                                distinct: bool,
                                value: select::Value,
                                limit: Option<u32>,
                                offset: Option<u32>)
                                -> SelectClause {
        SelectClause {
            table: table.into(),
            distinct: distinct,
            value: value,
            limit: limit,
            offset: offset,
        }
    }
}