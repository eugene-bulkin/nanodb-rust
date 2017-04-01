//! This module contains tools for using select clauses.

use std::default::Default;

use ::Schema;
use ::commands::ExecutionError;
use ::expressions::{Expression, FromClause, SelectValue};
use ::storage::{FileManager, TableManager};

/// This class represents a single `SELECT ...` statement or clause. `SELECT` statements can appear
/// as clauses within other expressions, so the class is written to be used easily within other
/// classes.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectClause {
    /// The optional from clause for the `SELECT` query.
    pub from_clause: Option<FromClause>,
    /// Whether the row values must be distinct.
    pub distinct: bool,
    /// What select values are desired.
    pub values: Vec<SelectValue>,
    /// An optional limit on the number of rows.
    pub limit: Option<u32>,
    /// An optional starting point at which to start returning rows.
    pub offset: Option<u32>,
    /// The optional where clause.
    pub where_expr: Option<Expression>,
    from_schema: Option<Schema>,
}

impl Default for SelectClause {
    fn default() -> SelectClause {
        // This select clause does nothing, but it's a reasonable default.
        SelectClause {
            from_clause: None,
            distinct: false,
            values: vec![],
            limit: None,
            offset: None,
            where_expr: None,
            from_schema: None,
        }
    }
}

impl SelectClause {
    /// Creates a new select clause.
    ///
    /// # Arguments
    /// * from_clause - The FROM clause.
    /// * distinct - Whether the values should be distinct.
    /// * values - The select values being selected.
    /// * limit - Optionally, how many rows to return.
    /// * offset - Optionally, the index at which to start returning rows.
    /// * where_expr - Optionally, the WHERE clause.
    pub fn new(from_clause: FromClause,
               distinct: bool,
               values: Vec<SelectValue>,
               limit: Option<u32>,
               offset: Option<u32>,
               where_expr: Option<Expression>)
               -> SelectClause {
        SelectClause {
            from_clause: Some(from_clause),
            distinct: distinct,
            values: values,
            limit: limit,
            offset: offset,
            where_expr: where_expr,
            ..Default::default()
        }
    }

    /// Creates a new scalar select clause. This is a clause like
    /// `SELECT 3, (SELECT MAX(a) FROM foo)`, which returns a single row and does not depend on any
    /// tables.
    ///
    /// # Arguments
    /// * values - The select values being selected.
    pub fn scalar(values: Vec<SelectValue>) -> SelectClause {
        SelectClause {
            values: values,
            ..Default::default()
        }
    }

    /// Checks if the projection is trivial.
    pub fn is_trivial_project(&self) -> bool {
        if self.values.len() == 1 {
            if let SelectValue::WildcardColumn { ref table } = self.values[0] {
                if table.is_none() {
                    return true;
                }
            }
        }
        false
    }

    /// Compute the schema for this select clause.
    pub fn compute_schema(&mut self,
                          file_manager: &FileManager,
                          table_manager: &TableManager)
                          -> Result<Schema, ExecutionError> {
        // TODO
        // For now, just return the from clause schema.
        match self.from_clause {
            Some(ref mut clause) => {
                let schema = try!(clause.compute_schema(file_manager, table_manager));
                self.from_schema = Some(schema.clone());
                Ok(schema)
            },
            None => {
                // TODO
                Err(ExecutionError::Unimplemented)
            }
        }

    }
}

impl ::std::fmt::Display for SelectClause {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        try!(write!(f, "SelectClause[\n"));
        if !self.is_trivial_project() {
            let values: Vec<String> = self.values.iter().map(|f| format!("{}", f)).collect();
            try!(write!(f, "\tvalues={}\n", values.join(", ")));
        }

        if let Some(ref clause) = self.from_clause {
            try!(write!(f, "\tfrom={}\n", clause));
        }

        if let Some(ref expr) = self.where_expr {
            try!(write!(f, "\twhere={}\n", expr));
        }

        if let Some(limit) = self.limit {
            try!(write!(f, "\tlimit={}\n", limit));
        }

        if let Some(offset) = self.offset {
            try!(write!(f, "\toffset={}\n", offset));
        }

        // TODO: GROUP BY, ORDER BY, HAVING, correlated with?
        write!(f, "]")
    }
}
