//! A module for handling select values.

use ::expressions::Expression;

/// This class represents a single expression in a `SELECT` clause.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectValue {
    /// An {@link edu.caltech.nanodb.expressions.Expression} that evaluates to a single value
    Expression {
        /// The expression in the value.
        expression: Expression,
        /// An optional alias for the value.
        alias: Option<String>,
    },
    /// A wildcard expression like "`*`" or "`loan.*`" that evaluates to a set of column values
    WildcardColumn {
        /// An optional table for the wildcard.
        table: Option<String>,
    }, // TODO: A scalar subquery (often correlated) that evaluates to a single column and row
}

impl ::std::fmt::Display for SelectValue {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            SelectValue::Expression { ref expression, ref alias } => {
                match *alias {
                    Some(ref name) => write!(f, "({}) AS {}", expression, name),
                    None => write!(f, "{}", expression),
                }
            }
            SelectValue::WildcardColumn { ref table } => {
                match *table {
                    Some(ref name) => write!(f, "{}.*", name),
                    None => write!(f, "*"),
                }
            }
        }
    }
}
