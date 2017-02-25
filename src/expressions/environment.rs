//! This module contains classes and utilities for storing environment information for NanoDB. These
//! environments are used for evaluating expressions.

use super::{ExpressionError, Literal};
use super::super::{ColumnName, Schema};
use super::super::storage::{Tuple, TupleLiteral};

/// This class holds the environment for evaluating expressions that include symbols. For example,
/// in the SQL command:
///
/// ```SELECT a, b + 5 FROM t WHERE c < 20;```
///
/// All of the expressions refer to columns in the current tuple being considered
/// from the table `t`, and thus need to be able to access the current
/// tuple. This is the role that the environment class serves.
///
/// An important detail about the environment is that a single tuple's schema can hold values from
/// multiple tables, such as when a tuple is produced as the result of a join operation between two
/// tables.
///
/// # Design
/// This class could be applied in several different ways.
///
/// Any SELECT clause really could (or should) have its own environment associated with it, because
/// it will reference tables. In addition, a derived table (a named subquery in the FROM clause)
/// can also be referred to by name. So, we will have to devise a strategy for managing
/// environments properly. Some plan-nodes will have to be responsible for updating environments,
/// but definitely not all will do so.
///
/// It probably makes the most sense to give *every* plan-node its own environment-reference. If
/// the reference is null, the node could get its parent's environment. Or, we could set all
/// plan-nodes to have a specific environment, and just manage that assignment process carefully.
///
/// Environments can refer to a parent environment, for cases where a query contains subqueries.
/// The subqueries can refer to the same table(s) as the outer query, and thus they need their own
/// environment to track that information. This becomes especially useful with correlated
/// subqueries, as the inner query needs to be completely reevaluated for each value of the outer
/// query.
///
/// Matching a symbol name goes from child to parent. If a child environment contains a value for a
/// particular symbol, that value is returned. It is only if the child environment *doesn't*
/// contain a value that the parent environment is utilized.
#[derive(Clone, Debug, PartialEq)]
pub struct Environment {
    current_schemas: Vec<Schema>,
    current_tuples: Vec<TupleLiteral>,
    parent_envs: Vec<Environment>,
}

impl Environment {
    /// Reset the environment.
    pub fn clear(&mut self) {
        self.current_schemas.clear();
        self.current_tuples.clear();
    }

    /// Add a parent environment.
    ///
    /// # Arguments
    /// * env - The environment to add as a parent.
    pub fn add_parent_env(&mut self, env: Environment) {
        self.parent_envs.push(env);
    }

    /// Adds a tuple to the environment with the given schema.
    ///
    /// # Arguments
    /// * schema - the schema for the specified tuple
    /// * tuple - the tuple to be added
    pub fn add_tuple<T: Tuple>(&mut self, schema: Schema, tuple: T) {
        self.current_schemas.push(schema);
        self.current_tuples.push(TupleLiteral::from_tuple(tuple));
    }

    /// Returns the list of tuples being considered.
    pub fn get_current_tuples(&self) -> Vec<TupleLiteral> {
        self.current_tuples.clone()
    }
    /// Get the actual value at the specified column.
    ///
    /// # Arguments
    /// * col_name - the name of the column.
    pub fn get_column_value(&self, col_name: &ColumnName) -> Result<Literal, ExpressionError> {
        let mut found = false;
        let mut result: Option<Literal> = None;

        let is_col_wildcard = col_name.1.is_none();

        // First try to find it in the current environment.
        for i in 0..self.current_tuples.len() {
            let ref tuple: TupleLiteral = self.current_tuples[i];
            let ref schema: Schema = self.current_schemas[i];

            let columns = schema.find_columns(col_name);
            if columns.is_empty() {
                continue;
            }

            if found || columns.len() > 1 && !is_col_wildcard {
                return Err(ExpressionError::AmbiguousColumnName(col_name.clone()));
            }

            result = Some(tuple.get_column_value(columns[0].0));
            found = true;
        }

        // If that doesn't work, try the parents.
        if !found && !self.parent_envs.is_empty() {
            for parent in self.parent_envs.iter() {
                if let Ok(value) = parent.get_column_value(col_name) {
                    result = Some(value);
                    found = true;
                    break;
                }
            }
        }

        if !found {
            return Err(ExpressionError::CouldNotResolve(col_name.clone()));
        }

        Ok(result.unwrap())
    }
}
