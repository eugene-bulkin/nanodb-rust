//! This module contains classes and utilities for storing environment information for NanoDB. These
//! environments are used for evaluating expressions.

use std::default::Default;

use ::{ColumnName, Schema};
use ::expressions::{ExpressionError, Literal};
use ::storage::{Tuple, TupleError, TupleLiteral};

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
    /// Instantiate a new environment.
    pub fn new() -> Environment { Default::default() }

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
    pub fn add_tuple<T: Tuple>(&mut self, schema: Schema, mut tuple: T) {
        self.current_schemas.push(schema);
        self.current_tuples.push(TupleLiteral::from_tuple(&mut tuple));
    }

    /// Adds a tuple to the environment with the given schema given a reference to a tuple.
    ///
    /// # Arguments
    /// * schema - the schema for the specified tuple
    /// * tuple - the tuple to be added
    pub fn add_tuple_ref<T: Tuple + ?Sized>(&mut self, schema: Schema, tuple: &mut T) {
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
    pub fn get_column_value(&mut self, col_name: &ColumnName) -> Result<Literal, ExpressionError> {
        let mut found = false;
        let mut result: Option<Result<Literal, TupleError>> = None;

        // First try to find it in the current environment.
        for i in 0..self.current_tuples.len() {
            let ref mut tuple: TupleLiteral = self.current_tuples[i];
            let ref schema: Schema = self.current_schemas[i];

            let columns = schema.find_columns(col_name);
            if columns.is_empty() {
                continue;
            }

            if found || columns.len() > 1 {
                // The original code has an !is_col_wildcard mentioning COUNT(*) expressions... but
                // I don't see how that's relevant?
                return Err(ExpressionError::AmbiguousColumnName(col_name.clone()));
            }

            result = Some(tuple.get_column_value(columns[0].0));
            found = true;
        }

        // If that doesn't work, try the parents.
        if !found && !self.parent_envs.is_empty() {
            for parent in self.parent_envs.iter_mut() {
                if let Ok(value) = parent.get_column_value(col_name) {
                    result = Some(Ok(value));
                    found = true;
                    break;
                }
            }
        }

        if !found {
            return Err(ExpressionError::CouldNotResolve(col_name.clone()));
        }

        result.unwrap().map_err(|e| ExpressionError::CouldNotRead(e))
    }
}

impl Default for Environment {
    fn default() -> Self {
        Environment {
            current_schemas: vec![],
            current_tuples: vec![],
            parent_envs: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::{Schema, ColumnType, ColumnInfo, ColumnName};
    use ::expressions::ExpressionError::*;
    use ::expressions::Literal::*;
    use ::storage::TupleLiteral;

    #[test]
    fn test_get_column_value() {
        let sch1 = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "A", "FOO"),
            ColumnInfo::with_table_name(ColumnType::Double, "B", "FOO"),
        ]).unwrap();
        let sch2 = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "A", "BAR"),
            ColumnInfo::with_table_name(ColumnType::Double, "C", "BAR"),
        ]).unwrap();
        let sch3 = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "A", "FOO"),
        ]).unwrap();
        let sch4 = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "A", "BAR"),
        ]).unwrap();
        let sch5 = Schema::with_columns(vec![
            ColumnInfo::with_table_name(ColumnType::Integer, "C", "BAR"),
        ]).unwrap();

        let mut tup1 = TupleLiteral::from_iter(vec![Int(1), Double(1.5)]);
        let mut tup2 = TupleLiteral::from_iter(vec![Int(3), Double(2.5)]);
        let mut tup3 = TupleLiteral::from_iter(vec![Int(2)]);
        let mut tup4 = TupleLiteral::from_iter(vec![Int(4)]);
        let mut tup5 = TupleLiteral::from_iter(vec![Int(6)]);

        let mut env1 = {
            let mut env = Environment::new();
            env.add_tuple_ref(sch1.clone(), &mut tup1);

            env
        };
        let mut env2 = {
            let mut env = Environment::new();
            env.add_tuple_ref(sch1.clone(), &mut tup1);
            env.add_tuple_ref(sch2.clone(), &mut tup2);

            env
        };
        let mut env3 = {
            let mut env = Environment::new();
            env.add_tuple_ref(sch3.clone(), &mut tup3);
            env.add_tuple_ref(sch4.clone(), &mut tup4);
            env.add_tuple_ref(sch5.clone(), &mut tup5);

            env
        };

        let col_a: ColumnName = (None, Some("A".into()));
        let col_b: ColumnName = (None, Some("B".into()));
        let col_c: ColumnName = (None, Some("C".into()));
        let foo_a: ColumnName = (Some("FOO".into()), Some("A".into()));
        let foo_b: ColumnName = (Some("FOO".into()), Some("B".into()));
        let foo_w: ColumnName = (Some("FOO".into()), None);
        let bar_a: ColumnName = (Some("BAR".into()), Some("A".into()));
        let bar_c: ColumnName = (Some("BAR".into()), Some("C".into()));
        let bar_w: ColumnName = (Some("BAR".into()), None);

        assert_eq!(Ok(Int(1)), env1.get_column_value(&foo_a));
        assert_eq!(Ok(Double(1.5)), env1.get_column_value(&foo_b));
        assert_eq!(Err(CouldNotResolve(bar_a.clone())), env1.get_column_value(&bar_a));
        assert_eq!(Err(CouldNotResolve(bar_c.clone())), env1.get_column_value(&bar_c));
        assert_eq!(Ok(Double(1.5)), env1.get_column_value(&foo_b));
        assert_eq!(Ok(Int(1)), env1.get_column_value(&col_a));
        assert_eq!(Ok(Double(1.5)), env1.get_column_value(&col_b));
        assert_eq!(Err(AmbiguousColumnName(foo_w.clone())), env2.get_column_value(&foo_w));

        assert_eq!(Ok(Int(1)), env2.get_column_value(&foo_a));
        assert_eq!(Ok(Double(1.5)), env2.get_column_value(&foo_b));
        assert_eq!(Ok(Int(3)), env2.get_column_value(&bar_a));
        assert_eq!(Ok(Double(2.5)), env2.get_column_value(&bar_c));
        assert_eq!(Err(AmbiguousColumnName(col_a.clone())), env2.get_column_value(&col_a));
        assert_eq!(Ok(Double(1.5)), env2.get_column_value(&col_b));
        assert_eq!(Ok(Double(2.5)), env2.get_column_value(&col_c));

        assert_eq!(Ok(Int(2)), env3.get_column_value(&foo_a));
        assert_eq!(Ok(Int(4)), env3.get_column_value(&bar_a));
        assert_eq!(Ok(Int(6)), env3.get_column_value(&bar_c));
        assert_eq!(Err(AmbiguousColumnName(col_a.clone())), env3.get_column_value(&col_a));
        assert_eq!(Err(CouldNotResolve(col_b.clone())), env3.get_column_value(&col_b));
        assert_eq!(Ok(Int(6)), env3.get_column_value(&col_c));
        assert_eq!(Ok(Int(2)), env3.get_column_value(&foo_w));
        assert_eq!(Err(AmbiguousColumnName(bar_w.clone())), env3.get_column_value(&bar_w));
    }
}