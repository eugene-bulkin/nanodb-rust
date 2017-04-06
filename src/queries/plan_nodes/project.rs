//! This module provides the project plan node.

use ::expressions::{Environment, Expression, ExpressionError, SelectValue};
use ::queries::plan_nodes::PlanNode;
use ::queries::planning::{PlanError, PlanResult};
use ::relations::{ColumnInfo, ColumnName, NameError, Schema, SchemaError, column_name_to_string};
use ::storage::{Tuple, TupleLiteral, TupleError};

/// An error that could occur during projection.
#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    /// The specified column does not exist.
    ColumnDoesNotExist(ColumnName),
    /// The specified column is ambiguous.
    ColumnAmbiguous(ColumnName),
    /// Unable to resolve the expression given.
    CouldNotResolve(Expression, Box<ExpressionError>),
    /// Unable to read a column value due to some tuple error.
    CouldNotReadColumnValue(ColumnName, TupleError),
    /// Some other schema error occurred.
    SchemaError(SchemaError),
}

impl From<SchemaError> for Error {
    fn from(e: SchemaError) -> Error {
        if let SchemaError::Name(ref ne) = e {
            if let NameError::Duplicate(ref col_info) = *ne {
                Error::ColumnAmbiguous(col_info.get_column_name())
            } else if let NameError::NoName(ref col_info) = *ne {
                Error::ColumnDoesNotExist(col_info.get_column_name())
            } else {
                Error::SchemaError(SchemaError::Name(ne.clone()))
            }
        } else {
            Error::SchemaError(e)
        }
    }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::ColumnDoesNotExist(ref col_name) => {
                write!(f, "the column {} does not exist", column_name_to_string(col_name))
            }
            Error::ColumnAmbiguous(ref col_name) => {
                write!(f, "the column {} is ambiguous", column_name_to_string(col_name))
            }
            Error::CouldNotResolve(ref expr, ref e) => {
                write!(f, "the expression {} could not be resolved: {}", expr, e)
            }
            Error::CouldNotReadColumnValue(ref col_name, ref e) => {
                write!(f, "the column value for column {} could not be read: {}", column_name_to_string(col_name), e)
            }
            Error::SchemaError(ref e) => {
                write!(f, "some schema error occurred: {}", e)
            }
        }
    }
}

pub use self::Error as ProjectError;

/// PlanNode representing the `SELECT` clause in a SQL query. This is the relational algebra Project
/// operator.
pub struct ProjectNode<'a> {
    child: Box<PlanNode + 'a>,
    values: Vec<SelectValue>,
    current_tuple: Option<Box<Tuple>>,
    input_schema: Schema,
    output_schema: Option<Schema>,
}

impl<'a> ProjectNode<'a> {
    /// Instantiate a new project node.
    ///
    /// # Argument
    /// * child - The child of the node.
    /// * values - The select values of the query.
    pub fn new(child: Box<PlanNode + 'a>, values: Vec<SelectValue>) -> ProjectNode<'a> {
        let schema = child.get_schema();
        ProjectNode {
            child: child,
            values: values,
            current_tuple: None,
            input_schema: schema,
            // This will only be Some(...) if the node has been prepared!
            output_schema: None,
        }
    }

    fn project_tuple(&self, tuple: &mut Tuple) -> PlanResult<TupleLiteral> {
        let mut result = TupleLiteral::new();
        for select_value in self.values.iter() {
            match *select_value {
                SelectValue::Expression { ref expression, .. } => {
                    if let Expression::ColumnValue(ref column_name) = *expression {
                        let matches = self.input_schema.find_columns(column_name);
                        if matches.is_empty() {
                            return Err(Error::ColumnDoesNotExist(column_name.clone()).into());
                        }
                        if matches.len() > 1 {
                            return Err(Error::ColumnAmbiguous(column_name.clone()).into());
                        }
                        let (ref idx, _) = matches[0];
                        let value = try!(tuple.get_column_value(*idx).map_err(|e| ProjectError::CouldNotReadColumnValue(column_name.clone(), e)));
                        result.add_value(value);
                    } else {
                        let mut env = Environment::new();
                        env.add_tuple_ref(self.input_schema.clone(), tuple);
                        let value = try!(expression.evaluate(&mut Some(&mut env))
                            .map_err(|e| ProjectError::CouldNotResolve(expression.clone(), Box::new(e))));
                        result.add_value(value);
                    }
                }
                SelectValue::WildcardColumn { ref table } => {
                    // This value is a wildcard.  Find the columns that match the
                    // wildcard, then add their values one by one.

                    // Wildcard expressions cannot rename their results.
                    match *table {
                        Some(ref name) => {
                            // Need to find all columns that are associated with the
                            // specified table.
                            let matches = self.input_schema.find_columns(&(Some(name.clone()), None));

                            for (idx, _) in matches {
                                let value = tuple.get_column_value(idx).unwrap();
                                result.add_value(value);
                            }
                        }
                        None => {
                            // No table is specified, so this is all columns in the child schema.
                            result.append_tuple(tuple);
                        }
                    }
                }
            }
        }
        Ok(result)
    }

    fn get_next_tuple_helper(&mut self) -> PlanResult<()> {
        if self.output_schema.is_none() {
            return Err(PlanError::NodeNotPrepared);
        }
        let mut next = {
            let next = try!(self.child.get_next_tuple());
            if next.is_none() {
                self.current_tuple = None;
                return Ok(());
            }
            TupleLiteral::from_tuple(next.unwrap())
        };
        self.current_tuple = Some(Box::new(try!(self.project_tuple(&mut next))));
        Ok(())
    }
}

impl<'a> PlanNode for ProjectNode<'a> {
    fn get_schema(&self) -> Schema {
        self.output_schema.clone().unwrap_or(Schema::new())
    }

    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>> {
        try!(self.get_next_tuple_helper());

        Ok(match self.current_tuple.as_mut() {
            Some(mut boxed_tuple) => Some(&mut **boxed_tuple),
            _ => None,
        })
    }

    fn prepare(&mut self) -> PlanResult<()> {
        let mut default_env = {
            let mut env = Environment::new();
            let default_tuple = self.input_schema.default_tuple();
            env.add_tuple(self.input_schema.clone(), default_tuple);
            env
        };

        if self.input_schema.is_empty() {
            return Err(PlanError::NodeNotPrepared);
        }

        let mut result = Schema::new();
        for select_value in self.values.iter() {
            // Kind of weird looking, but we're doing this because we want to ensure that the plan
            // error that occurs is specifically one about projection, not a generic schema issue.
            let result: Result<_, ProjectError> = match *select_value {
                SelectValue::Expression { ref expression, ref alias } => {
                    // Determining the schema is relatively straightforward.  The
                    // statistics, unfortunately, are a different matter:  if the
                    // expression is a simple column-reference then we can look up
                    // the stats from the subplan, but if the expression is an
                    // arithmetic operation, we need to guess...

                    let col_info = if let Expression::ColumnValue(ref column_name) = *expression {
                        let matches = self.input_schema.find_columns(column_name);
                        if matches.is_empty() {
                            return Err(Error::ColumnDoesNotExist(column_name.clone()).into());
                        }
                        if matches.len() > 1 {
                            return Err(Error::ColumnAmbiguous(column_name.clone()).into());
                        }
                        let (ref idx, _) = matches[0];
                        ColumnInfo::with_name(self.input_schema[*idx].column_type,
                                              match *alias {
                                                  Some(ref name) => name.clone(),
                                                  None => column_name.1.clone().unwrap(),
                                              })
                    } else {
                        // First, see if we can just figure out what it is without a tuple (e.g. it's a
                        // constant expression).
                        if let Ok(literal) = expression.evaluate(&mut None) {
                            let col_type = literal.get_column_type();
                            ColumnInfo::with_name(col_type,
                                                  match *alias {
                                                      Some(ref name) => name.clone(),
                                                      None => format!("{}", literal),
                                                  })
                        } else {
                            match expression.evaluate(&mut Some(&mut default_env)) {
                                Ok(literal) => {
                                    let col_type = literal.get_column_type();
                                    ColumnInfo::with_name(col_type,
                                                          match *alias {
                                                              Some(ref name) => name.clone(),
                                                              None => format!("{}", expression),
                                                          })
                                }
                                Err(e) => {
                                    return Err(ProjectError::CouldNotResolve(expression.clone(), Box::new(e)).into());
                                },
                            }
                        }
                    };
                    result.add_column(col_info).map_err(Into::into)
                }
                SelectValue::WildcardColumn { ref table } => {
                    // This value is a wildcard.  Find the columns that match the
                    // wildcard, then add their values one by one.

                    // Wildcard expressions cannot rename their results.
                    let column_infos: Vec<ColumnInfo> = match *table {
                        Some(ref name) => {
                            // Need to find all columns that are associated with the
                            // specified table.
                            let matches = self.input_schema.find_columns(&(Some(name.clone()), None));
                            matches.iter().map(|&(_, ref info)| info.clone()).collect()
                        }
                        None => {
                            // No table is specified, so this is all columns in the child schema.
                            self.input_schema.iter().map(Clone::clone).collect()
                        }
                    };
                    result.add_columns(column_infos).map_err(Into::into)
                }
            };
            try!(result);
        }
        self.output_schema = Some(result);
        Ok(())
    }
}
