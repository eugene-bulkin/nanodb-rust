//! This module provides the project plan node.

use ::{ColumnInfo, Schema};
use ::expressions::{Environment, Expression, SelectValue};
use std::default::Default;
use ::storage::{Tuple, TupleLiteral};
use super::PlanNode;

use super::super::{PlanError, PlanResult};

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
                            return Err(PlanError::ColumnDoesNotExist(column_name.clone()));
                        }
                        if matches.len() > 1 {
                            return Err(PlanError::Unimplemented);
                        }
                        let (ref idx, _) = matches[0];
                        // TODO: Propagate error
                        let value = tuple.get_column_value(*idx).unwrap();
                        result.add_value(value);
                    } else {
                        let mut env: Environment = Default::default();
                        env.add_tuple_ref(self.input_schema.clone(), tuple);
                        // TODO: Propagate error
                        let value = expression.evaluate(&mut Some(&mut env)).unwrap();
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
            let mut env: Environment = Default::default();
            let default_tuple = self.input_schema.default_tuple();
            env.add_tuple(self.input_schema.clone(), default_tuple);
            env
        };

        if self.input_schema.is_empty() {
            return Err(PlanError::NodeNotPrepared);
        }

        let mut result = Schema::new();
        for select_value in self.values.iter() {
            match *select_value {
                SelectValue::Expression { ref expression, ref alias } => {
                    // Determining the schema is relatively straightforward.  The
                    // statistics, unfortunately, are a different matter:  if the
                    // expression is a simple column-reference then we can look up
                    // the stats from the subplan, but if the expression is an
                    // arithmetic operation, we need to guess...

                    if let Expression::ColumnValue(ref column_name) = *expression {
                        let matches = self.input_schema.find_columns(column_name);
                        if matches.is_empty() {
                            return Err(PlanError::ColumnDoesNotExist(column_name.clone()));
                        }
                        if matches.len() > 1 {
                            // TODO: Return a real error here
                            return Err(PlanError::Unimplemented);
                        }
                        let (ref idx, _) = matches[0];
                        try!(result.add_column(ColumnInfo::with_name(self.input_schema[*idx].column_type,
                                                              match *alias {
                                                                  Some(ref name) => name.clone(),
                                                                  None => column_name.1.clone().unwrap(),
                                                              }))
                            .map_err(|_| PlanError::Unimplemented)); // TODO: Return a real error here
                    } else {
                        // First, see if we can just figure out what it is without a tuple (e.g. it's a
                        // constant expression).
                        // TODO: Return real errors here, and maybe find a way to combine the two cases.
                        if let Ok(literal) = expression.evaluate(&mut None) {
                            let col_type = literal.get_column_type();
                            try!(result.add_column(ColumnInfo::with_name(col_type,
                                                                  match *alias {
                                                                      Some(ref name) => name.clone(),
                                                                      None => format!("{}", literal),
                                                                  }))
                                .map_err(|_| PlanError::Unimplemented));
                        } else if let Ok(literal) = expression.evaluate(&mut Some(&mut default_env)) {
                            let col_type = literal.get_column_type();
                            try!(result.add_column(ColumnInfo::with_name(col_type,
                                                                  match *alias {
                                                                      Some(ref name) => name.clone(),
                                                                      None => format!("{}", expression),
                                                                  }))
                                .map_err(|_| PlanError::Unimplemented));
                        } else {
                            return Err(PlanError::Unimplemented);
                        }
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

                            for (_, info) in matches {
                                // TODO: Return a real error here
                                try!(result.add_column(info).map_err(|_| PlanError::Unimplemented));
                            }
                        }
                        None => {
                            // No table is specified, so this is all columns in the child schema.
                            try!(result.add_columns(self.input_schema.clone()).map_err(|_| PlanError::Unimplemented));
                        }
                    }
                }
            }

        }
        self.output_schema = Some(result);
        Ok(())
    }
}
