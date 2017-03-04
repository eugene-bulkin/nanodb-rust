//! This module provides the project plan node.

use super::super::{PlanResult, PlanError};
use super::PlanNode;
use ::expressions::Expression;
use ::{Schema, ColumnInfo};
use ::storage::{Tuple, TupleLiteral};

/// PlanNode representing the `SELECT` clause in a SQL query. This is the relational algebra Project
/// operator.
pub struct ProjectNode<'a> {
    child: Box<PlanNode + 'a>,
    values: Vec<(Expression, Option<String>)>,
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
    pub fn new(child: Box<PlanNode + 'a>, values: Vec<(Expression, Option<String>)>) -> ProjectNode<'a> {
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
        for &(ref select_value, _) in self.values.iter() {
            match *select_value {
                Expression::ColumnValue(ref column_name) => {
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
                },
                _ => {
                    return Err(PlanError::Unimplemented);
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
        println!("{:?}", next);
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
            Some(mut boxed_tuple) => {
                Some(&mut **boxed_tuple)
            },
            _ => { None }
        })
    }

    fn prepare(&mut self) -> PlanResult<()> {
        let mut result = Schema::new();
        for &(ref select_value, ref alias) in self.values.iter() {
            match *select_value {
                Expression::ColumnValue(ref column_name) => {
                    let matches = self.input_schema.find_columns(column_name);
                    if matches.is_empty() {
                        return Err(PlanError::ColumnDoesNotExist(column_name.clone()));
                    }
                    if matches.len() > 1 {
                        return Err(PlanError::Unimplemented);
                    }
                    let (ref idx, _) = matches[0];
                    try!(result.add_column(ColumnInfo::with_name(self.input_schema[*idx].column_type, match *alias {
                        Some(ref name) => name.clone(),
                        None => column_name.1.clone().unwrap(),
                    })).map_err(|_| PlanError::Unimplemented)); // TODO: Return a real error here
                },
                _ => {
                    return Err(PlanError::Unimplemented);
                }
            }
        }
        self.output_schema = Some(result);
        Ok(())
    }
}