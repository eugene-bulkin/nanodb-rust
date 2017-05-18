use std::borrow::Cow;
use std::collections::HashMap;

use ::expressions::{Environment, Expression, ExpressionError, Literal};
use ::functions::{AggregateFunction, Directory, Function};
use ::queries::{PlanError, PlanNode, PlanResult};
use ::relations::{ColumnInfo, Schema};
use ::storage::{Tuple, TupleLiteral};

lazy_static! {
    static ref DIRECTORY: Directory = Directory::new();
}

fn get_aggregate_function<I: Iterator<Item=Expression>>(func_name: &str, mut args: I, distinct: bool) -> Box<Function> {
    // No need to make another allocation if we don't need to update the function name.
    let mut func_name = Cow::from(func_name);

    if distinct {
        func_name += "#DISTINCT";
    }

    // Only COUNT can take * as an argument.
    let has_wildcard_arg = args.any(|arg| arg == Expression::ColumnValue((None, None)));
    if has_wildcard_arg && func_name == "COUNT" {
        func_name = "COUNT#STAR".into();
    }

    // Doesn't need to be mutable anymore.
    let func_name = func_name;

    // This shouldn't panic (this constructor should only be called after an actual
    // aggregate extraction, which will not allow unknown functions).
    DIRECTORY.get(func_name.as_ref()).unwrap()
}

#[derive(Debug)]
struct FunctionCall {
    expr: Expression,
    function: Box<AggregateFunction>,
    distinct: bool,
    args: Vec<Expression>,
}

impl Clone for FunctionCall {
    fn clone(&self) -> Self {
        if let Expression::Function { name: ref func_name, ref distinct, ref args } = self.expr {
            let func = get_aggregate_function(func_name, args.clone().into_iter(), *distinct);
            if func.is_aggregate() {
                FunctionCall {
                    expr: self.expr.clone(),
                    function: func.get_as_aggregate().unwrap(),
                    distinct: *distinct,
                    args: args.clone()
                }
            } else {
                // This shouldn't happen...
                unimplemented!()
            }
        } else {
            // This shouldn't happen...
            unimplemented!()
        }
    }
}

fn evaluate_group_by_exprs<'a, I: Iterator<Item=&'a Expression>>(group_by_exprs: I, mut env: &mut Environment) -> Result<TupleLiteral, ExpressionError> {
    let mut result = TupleLiteral::new();

    // Compute each group-by value and add it to the result tuple.
    for expr in group_by_exprs {
        result.add_value(try!(expr.evaluate(&mut Some(env), &None)));
    }

    Ok(result)
}

fn update_aggregates(aggregates: &mut HashMap<String, FunctionCall>, mut env: &mut Environment) -> Result<(), PlanError> {
    let names: Vec<String> = aggregates.keys().map(Clone::clone).collect();
    for name in names.iter() {
        if let Some(ref mut call) = aggregates.get_mut(name) {
            if call.args.len() != 1 {
                // TODO
                return Err(PlanError::Unimplemented);
            }
            if let Expression::Function { ref name, .. } = call.expr {
                // Special case for COUNT(*), since we don't actually care what the value is.
                let value = if *name == "COUNT" && call.args[0] == Expression::ColumnValue((None, None)) {
                    Literal::Null
                } else {
                    try!(call.args[0].evaluate(&mut Some(env), &None)
                        .map_err(PlanError::CouldNotProcessAggregates))
                };
                call.function.add_value(value.clone());
                debug!("Argument to aggregate function = {}, new aggregate result = {}", value, call.function.get_result());
            } else {
                unreachable!()
            }
        }
    }
    Ok(())
}

/// Implements grouping and aggregation by using hashing as a method to identify groups.
pub struct HashedGroupAggregateNode<'a> {
    child: Box<PlanNode + 'a>,
    input_schema: Schema,
    output_schema: Option<Schema>,
    group_by_exprs: Vec<Expression>,
    aggregates: HashMap<String, FunctionCall>,
    computed_aggregates: HashMap<TupleLiteral, HashMap<String, FunctionCall>>,
    groups: Option<Vec<TupleLiteral>>,
    group_idx: usize,
    current_tuple: Option<Box<Tuple>>,
    done: bool,
}

impl<'a> PlanNode for HashedGroupAggregateNode<'a> {
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
        let mut schema = Schema::new();

        for expr in self.group_by_exprs.iter() {
            if let Expression::ColumnValue(ref col_name) = *expr {
                let info = ColumnInfo {
                    column_type: try!(expr.get_column_type(&self.input_schema)
                        .map_err(PlanError::CouldNotProcessAggregates)),
                    table_name: col_name.0.clone(),
                    name: col_name.1.clone(),
                };
                try!(schema.add_column(info));
            } else {
                return Err(PlanError::SimpleColumnReferenceGroupBy(expr.clone()));
            }
        }

        for name in self.aggregates.keys() {
            let call = self.aggregates.get(name).unwrap();
            let col_type = try!(call.expr.get_column_type(&self.input_schema)
                .map_err(PlanError::CouldNotProcessAggregates));
            let info = ColumnInfo::with_name(col_type, name.clone());
            try!(schema.add_column(info));
        }
        info!("Grouping/aggregate node schema: {}", schema);
        self.output_schema = Some(schema);
        Ok(())
    }

    fn initialize(&mut self) {
        self.group_idx = 0;
    }
}

impl<'a> HashedGroupAggregateNode<'a> {
    /// Instantiate a new hashed-group aggregate node.
    ///
    /// # Argument
    /// * child - The child of the node.
    /// * group_by_exprs - The group by expressions.
    /// * aggregates - A list of aggregate function calls along with their projection name.
    pub fn new(child: Box<PlanNode + 'a>, group_by_exprs: Vec<Expression>, aggregates: Vec<(String, Expression)>) -> PlanResult<HashedGroupAggregateNode<'a>> {
        let mut map = HashMap::new();
        for &(ref name, ref expr) in aggregates.iter() {
            if let Expression::Function { name: ref func_name, ref distinct, ref args } = *expr {
                let has_wildcard_arg = args.iter().any(|arg| *arg == Expression::ColumnValue((None, None)));
                if has_wildcard_arg && &*func_name != "COUNT" {
                    // Theoretically, this usually won't be triggered since we usually try to
                    // resolve the type of the function expression first... but if a function always
                    // returns the same type, then this may happen if someone tried to use a wild
                    // card argument.
                    return Err(PlanError::WildCardInNonCountFunction(func_name.clone()));
                }

                let func = get_aggregate_function(func_name, args.clone().into_iter(), *distinct);
                if func.is_aggregate() {
                    map.insert(name.clone(), FunctionCall {
                        expr: expr.clone(),
                        function: func.get_as_aggregate().unwrap(),
                        distinct: *distinct,
                        args: args.clone()
                    });
                } else {
                    // This shouldn't happen...
                    unimplemented!()
                }
            } else {
                // This shouldn't happen...
                unimplemented!()
            }
        }

        let input_schema = child.get_schema();
        Ok(HashedGroupAggregateNode {
            child: child,
            input_schema: input_schema,
            output_schema: None,
            group_by_exprs: group_by_exprs,
            aggregates: map,
            computed_aggregates: HashMap::new(),
            groups: None,
            group_idx: 0,
            current_tuple: None,
            done: false
        })
    }

    fn generate_output_tuple(&self, mut group: &mut TupleLiteral, aggregates: &HashMap<String, FunctionCall>) -> TupleLiteral {
        // Construct the result tuple from the group, and from the
        // computed aggregate values.
        let mut result = TupleLiteral::new();
        if group.len() > 0 {
            result.append_tuple(group);
        }

        // TODO:  Add the aggregate values in an order that matches what
        //        the grouping/aggregate plan node must output.
        for name in aggregates.keys() {
            let call = aggregates.get(name).unwrap();
            result.add_value(call.function.get_result());
        }

        result
    }

    fn compute_aggregates(&mut self) -> PlanResult<()> {
        let mut result = HashMap::new();

        // Pull tuples from the left child until we run out.
        let mut cur_tuple = try!(self.child.get_next_tuple()).map(TupleLiteral::from_tuple);
        let aggregate_keys: Vec<String> = self.aggregates.keys().map(Clone::clone).collect();
        while let Some(tuple) = cur_tuple {
            let mut environment = Environment::new();
            environment.add_tuple(self.input_schema.clone(), tuple);

            // Get the group values for the current row.
            let group_values = try!(evaluate_group_by_exprs(self.group_by_exprs.iter(), &mut environment)
                .map_err(PlanError::CouldNotProcessAggregates));

            debug!("Group values = {}", group_values);

            // Look up the collection of aggregate functions for this group,
            // or create one if it doesn't already exist.
            let group_aggregates = result.entry(group_values).or_insert_with(|| {
                let mut result = HashMap::new();
                debug!(" * Creating new computed aggregates for this group");

                // Clone each aggregate function, since aggregates keep some
                // internal scratch space for computation.
                for name in aggregate_keys.iter() {
                    result.insert(name.clone(), self.aggregates.get(name).unwrap().clone());
                }

                result
            });

            // Now that we know the group, and we have aggregate functions to
            // do the computation, update each aggregate with the tuple's
            // current value.
            try!(update_aggregates(group_aggregates, &mut environment));
            cur_tuple = try!(self.child.get_next_tuple()).map(TupleLiteral::from_tuple);
        }
        self.computed_aggregates = result;
        Ok(())
    }

    fn get_next_tuple_helper(&mut self) -> PlanResult<()> {
        if self.done {
            return Ok(());
        }

        if self.computed_aggregates.is_empty() {
            try!(self.compute_aggregates());
            self.groups = Some(self.computed_aggregates.keys().map(Clone::clone).collect());
        }

        if let Some(ref groups) = self.groups {
            if self.group_idx < groups.len() {
                let mut group = groups[self.group_idx].clone();
                let group_aggregates = self.computed_aggregates.get(&group).unwrap();

                // Construct the result tuple from the group, and from the
                // computed aggregate values.
                let result = self.generate_output_tuple(&mut group, group_aggregates);
                self.current_tuple = Some(Box::new(result));
            } else {
                self.done = true;
                self.current_tuple = None;
            }
            self.group_idx += 1;
        }
        Ok(())
    }
}