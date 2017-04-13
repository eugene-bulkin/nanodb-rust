//! This module contains the classes and functions needed for a simple query planner.

use ::expressions::{FromClause, FromClauseType, SelectClause, SelectValue};
use ::queries::{AggregateFunctionExtractor, HashedGroupAggregateNode, NestedLoopJoinNode,
                NodeResult, PlanError, PlanNode, Planner, PlanResult, ProjectNode,
                make_simple_select, RenameNode};
use ::storage::{FileManager, TableManager};

fn prepare_aggregates(mut clause: &mut SelectClause) -> PlanResult<AggregateFunctionExtractor> {
    // Analyze all expressions in the SELECT, WHERE and HAVING clauses for aggregate function calls.
    // (Obviously, if the WHERE clause contains aggregates then it's an error!)
    let mut extractor = AggregateFunctionExtractor::new();

    if let Some(ref where_expr) = clause.where_expr {
        try!(where_expr.clone().traverse(&mut extractor).map_err(PlanError::CouldNotProcessAggregates));

        if extractor.found_aggregates() {
            let aggregates = {
                let mut result = Vec::new();
                let calls = extractor.get_aggregate_calls();
                for (_, ref expr) in calls {
                    result.push(expr.clone());
                }
                result
            };
            return Err(PlanError::AggregatesInWhereExpr(aggregates));
        }
    }

    // Make sure no conditions in the FROM clause contain aggregates...
    // TODO

    // Now it's OK to find aggregates, so scan SELECT and HAVING clauses.
    for value in clause.values.iter_mut() {
        match *value {
            SelectValue::Expression { ref mut expression, .. } => {
                *expression = try!(expression.traverse(&mut extractor).map_err(PlanError::CouldNotProcessAggregates));
            }
            SelectValue::WildcardColumn { .. } => {}
        }
    }

    if let Some(ref mut having) = clause.having {
        try!(having.traverse(&mut extractor).map_err(PlanError::CouldNotProcessAggregates));
    }

    if extractor.found_aggregates() {
        // Print out some useful details about what happened during the aggregate-function
        // extraction.

        let aggregates = extractor.get_aggregate_calls();

        info!("Found {} aggregate functions.", aggregates.len());

        for &(ref name, ref expr) in aggregates.iter() {
            info!(" * {} = {}", name, expr);
        }

        info!("Transformed select-values:");
        for sv in clause.values.iter() {
            info!(" * {}", sv);
        }

        if let Some(ref having) = clause.having {
            info!("Transformed HAVING clause: {}", having);
        }
    }

    Ok(extractor)
}

/// This class generates execution plannodes for performing SQL queries. The primary responsibility
/// is to generate plannodes for SQL `SELECT` statements, but `UPDATE` and `DELETE` expressions will
/// also use this class to generate simple plannodes to identify the tuples to update or delete.
pub struct SimplePlanner<'a> {
    file_manager: &'a FileManager,
    table_manager: &'a TableManager,
}

impl<'a> SimplePlanner<'a> {
    /// Instantiates a new SimplePlanner.
    pub fn new(file_manager: &'a FileManager, table_manager: &'a TableManager) -> SimplePlanner<'a> {
        SimplePlanner {
            file_manager: file_manager,
            table_manager: table_manager,
        }
    }

    fn make_join_tree(&self, clause: FromClause) -> NodeResult {
        match *clause {
            FromClauseType::BaseTable { ref table, ref alias } => {
                let mut cur_node = try!(make_simple_select(self.file_manager, self.table_manager, table.clone(), None));
                if let Some(ref name) = *alias {
                    cur_node = Box::new(RenameNode::new(cur_node, name.as_ref()));
                }
                Ok(cur_node)
            }
            FromClauseType::JoinExpression { ref left, ref right, ref join_type, .. } => {
                let left_child = try!(self.make_join_tree(*left.clone()));
                let right_child = try!(self.make_join_tree(*right.clone()));

                let mut cur_node: Box<PlanNode> = Box::new(NestedLoopJoinNode::new(left_child,
                                                                                   right_child,
                                                                                   join_type.clone(),
                                                                                   clause.get_computed_join_expr()));
                try!(cur_node.prepare());

                if let Some(values) = clause.get_computed_select_values() {
                    cur_node = Box::new(ProjectNode::new(cur_node, values, self));
                    try!(cur_node.prepare());
                }

                Ok(cur_node)
            }
        }
    }
}

impl<'a> Planner for SimplePlanner<'a> {
    fn make_plan(&self, mut clause: SelectClause) -> NodeResult {
        let node = match clause.from_clause.clone() {
            Some(ref from_clause) => {
                let mut cur_node = try!(self.make_join_tree(from_clause.clone()));
                try!(cur_node.prepare());

                // Look for aggregate function calls, and transform expressions that include them so
                // that we can compute them all in one grouping / aggregate plan node.
                let extractor = try!(prepare_aggregates(&mut clause));

                if cur_node.has_predicate() {
                    if let Some(ref expr) = clause.where_expr {
                        try!(cur_node.as_mut().set_predicate(expr.clone()));
                    }
                }

                // Handle grouping and aggregation next, if there are any aggregate operations.
                let has_group_by_exprs = if let Some(ref exprs) = clause.group_by_exprs {
                    !exprs.is_empty()
                } else {
                    false
                };
                if extractor.found_aggregates() || has_group_by_exprs {
                    // Get the aggregates too (if present).
                    let aggregates = extractor.get_aggregate_calls();

                    // By default, use a hash-based grouping/aggregate node. Later we can replace
                    // with a sort-based grouping/aggregate node if it would be more efficient.
                    let node = HashedGroupAggregateNode::new(cur_node,
                                                             clause.group_by_exprs
                                                                 .clone()
                                                                 .unwrap_or(vec![]),
                                                             aggregates);
                    cur_node = Box::new(node);
                    try!(cur_node.prepare());
                }

                if !clause.is_trivial_project() {
                    cur_node = Box::new(ProjectNode::new(cur_node, clause.values, self));
                    try!(cur_node.prepare());
                }

                cur_node
            }
            None => {
                let mut cur_node = Box::new(try!(ProjectNode::scalar(clause.values, self)));
                try!(cur_node.prepare());
                cur_node
            }
        };
        Ok(node)
    }
}
