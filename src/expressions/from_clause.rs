//! This module contains `FROM` clause information.


use ::{ColumnInfo, Schema};
use ::commands::{ExecutionError, InvalidSchemaError};
use ::expressions::{CompareType, Expression, SelectValue};
use std::collections::HashSet;
use std::default::Default;
use ::storage::{FileManager, TableManager};

/// For FROM clauses that contain join expressions, this enumeration specifies the kind of
/// join-condition for each join expression.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinConditionType {
    /// Perform a natural join, which implicitly specifies that values in all shared columns must be
    /// equal.
    NaturalJoin,
    /// The join clause specifies an ON clause with an expression that must evaluate to true.
    OnExpr(Expression),
    /// The join clause specifies a USING clause, which explicitly lists the shared columns whose
    /// values must be equal.
    Using(Vec<String>),
}

impl Default for JoinConditionType {
    fn default() -> Self {
        JoinConditionType::OnExpr(Expression::True)
    }
}

impl ::std::fmt::Display for JoinConditionType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            JoinConditionType::NaturalJoin => write!(f, "NaturalJoin"),
            JoinConditionType::OnExpr(_) => write!(f, "JoinOnExpression"),
            JoinConditionType::Using(_) => write!(f, "JoinUsing"),
        }
    }
}

/// An enumeration specifying the different types of join operation.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum JoinType {
    /// Inner joins, where only matching rows are included in the result.
    Inner,
    /// Left outer joins, where non-matching rows from the left table are included in the results.
    LeftOuter,
    /// Right outer joins, where non-matching rows from the right table are included in the results.
    RightOuter,
    /// Full outer joins, where non-matching rows from either the left or right table are included
    /// in the results.
    FullOuter,
    /// Cross joins, which are simply a Cartesian product.
    Cross,
    /// Semijoin, where the left table's rows are included when they match one or more rows from the
    /// right table.
    Semijoin,
    /// Antijoin (aka anti-semijoin), where the left table's rows are included when they match none
    /// of the rows from the right table.
    Antijoin,
}

impl ::std::fmt::Display for JoinType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            JoinType::Inner => write!(f, "Inner"),
            JoinType::LeftOuter => write!(f, "Left Outer"),
            JoinType::RightOuter => write!(f, "Right Outer"),
            JoinType::FullOuter => write!(f, "Full Outer"),
            JoinType::Cross => write!(f, "Cross"),
            JoinType::Semijoin => write!(f, "Semijoin"),
            JoinType::Antijoin => write!(f, "Antijoin"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
/// This enum represents a hierarchy of one or more base and derived relations that produce the rows
/// considered by `SELECT` clauses.
pub struct FromClause {
    /// The actual FROM clause data.
    pub clause_type: FromClauseType,
    computed_join_expr: Option<Expression>,
    computed_schema: Option<Schema>,
    computed_select_values: Option<Vec<SelectValue>>,
}

impl ::std::ops::Deref for FromClause {
    type Target = FromClauseType;
    fn deref(&self) -> &Self::Target {
        &self.clause_type
    }
}

fn build_join_schema(left: Schema,
                     right: Schema,
                     common: HashSet<String>,
                     result: &mut Schema,
                     join_type: JoinType)
                     -> Result<(Option<Expression>, Option<Vec<SelectValue>>), ExecutionError> {
    let mut join_expr = None;
    let mut select_values = Vec::new();

    if !common.is_empty() {
        // We will need to generate a join expression using the common
        // columns.  We will also need a project-spec that will project down
        // to only one copy of the common columns.

        let mut and_clauses: Vec<Expression> = Vec::new();

        // Handle the shared columns.  We need to check that the
        // names aren't ambiguous on one or the other side.
        for name in common.iter() {
            let left_count = left.num_columns_with_name(name.as_ref());
            let right_count = right.num_columns_with_name(name.as_ref());

            if left_count != 1 || right_count != 1 {
                // TODO: Make this an invalid schema error
                return Err(ExecutionError::Unimplemented);
            }

            let left_info = left.get_column(name.as_ref()).unwrap();
            let right_info = right.get_column(name.as_ref()).unwrap();

            try!(result.add_column(ColumnInfo::with_name(left_info.column_type, name.as_ref())).map_err(ExecutionError::CouldNotCreateSchema));

            let compare_expr = Expression::Compare(Box::new(Expression::ColumnValue(left_info.get_column_name())),
                                                   CompareType::Equals,
                                                   Box::new(Expression::ColumnValue(right_info.get_column_name())));

            and_clauses.push(compare_expr);

            // Add a select-value that projects the appropriate source column down to the common
            // column.
            match join_type {
                JoinType::Inner | JoinType::LeftOuter => {
                    // We can use the left column in the result, as it will always be non-NULL.
                    select_values.push(SelectValue::Expression {
                        expression: Expression::ColumnValue(left_info.get_column_name()),
                        alias: Some(name.clone()),
                    });
                }
                JoinType::RightOuter => {
                    // We can use the right column in the result, as it will always be non-NULL.
                    select_values.push(SelectValue::Expression {
                        expression: Expression::ColumnValue(right_info.get_column_name()),
                        alias: Some(name.clone()),
                    });
                }
                JoinType::FullOuter => {
                    // TODO: Need function calls here to use COALESCE.
                    return Err(ExecutionError::Unimplemented);
                }
                _ => {
                    // Do nothing...?
                }
            }
        }

        join_expr = Some(Expression::AND(and_clauses));
    }

    // Handle the non-shared columns
    for col_info in left.iter().chain(right.iter()) {
        let col_name = col_info.get_column_name();
        match col_name {
            (_, Some(ref name)) => {
                if !common.contains(name) {
                    try!(result.add_column(col_info.clone()).map_err(ExecutionError::CouldNotCreateSchema));

                    select_values.push(SelectValue::Expression {
                        expression: Expression::ColumnValue(col_name.clone()),
                        alias: None,
                    });
                }
            }
            _ => {}
        }
    }

    Ok((join_expr,
        if select_values.is_empty() {
        None
    } else {
        Some(select_values)
    }))
}

#[derive(Clone, Debug, PartialEq)]
/// This enum contains information about what kind of FROM clause the clause is.
pub enum FromClauseType {
    /// A `FROM` clause that just selects a base table and possibly an alias.
    BaseTable {
        /// The name of the table being selected from.
        table: String,
        /// An optional alias to rename the table with.
        alias: Option<String>,
    },
    /// A `FROM` clause that is a join expression (may be nested).
    JoinExpression {
        /// The left child of the join.
        left: Box<FromClause>,
        /// The right child of the join.
        right: Box<FromClause>,
        /// The join type.
        join_type: JoinType,
        /// The join condition type.
        condition_type: JoinConditionType,
    },
}

impl ::std::fmt::Display for FromClauseType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            FromClauseType::BaseTable { .. } => write!(f, "BaseTable"),
            FromClauseType::JoinExpression { .. } => write!(f, "JoinExpression"),
        }
    }
}


impl FromClause {
    /// Instantiate a FROM clause that is a base table.
    pub fn base_table(table: String, alias: Option<String>) -> FromClause {
        FromClause {
            clause_type: FromClauseType::BaseTable {
                table: table,
                alias: alias,
            },
            computed_schema: None,
            computed_join_expr: None,
            computed_select_values: None,
        }
    }

    /// Instantiate a FROM clause that is a join expression.
    pub fn join_expression(left: Box<FromClause>,
                           right: Box<FromClause>,
                           join_type: JoinType,
                           condition_type: JoinConditionType)
                           -> FromClause {
        FromClause {
            clause_type: FromClauseType::JoinExpression {
                left: left,
                right: right,
                join_type: join_type,
                condition_type: condition_type,
            },
            computed_schema: None,
            computed_join_expr: None,
            computed_select_values: None,
        }
    }

    /// Retrieve the computed join expression.
    pub fn get_computed_join_expr(&self) -> Option<Expression> {
        self.computed_join_expr.clone()
    }

    /// Retrieve the computed select values.
    pub fn get_computed_select_values(&self) -> Option<Vec<SelectValue>> {
        self.computed_select_values.clone()
    }

    /// Calculate the schema and computed join expression for the FROM clause.
    pub fn compute_schema(&mut self,
                          file_manager: &FileManager,
                          table_manager: &TableManager)
                          -> Result<Schema, ExecutionError> {
        let result = match self.clause_type {
            FromClauseType::BaseTable { ref table, ref alias } => {
                debug!("Preparing BASE_TABLE from-clause.");

                let table = try!(table_manager.get_table(file_manager, table.clone()).map_err(ExecutionError::CouldNotComputeSchema));
                let schema = table.get_schema();

                if let Some(name) = alias.clone() {
                    // TODO
                }

                self.computed_schema = Some(schema.clone());
                schema.clone()
            }
            FromClauseType::JoinExpression { ref mut left, ref mut right, ref condition_type, ref join_type } => {
                debug!("Preparing JOIN_EXPR from-clause.  Condition type = {:?}", condition_type);

                let mut schema = Schema::new();

                let left_schema = try!(left.compute_schema(file_manager, table_manager));
                let right_schema = try!(right.compute_schema(file_manager, table_manager));

                match *condition_type {
                    JoinConditionType::NaturalJoin => {
                        if left_schema.has_multiple_columns_with_same_name() {
                            return Err(ExecutionError::InvalidSchema(InvalidSchemaError::LeftSchemaDuplicates));
                        }
                        if right_schema.has_multiple_columns_with_same_name() {
                            return Err(ExecutionError::InvalidSchema(InvalidSchemaError::RightSchemaDuplicates));
                        }
                        let common_cols = left_schema.get_common_column_names(&right_schema);
                        if common_cols.is_empty() {
                            return Err(ExecutionError::InvalidSchema(InvalidSchemaError::NoShared));
                        }

                        println!("{}", schema);
                        let built =
                            try!(build_join_schema(left_schema, right_schema, common_cols, &mut schema, *join_type));
                        println!("{}", schema);
                        self.computed_join_expr = built.0;
                        self.computed_select_values = built.1;
                    }
                    JoinConditionType::Using(ref names) => {
                        return Err(ExecutionError::Unimplemented);
                    }
                    JoinConditionType::OnExpr(ref expr) => {
                        try!(schema.add_columns(left_schema));
                        try!(schema.add_columns(right_schema));

                        self.computed_join_expr = Some(expr.clone());
                    }
                }

                self.computed_schema = Some(schema.clone());
                schema.clone()
            }
        };
        Ok(result)
    }
}

impl ::std::fmt::Display for FromClause {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        try!(write!(f, "JoinClause[type={}", self.clause_type));
        match self.clause_type {
            FromClauseType::BaseTable { ref table, ref alias } => {
                try!(write!(f, ", table={}", table));
                if let Some(ref name) = *alias {
                    try!(write!(f, " AS {}", name));
                }
            }
            FromClauseType::JoinExpression { ref left, ref right, ref join_type, ref condition_type } => {
                try!(write!(f, ", join_type={}", join_type));
                try!(write!(f, ", cond_type={}", condition_type));

                match *condition_type {
                    JoinConditionType::NaturalJoin => {
                        try!(write!(f, ", computed_join_expr={}", self.computed_join_expr.clone().unwrap()));
                    }
                    JoinConditionType::Using(ref names) => {
                        try!(write!(f, ", using_names={:?}", names));
                        try!(write!(f, ", computed_join_expr={}", self.computed_join_expr.clone().unwrap()));
                    }
                    JoinConditionType::OnExpr(ref expr) => {
                        if *expr != Expression::True {
                            try!(write!(f, ", on_expr={}", expr));

                        }
                    }
                }
                try!(write!(f, ", left_child={}", left));
                try!(write!(f, ", right_child={}", right));
            }
        }
        write!(f, "]")
    }
}
