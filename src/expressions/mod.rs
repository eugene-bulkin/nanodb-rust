//! A module containing classes for representing and evaluating arithmetic and logical expressions.

pub mod expression;
pub mod processor;
pub mod environment;
pub mod literal;
pub mod select_clause;
pub mod from_clause;
pub mod select_value;

pub use self::Error as ExpressionError;
pub use self::environment::Environment;
pub use self::expression::Expression;
pub use self::from_clause::{FromClause, FromClauseType, JoinConditionType, JoinType};
pub use self::literal::Literal;
pub use self::processor::Processor as ExpressionProcessor;
pub use self::select_clause::SelectClause;
pub use self::select_value::SelectValue;

use ::ColumnName;
use ::functions::FunctionError;
use ::queries::{PlanError};
use ::relations::ColumnType;
use ::storage::TupleError;

fn col_name_to_string(col_name: &ColumnName) -> String {
    match *col_name {
        (Some(ref table_name), Some(ref column_name)) => format!("{}.{}", table_name, column_name),
        (None, Some(ref column_name)) => column_name.clone(),
        (Some(ref table_name), None) => format!("{}.*", table_name),
        (None, None) => "*".into(),
    }
}

/// Describes a comparison operation
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum CompareType {
    /// Equality
    Equals,
    /// Inequality
    NotEquals,
    /// Less than
    LessThan,
    /// Less than or equal to
    LessThanEqual,
    /// Greater than or equal to
    GreaterThan,
    /// Greater than or equal to
    GreaterThanEqual,
}

impl<'a> From<&'a [u8]> for CompareType {
    fn from(bytes: &'a [u8]) -> Self {
        match bytes {
            b"!=" | b"<>" => CompareType::NotEquals,
            b"<" => CompareType::LessThan,
            b"<=" => CompareType::LessThanEqual,
            b">" => CompareType::GreaterThan,
            b">=" => CompareType::GreaterThanEqual,
            b"=" | b"==" | _ => CompareType::Equals,
        }
    }
}

impl ::std::fmt::Display for CompareType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            CompareType::NotEquals => write!(f, "!="),
            CompareType::LessThan => write!(f, "<"),
            CompareType::LessThanEqual => write!(f, "<="),
            CompareType::GreaterThan => write!(f, ">"),
            CompareType::GreaterThanEqual => write!(f, ">="),
            CompareType::Equals => write!(f, "="),
        }
    }
}

/// Describes an arithmetic operation.
#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ArithmeticType {
    /// Addition
    Plus,
    /// Subtraction
    Minus,
    /// Multiplication
    Multiply,
    /// Division
    Divide,
    /// Modulo
    Remainder,
    /// Minimum (used for functions)
    Min,
    /// Maximum (used for functions)
    Max,
}

impl<'a> From<&'a [u8]> for ArithmeticType {
    fn from(bytes: &'a [u8]) -> Self {
        match bytes {
            b"-" => ArithmeticType::Minus,
            b"*" => ArithmeticType::Multiply,
            b"/" => ArithmeticType::Divide,
            b"%" => ArithmeticType::Remainder,
            b"+" | _ => ArithmeticType::Plus,
        }
    }
}

impl ::std::fmt::Display for ArithmeticType {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            ArithmeticType::Minus => write!(f, "-"),
            ArithmeticType::Multiply => write!(f, "*"),
            ArithmeticType::Divide => write!(f, "/"),
            ArithmeticType::Remainder => write!(f, "%"),
            ArithmeticType::Plus => write!(f, "+"),
            ArithmeticType::Min => write!(f, "%min%"),
            ArithmeticType::Max => write!(f, "%max%"),
        }
    }
}

/// An error that occurs while working with expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// The column name given is ambiguous.
    AmbiguousColumnName(ColumnName),
    /// Unable to resolve the column name.
    CouldNotResolve(ColumnName),
    /// An expression requiring numeric values was provided with a non-numeric value.
    NotNumeric(Literal),
    /// An expression was expecting a boolean value and received a non-boolean value.
    NotBoolean(Literal),
    /// An expression requiring numeric expressions was provided with a non-numeric expression.
    NotNumericExpr(Expression, ColumnType),
    /// An expression was expecting a boolean expression and received a non-boolean expression.
    NotBooleanExpr(Expression, ColumnType),
    /// The expression provided needs more than one clause.
    EmptyExpression,
    /// The expression tried to read a column value and failed.
    CouldNotRead(TupleError),
    /// A function error occurred during evaluation.
    FunctionError(FunctionError),
    /// The function being called is not a scalar function.
    NotScalarFunction(String),
    /// Subqueries must be evaluated using a planner.
    SubqueryNeedsPlanner,
    /// Could not evaluate a subquery.
    CouldNotEvaluateSubquery(SelectClause, Box<PlanError>),
    /// The subquery given needed to be scalar, but it was not.
    SubqueryNotScalar(SelectClause),
    /// The subquery given was empty.
    SubqueryEmpty(SelectClause),
    /// Could not determine the type of the scalar subquery given.
    CannotDetermineSubqueryType(SelectClause),
    /// Aggregate calls cannot be nested.
    NestedAggregateCall {
        /// The parent call (i.e. the already traversed one).
        parent: Expression,
        /// The nested call.
        nested: Expression
    },
    /// The aggregate expected was not traversed.
    UnexpectedAggregate {
        /// The expression expected.
        expected: Expression,
        /// The expression given.
        received: Expression
    },
    /// This expression's evaluation has not been implemented yet.
    Unimplemented,
}

impl From<FunctionError> for Error {
    fn from(error: FunctionError) -> Self {
        Error::FunctionError(error)
    }
}

impl ::std::fmt::Display for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Error::AmbiguousColumnName(ref col_name) => {
                write!(f,
                       "The column {} is ambiguous.",
                       col_name_to_string(col_name))
            }
            Error::CouldNotResolve(ref col_name) => {
                write!(f,
                       "The column {} could not be resolved.",
                       col_name_to_string(col_name))
            }
            Error::NotNumeric(ref literal) => {
                write!(f,
                       "The expression was expected to evaluate to a numeric literal, got {}.",
                       literal)
            }
            Error::NotBoolean(ref literal) => {
                write!(f,
                       "The expression was expected to evaluate to a boolean literal, got {}.",
                       literal)
            }
            Error::NotNumericExpr(ref expr, ref t) => {
                write!(f, "The expression {} was expected to have a numeric type, but had type {}.",
                       expr, t)
            }
            Error::NotBooleanExpr(ref expr, ref t) => {
                write!(f, "The expression {} was expected to have a boolean type, but had type {}.",
                       expr, t)
            }
            Error::EmptyExpression => {
                write!(f,
                       "The expression was expecting a set of clauses and got none.")
            }
            Error::CouldNotRead(ref e) => write!(f, "Could not read a value from a tuple: {}", e),
            Error::FunctionError(ref e) => write!(f, "{}", e),
            Error::NotScalarFunction(ref name) => write!(f, "{} is not a scalar function.", name),
            Error::SubqueryNeedsPlanner => write!(f, "Subqueries require planners to evaluate."),
            Error::CouldNotEvaluateSubquery(ref clause, ref e) => {
                write!(f, "Subquery {} could not be evaluated: {}", clause, e)
            }
            Error::SubqueryNotScalar(ref clause) => write!(f, "The subquery {} is not scalar.", clause),
            Error::SubqueryEmpty(ref clause) => write!(f, "The subquery {} is empty.", clause),
            Error::CannotDetermineSubqueryType(ref clause) => write!(f, "Could not determine the return type of scalar subquery {}.", clause),
            Error::NestedAggregateCall { ref parent, ref nested } => {
                write!(f, "Found aggregate function call {} nested \
                within another aggregate call {}", nested, parent)
            }
            Error::UnexpectedAggregate { ref expected, ref received } => {
                write!(f, "Expected to find aggregate {} but found {} instead.", expected, received)
            }
            Error::Unimplemented => {
                write!(f,
                       "The expression's evaluation has not yet been implemented.")
            }
        }
    }
}
