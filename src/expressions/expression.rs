//! This module contains utilities for dealing with expressions, including the `Expression` struct.

use ::expressions::{ArithmeticType, CompareType, Environment, ExpressionError, Literal,
                    ExpressionProcessor, SelectClause};
use ::functions::Directory;
use ::queries::{Planner, get_plan_results};
use ::relations::{EMPTY_NUMERIC, ColumnInfo, ColumnName, ColumnType, Schema, column_name_to_string};

lazy_static! {
    static ref DIRECTORY: Directory = Directory::new();
}

static TYPE_ORDER: &'static [ColumnType] = &[EMPTY_NUMERIC, ColumnType::Double, ColumnType::Float,
    ColumnType::BigInt, ColumnType::Integer, ColumnType::SmallInt, ColumnType::TinyInt];

fn arithmetic_result_type(op: ArithmeticType, left: ColumnType, right: ColumnType) -> ColumnType {
    // This shouldn't be called with non-arithmetic types.
    assert!(left.is_numeric());
    assert!(right.is_numeric());

    // We don't care what the precision/scale are.
    let left = match left {
        ColumnType::Numeric { .. } => EMPTY_NUMERIC,
        _ => left
    };
    let right = match right {
        ColumnType::Numeric { .. } => EMPTY_NUMERIC,
        _ => right
    };

    match op {
        ArithmeticType::Divide => ColumnType::Double,
        _ => {
            for t in TYPE_ORDER {
                if &left == t || &right == t {
                    return *t;
                }
            }

            // Just guess INTEGER.  Works for C... (from original nanodb...)
            ColumnType::Integer
        }
    }
}

fn coerce_literals(left: Literal, right: Literal) -> (Literal, Literal) {
    // WE ASSUME THAT BOTH LITERALS ARE ARITHMETIC HERE.
    if left.is_double() || right.is_double() {
        // If either is a double, coerce both to doubles.
        (left.as_double().unwrap(), right.as_double().unwrap())
    } else if left.is_float() || right.is_float() {
        // If either is a float, coerce both to floats.
        (left.as_float().unwrap(), right.as_float().unwrap())
    } else if left.is_long() || right.is_long() {
        // If either is a long, coerce both to longs.
        (left.as_long().unwrap(), right.as_long().unwrap())
    } else {
        (left.as_int().unwrap(), right.as_int().unwrap())
    }
}

/// A SQL-supported expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// A function call
    Function {
        /// The string name of the function as specified in the original SQL.
        name: String,
        /// A flag indicating whether the `DISTINCT` keyword was used in the function invocation,
        /// e.g. `COUNT(DISTINCT n)`. This flag is only used in the context of aggregate functions;
        /// if it is set for other kinds of functions, it is a semantic error.
        distinct: bool,
        /// The list of one or more arguments for the function call.
        args: Vec<Expression>
    },
    /// A Boolean OR expression
    OR(Vec<Expression>),
    /// A Boolean AND expression
    AND(Vec<Expression>),
    /// A Boolean NOT expression
    NOT(Box<Expression>),
    /// A comparison expression
    Compare(Box<Expression>, CompareType, Box<Expression>),
    /// An IS NULL operator
    IsNull(Box<Expression>),
    /// An arithmetic expression
    Arithmetic(Box<Expression>, ArithmeticType, Box<Expression>),
    /// A column value for later dynamic evaluation.
    ColumnValue(ColumnName),
    /// A subquery expression
    Subquery(Box<SelectClause>),
    /// NULL
    Null,
    /// TRUE
    True,
    /// FALSE
    False,
    /// An integer
    Int(i32),
    /// A long
    Long(i64),
    /// A float
    Float(f32),
    /// A double
    Double(f64),
    /// A string
    String(String),
}

impl From<Literal> for Expression {
    fn from(literal: Literal) -> Self {
        match literal {
            Literal::Int(i) => Expression::Int(i),
            Literal::Long(l) => Expression::Long(l),
            Literal::Float(f) => Expression::Float(f),
            Literal::Double(d) => Expression::Double(d),
            Literal::String(s) => Expression::String(s),
            Literal::Null => Expression::Null,
            Literal::True => Expression::True,
            Literal::False => Expression::False,
            // There really isn't anything else to convert this to... a file pointer should never
            // come up in a spot where it would be converted into an expression.
            Literal::FilePointer { .. } => Expression::Null,
        }
    }
}

impl From<ColumnName> for Expression {
    fn from(name: ColumnName) -> Self {
        Expression::ColumnValue(name)
    }
}

impl From<String> for Expression {
    fn from(s: String) -> Self {
        Expression::String(s)
    }
}

impl<'a> From<&'a str> for Expression {
    fn from(s: &str) -> Self {
        Expression::String(s.into())
    }
}

impl Expression {
    fn try_literal(&self) -> Option<Literal> {
        match *self {
            Expression::Int(i) => Literal::Int(i).into(),
            Expression::Long(l) => Literal::Long(l).into(),
            Expression::Float(f) => Literal::Float(f).into(),
            Expression::Double(d) => Literal::Double(d).into(),
            Expression::String(ref s) => Literal::String(s.clone()).into(),
            Expression::Null => Literal::Null.into(),
            Expression::True => Literal::True.into(),
            Expression::False => Literal::False.into(),
            _ => None,
        }
    }

    /// Evaluates this expression object in the context of the specified environment. The
    /// environment provides any external information necessary to evaluate the expression, such as
    /// the current tuples loaded from tables referenced within the expression.
    ///
    /// # Arguments
    /// * env - the environment to look up symbol-values from, when evaluating the expression
    /// * planner - optionally, a planner to use. This is required if subqueries are involved that
    ///             actually need to be resolved.
    ///
    /// # Errors
    /// This will return some `ExpressionError` if the expression cannot be evaluated given the
    /// environment.
    pub fn evaluate(&self, mut env: &mut Option<&mut Environment>, planner: &Option<&Planner>) -> Result<Literal, ExpressionError> {
        if let Some(l) = self.try_literal() {
            return Ok(l);
        }
        match *self {
            Expression::Arithmetic(ref left, op, ref right) => {
                self.evaluate_arithmetic(&mut env, left.clone(), right.clone(), op, planner)
            }
            Expression::Compare(ref left, op, ref right) => self.evaluate_compare(env, left.clone(), right.clone(), op, planner),
            Expression::OR(ref exprs) => {
                if exprs.is_empty() {
                    return Err(ExpressionError::EmptyExpression);
                }
                for expr in exprs {
                    let value = try!(expr.evaluate(env, planner)).clone();
                    match value {
                        Literal::True => {
                            // Can short-circuit here.
                            return Ok(Literal::True);
                        }
                        Literal::False => {
                            // Do nothing because we have to check the others.
                        }
                        _ => {
                            return Err(ExpressionError::NotBoolean(value));
                        }
                    }
                }
                Ok(Literal::False)
            }
            Expression::AND(ref exprs) => {
                if exprs.is_empty() {
                    return Err(ExpressionError::EmptyExpression);
                }
                for expr in exprs {
                    let value = try!(expr.evaluate(env, planner));
                    match value {
                        Literal::True => {
                            // Do nothing because we have to check the others.
                        }
                        Literal::False => {
                            // Can short-circuit here.
                            return Ok(Literal::False);
                        }
                        _ => {
                            return Err(ExpressionError::NotBoolean(value));
                        }
                    }
                }
                Ok(Literal::True)
            }
            Expression::NOT(ref inner) => {
                let value = try!(inner.evaluate(env, planner));
                match value {
                    Literal::False => Ok(Literal::True),
                    Literal::True => Ok(Literal::False),
                    _ => Err(ExpressionError::NotBoolean(value)),
                }
            }
            Expression::IsNull(ref inner) => {
                let value = try!(inner.evaluate(env, planner));
                Ok(if value == Literal::Null {
                    Literal::True
                } else {
                    Literal::False
                })
            }
            Expression::ColumnValue(ref name) => {
                if let Some(ref mut inner) = *env {
                    inner.get_column_value(&name)
                } else {
                    Err(ExpressionError::CouldNotResolve(name.clone()))
                }
            }
            Expression::Function { ref name, ref args, .. } => {
                let func = try!(DIRECTORY.get(name.as_ref()));
                func.evaluate(&mut env, args.to_vec(), planner).map_err(Into::into)
            }
            Expression::Subquery(ref clause) => {
                match *planner {
                    Some(ref planner) => {
                        let mut plan = try!(planner.make_plan(*clause.clone())
                            .map_err(|e| ExpressionError::CouldNotEvaluateSubquery(*clause.clone(), Box::new(e))));
                        let results = try!(get_plan_results(&mut *plan)
                            .map_err(|e| ExpressionError::CouldNotEvaluateSubquery(*clause.clone(), Box::new(e))));
                        if results.is_empty() {
                            Err(ExpressionError::SubqueryEmpty(*clause.clone()))
                        } else if results.len() > 1 || results[0].len() > 1 {
                            Err(ExpressionError::SubqueryNotScalar(*clause.clone()))
                        } else {
                            Ok(results[0][0].clone().into())
                        }
                    }
                    None => Err(ExpressionError::SubqueryNeedsPlanner)
                }
            }
            _ => Err(ExpressionError::Unimplemented),
        }
    }

    fn evaluate_arithmetic(&self,
                           mut env: &mut Option<&mut Environment>,
                           left: Box<Expression>,
                           right: Box<Expression>,
                           op: ArithmeticType,
                           planner: &Option<&Planner>)
                           -> Result<Literal, ExpressionError> {
        let left_val = try!(left.evaluate(&mut env, planner));
        let right_val = try!(right.evaluate(&mut env, planner));
        if !left_val.is_numeric() {
            return Err(ExpressionError::NotNumeric(left_val.clone()));
        }
        if !right_val.is_numeric() {
            return Err(ExpressionError::NotNumeric(right_val.clone()));
        }
        let (left_val, right_val) = coerce_literals(left_val, right_val);
        match op {
            ArithmeticType::Plus => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok(Literal::Int(l + r)),
                    (Literal::Double(l), Literal::Double(r)) => Ok(Literal::Double(l + r)),
                    (Literal::Float(l), Literal::Float(r)) => Ok(Literal::Float(l + r)),
                    (Literal::Long(l), Literal::Long(r)) => Ok(Literal::Long(l + r)),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            ArithmeticType::Minus => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok(Literal::Int(l - r)),
                    (Literal::Double(l), Literal::Double(r)) => Ok(Literal::Double(l - r)),
                    (Literal::Float(l), Literal::Float(r)) => Ok(Literal::Float(l - r)),
                    (Literal::Long(l), Literal::Long(r)) => Ok(Literal::Long(l - r)),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            ArithmeticType::Multiply => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok(Literal::Int(l * r)),
                    (Literal::Double(l), Literal::Double(r)) => Ok(Literal::Double(l * r)),
                    (Literal::Float(l), Literal::Float(r)) => Ok(Literal::Float(l * r)),
                    (Literal::Long(l), Literal::Long(r)) => Ok(Literal::Long(l * r)),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            ArithmeticType::Divide => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok(Literal::Int(l / r)),
                    (Literal::Double(l), Literal::Double(r)) => Ok(Literal::Double(l / r)),
                    (Literal::Float(l), Literal::Float(r)) => Ok(Literal::Float(l / r)),
                    (Literal::Long(l), Literal::Long(r)) => Ok(Literal::Long(l / r)),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            ArithmeticType::Remainder => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok(Literal::Int(l % r)),
                    (Literal::Double(l), Literal::Double(r)) => Ok(Literal::Double(l % r)),
                    (Literal::Float(l), Literal::Float(r)) => Ok(Literal::Float(l % r)),
                    (Literal::Long(l), Literal::Long(r)) => Ok(Literal::Long(l % r)),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
        }
    }

    fn evaluate_compare(&self,
                        mut env: &mut Option<&mut Environment>,
                        left: Box<Expression>,
                        right: Box<Expression>,
                        op: CompareType,
                        planner: &Option<&Planner>)
                        -> Result<Literal, ExpressionError> {
        let left_val = try!(left.evaluate(&mut env, planner));
        let right_val = try!(right.evaluate(&mut env, planner));
        if !left_val.is_numeric() {
            return Err(ExpressionError::NotNumeric(left_val.clone()));
        }
        if !right_val.is_numeric() {
            return Err(ExpressionError::NotNumeric(right_val.clone()));
        }
        let (left_val, right_val) = coerce_literals(left_val, right_val);
        match op {
            CompareType::GreaterThan => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l > r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l > r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l > r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l > r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            CompareType::GreaterThanEqual => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l >= r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l >= r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l >= r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l >= r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            CompareType::LessThan => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l < r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l < r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l < r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l < r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            CompareType::LessThanEqual => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l <= r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l <= r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l <= r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l <= r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            CompareType::Equals => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l == r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l == r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l == r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l == r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
            CompareType::NotEquals => {
                match (left_val, right_val) {
                    (Literal::Int(l), Literal::Int(r)) => Ok((l != r).into()),
                    (Literal::Double(l), Literal::Double(r)) => Ok((l != r).into()),
                    (Literal::Float(l), Literal::Float(r)) => Ok((l != r).into()),
                    (Literal::Long(l), Literal::Long(r)) => Ok((l != r).into()),
                    _ => Err(ExpressionError::Unimplemented),
                }
            }
        }
    }

    /// This method allows the entire expression tree to be traversed node by node, either for
    /// analysis or for transformation. The [`ExpressionProcessor`] instance receives notifications
    /// as each node in the expression is entered and left.
    ///
    /// The expression tree can also be manipulated by this traversal process, depending on what the
    /// [`ExpressionProcessor`] wants to do. If the expression node that `traverse()` is invoked on,
    /// needs to be replaced with a new expression node, the replacement is returned by the
    /// `traverse` method. (The [`ExpressionProcessor`] specifies the replacement as the
    /// return-value from the [`ExpressionProcessor.leave`] method.)
    ///
    /// [`ExpressionProcessor`]: ../processor/trait.Processor.html
    /// [`ExpressionProcessor.leave`]: ../processor/trait.Processor.html#tymethod.leave
    pub fn traverse(&mut self, processor: &mut ExpressionProcessor) -> Result<Expression, ExpressionError> {
        try!(processor.enter(self));
        match *self {
            Expression::Arithmetic(ref mut left, _, ref mut right) => {
                *left = Box::new(try!(left.traverse(processor)));
                *right = Box::new(try!(right.traverse(processor)));
            }
            Expression::Compare(ref mut left, _, ref mut right) => {
                *left = Box::new(try!(left.traverse(processor)));
                *right = Box::new(try!(right.traverse(processor)));
            }
            Expression::OR(ref mut exprs) | Expression::AND(ref mut exprs) => {
                for i in 0..exprs.len() {
                    let e = try!(exprs[i].traverse(processor));
                    exprs[i] = e;
                }
            }
            Expression::NOT(ref mut inner) | Expression::IsNull(ref mut inner) => {
                *inner = Box::new(try!(inner.traverse(processor)));
            }
            Expression::ColumnValue(_) => {
                // This is a leaf, don't traverse the inner node.
            }
            Expression::Function { ref mut args, .. } => {
                for i in 0..args.len() {
                    let e = try!(args[i].traverse(processor));
                    args[i] = e;
                }
            }
            Expression::Subquery(_) => {
                // We do not traverse the subquery; it is treated as a "black box" by the
                // expression-traversal mechanism.
            }
            Expression::Null | Expression::True | Expression::False | Expression::Int(_)
            | Expression::Long(_) | Expression::Float(_) | Expression::Double(_)
            | Expression::String(_) => {
                // These are literals so there's nothing else to do.
            }
        }
        processor.leave(self)
    }

    /// Returns a [`ColumnInfo`] object describing the type (and possibly the name) of the
    /// expression's result.
    ///
    /// [`ColumnInfo`]: ../relations/relations/struct.ColumnInfo.html
    pub fn get_column_type(&self, schema: &Schema) -> Result<ColumnType, ExpressionError> {
        match *self {
            Expression::Function { ref name, ref args, .. } => {
                let func = try!(DIRECTORY.get(name.as_ref()));
                match func.get_as_scalar() {
                    Some(scalar_func) => {
                        let result = try!(scalar_func.get_return_type(args.clone(), schema));
                        Ok(result)
                    }
                    None => {
                        Err(ExpressionError::NotScalarFunction(name.clone()))
                    }
                }
            }
            Expression::Subquery(ref clause) => {
                if clause.values.is_empty() {
                    Err(ExpressionError::SubqueryEmpty(*clause.clone()))
                } else if clause.values.len() > 1 {
                    Err(ExpressionError::SubqueryNotScalar(*clause.clone()))
                } else if let Some(info) = ColumnInfo::from_select_value(&clause.values[0], &mut None) {
                    Ok(info.column_type)
                } else {
                    Err(ExpressionError::CannotDetermineSubqueryType(*clause.clone()))
                }
            }
            Expression::True | Expression::False => Ok(ColumnType::TinyInt),
            Expression::Null => Ok(ColumnType::Null),
            Expression::Int(_) => Ok(ColumnType::Integer),
            Expression::Long(_) => Ok(ColumnType::BigInt),
            Expression::Float(_) => Ok(ColumnType::Float),
            Expression::Double(_) => Ok(ColumnType::Double),
            Expression::String(ref s) => Ok(ColumnType::VarChar { length: s.len() as u16 }),
            Expression::ColumnValue(ref name) => {
                let columns = schema.find_columns(name);
                if columns.len() != 1 {
                    Err(ExpressionError::CouldNotResolve(name.clone()))
                } else {
                    let (_, ref info) = columns[0];
                    Ok(info.column_type)
                }
            }
            Expression::OR(ref exprs) | Expression::AND(ref exprs) => {
                for expr in exprs {
                    let expr_type = try!(expr.get_column_type(schema));
                    if expr_type != ColumnType::TinyInt {
                        return Err(ExpressionError::NotBooleanExpr(expr.clone(), expr_type));
                    }
                }
                Ok(ColumnType::TinyInt)
            }
            Expression::NOT(ref inner) => {
                match try!(inner.get_column_type(schema)) {
                    ColumnType::TinyInt => Ok(ColumnType::TinyInt),
                    t => Err(ExpressionError::NotBooleanExpr(*inner.clone(), t)),
                }
            }
            Expression::IsNull(_) => Ok(ColumnType::TinyInt),
            // TODO: Compare should actually check if they're comparable.
            Expression::Compare(_, _, _) => Ok(ColumnType::TinyInt),
            Expression::Arithmetic(ref left, op, ref right) => {
                let left_type = try!(left.get_column_type(schema));
                let right_type = try!(right.get_column_type(schema));
                if !left_type.is_numeric() {
                    Err(ExpressionError::NotNumericExpr(*left.clone(), left_type))
                } else if !right_type.is_numeric() {
                    Err(ExpressionError::NotNumericExpr(*right.clone(), right_type))
                } else {
                    Ok(arithmetic_result_type(op, left_type, right_type))
                }
            }
        }
    }
}

fn write_expr_parens(f: &mut ::std::fmt::Formatter, expr: &Expression) -> ::std::fmt::Result {
    write!(f, "{}", expr)
}

fn wrap_expr_parens(expr: &Expression) -> String {
    if let Some(_) = expr.try_literal() {
        format!("{}", expr)
    } else {
        format!("({})", expr)
    }
}

impl ::std::fmt::Display for Expression {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            Expression::Function { ref name, ref distinct, ref args } => {
                try!(write!(f, "{}(", name));
                if *distinct {
                    try!(write!(f, "DISTINCT "));
                }
                let arg_vals: Vec<String> = args.iter().map(|expr| format!("{}", expr)).collect();
                write!(f, "{})", arg_vals.join(", "))
            }
            Expression::True => write!(f, "TRUE"),
            Expression::False => write!(f, "FALSE"),
            Expression::Null => write!(f, "NULL"),
            Expression::Int(num) => write!(f, "{}", num),
            Expression::Long(num) => write!(f, "{}", num),
            Expression::Float(num) => write!(f, "{}", num),
            Expression::Double(num) => write!(f, "{}", num),
            Expression::String(ref s) => write!(f, "\'{}\'", s),
            Expression::ColumnValue(ref name) => write!(f, "{}", column_name_to_string(name)),
            Expression::Subquery(ref clause) => write!(f, "({})", clause),
            Expression::OR(ref exprs) => {
                let r: Vec<_> = exprs.iter().map(|e| wrap_expr_parens(e)).collect();
                write!(f, "{}", r.join(" OR "))
            }
            Expression::AND(ref exprs) => {
                let r: Vec<_> = exprs.iter().map(|e| wrap_expr_parens(e)).collect();
                write!(f, "{}", r.join(" AND "))
            }
            Expression::NOT(ref e) => {
                try!(write!(f, "!"));
                write_expr_parens(f, e)
            }
            Expression::IsNull(ref e) => {
                try!(write_expr_parens(f, e));
                write!(f, " IS NULL")
            }
            Expression::Compare(ref l, op, ref r) => {
                try!(write_expr_parens(f, l));
                try!(write!(f, " {} ", op));
                write_expr_parens(f, r)
            }
            Expression::Arithmetic(ref l, op, ref r) => {
                try!(write_expr_parens(f, l));
                try!(write!(f, " {} ", op));
                write_expr_parens(f, r)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::expressions::{ArithmeticType, CompareType, ExpressionError, Literal};
    use ::relations::{ColumnInfo, ColumnType, Schema};

    #[test]
    fn test_arithmetic() {
        let expr1 = Expression::Int(123);
        let expr2 = Expression::Arithmetic(Box::new(Expression::Int(123)),
                                           ArithmeticType::Plus,
                                           Box::new(Expression::Int(432)));
        let expr3 = Expression::Arithmetic(Box::new(Expression::Int(123)),
                                           ArithmeticType::Minus,
                                           Box::new(Expression::Int(432)));
        let expr4 = Expression::Arithmetic(Box::new(Expression::Int(123)),
                                           ArithmeticType::Plus,
                                           Box::new(Expression::Long(432)));
        let expr5 = Expression::Arithmetic(Box::new(Expression::Long(123)),
                                           ArithmeticType::Plus,
                                           Box::new(Expression::Int(432)));
        let expr6 = Expression::Arithmetic(Box::new(Expression::Long(123)),
                                           ArithmeticType::Plus,
                                           Box::new(Expression::True));
        let expr7 = Expression::Arithmetic(Box::new(Expression::Int(3)),
                                           ArithmeticType::Multiply,
                                           Box::new(Expression::Int(7)));
        let expr8 = Expression::Arithmetic(Box::new(Expression::Int(11)),
                                           ArithmeticType::Divide,
                                           Box::new(Expression::Int(4)));
        let expr9 = Expression::Arithmetic(Box::new(Expression::Int(11)),
                                           ArithmeticType::Remainder,
                                           Box::new(Expression::Int(4)));
        let expr10 = Expression::Arithmetic(Box::new(Expression::Int(11)),
                                            ArithmeticType::Divide,
                                            Box::new(Expression::Float(4f32)));
        let expr11 = Expression::Arithmetic(Box::new(Expression::Int(11)),
                                            ArithmeticType::Divide,
                                            Box::new(Expression::Double(4f64)));
        assert_eq!(Err(ExpressionError::NotNumeric(Literal::True)), expr6.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(123)), expr1.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(555)), expr2.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(-309)), expr3.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Long(555)), expr4.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Long(555)), expr5.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(21)), expr7.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(2)), expr8.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Int(3)), expr9.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Float(2.75)), expr10.evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::Double(2.75)), expr11.evaluate(&mut None, &mut None));
    }

    #[test]
    fn test_is_null() {
        assert_eq!(Ok(Literal::True), Expression::IsNull(Box::new(Expression::Null)).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::IsNull(Box::new(Expression::True)).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::IsNull(Box::new(Expression::False)).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::IsNull(Box::new(Expression::Int(430))).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::IsNull(Box::new(Expression::Double(2.3))).evaluate(&mut None, &mut None));
    }

    #[test]
    fn test_boolean() {
        let e_true = Expression::True;
        let e_false = Expression::False;
        let e_other = Expression::Int(34);

        assert_eq!(Ok(Literal::True), Expression::AND(vec![e_true.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::OR(vec![e_true.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::NOT(Box::new(e_true.clone())).evaluate(&mut None, &mut None));

        assert_eq!(Ok(Literal::False), Expression::AND(vec![e_false.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::OR(vec![e_false.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::NOT(Box::new(e_false.clone())).evaluate(&mut None, &mut None));

        assert_eq!(Ok(Literal::False), Expression::AND(vec![e_false.clone(), e_true.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::OR(vec![e_false.clone(), e_true.clone()]).evaluate(&mut None, &mut None));

        assert_eq!(Err(ExpressionError::EmptyExpression), Expression::AND(vec![]).evaluate(&mut None, &mut None));
        assert_eq!(Err(ExpressionError::EmptyExpression), Expression::OR(vec![]).evaluate(&mut None, &mut None));

        assert_eq!(Err(ExpressionError::NotBoolean(Literal::Int(34))), Expression::AND(vec![e_other.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Err(ExpressionError::NotBoolean(Literal::Int(34))), Expression::OR(vec![e_other.clone()]).evaluate(&mut None, &mut None));
        assert_eq!(Err(ExpressionError::NotBoolean(Literal::Int(34))), Expression::NOT(Box::new(e_other.clone())).evaluate(&mut None, &mut None));
    }

    #[test]
    fn test_compare() {
        let left = Box::new(Expression::Int(30));
        let left2 = Box::new(Expression::Float(30.0));
        let right = Box::new(Expression::Long(35));

        assert_eq!(Err(ExpressionError::NotNumeric(Literal::Null)), Expression::Compare(left.clone(), CompareType::LessThan, Box::new(Expression::Null)).evaluate(&mut None, &mut None));

        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::LessThan, right.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::LessThanEqual, right.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::GreaterThan, right.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::GreaterThanEqual, right.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::Equals, right.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::NotEquals, right.clone()).evaluate(&mut None, &mut None));

        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::LessThan, left.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::LessThanEqual, left.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::GreaterThan, left.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::GreaterThanEqual, left.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::Equals, left.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::NotEquals, left.clone()).evaluate(&mut None, &mut None));

        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::LessThan, left2.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::LessThanEqual, left2.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::GreaterThan, left2.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::GreaterThanEqual, left2.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::True), Expression::Compare(left.clone(), CompareType::Equals, left2.clone()).evaluate(&mut None, &mut None));
        assert_eq!(Ok(Literal::False), Expression::Compare(left.clone(), CompareType::NotEquals, left2.clone()).evaluate(&mut None, &mut None));
    }

    #[test]
    fn test_expr_column_type() {
        let empty_schema = Schema::new();

        assert_eq!(Ok(ColumnType::Integer), Expression::Int(3).get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Double), Expression::Double(3.0).get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::BigInt), Expression::Long(30).get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Float), Expression::Float(3.0).get_column_type(&empty_schema));

        assert!(Expression::Function {
            name: "ABS".into(),
            distinct: false,
            args: vec![],
        }.get_column_type(&empty_schema).is_err());
        assert_eq!(Ok(ColumnType::Integer), Expression::Function {
            name: "ABS".into(),
            distinct: false,
            args: vec![Expression::Int(3)],
        }.get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Float), Expression::Function {
            name: "ABS".into(),
            distinct: false,
            args: vec![Expression::Float(3.3)],
        }.get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Double), Expression::Function {
            name: "ABS".into(),
            distinct: false,
            args: vec![Expression::String("s".into())],
        }.get_column_type(&empty_schema));
        assert_eq!(Err(ExpressionError::NotScalarFunction("COALESCE".into())), Expression::Function {
            name: "COALESCE".into(),
            distinct: false,
            args: vec![],
        }.get_column_type(&empty_schema));

        let schema = Schema::with_columns(vec![ColumnInfo::with_name(ColumnType::Integer, "A")]).unwrap();
        let col_name = (None, Some("A".into()));
        let a_expr = Expression::ColumnValue(col_name.clone());
        assert_eq!(Err(ExpressionError::CouldNotResolve(col_name.clone())), a_expr.get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Integer), a_expr.get_column_type(&schema));

        assert_eq!(Ok(ColumnType::BigInt), Expression::Arithmetic(Box::new(Expression::Int(6)),
                                                                  ArithmeticType::Plus,
                                                                  Box::new(Expression::Long(12304)))
            .get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Integer), Expression::Arithmetic(Box::new(Expression::Int(6)),
                                                                   ArithmeticType::Plus,
                                                                   Box::new(Expression::Int(12304)))
            .get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Float), Expression::Arithmetic(Box::new(Expression::Int(6)),
                                                                 ArithmeticType::Plus,
                                                                 Box::new(Expression::Float(123.4)))
            .get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Double), Expression::Arithmetic(Box::new(Expression::Int(6)),
                                                                  ArithmeticType::Plus,
                                                                  Box::new(Expression::Double(123.4)))
            .get_column_type(&empty_schema));
        assert_eq!(Ok(ColumnType::Double), Expression::Arithmetic(Box::new(Expression::Int(6)),
                                                                  ArithmeticType::Divide,
                                                                  Box::new(Expression::Int(124)))
            .get_column_type(&empty_schema));
        // No way to express NUMERIC types yet, so... nothing here.
        assert_eq!(Err(ExpressionError::NotNumericExpr(Expression::Null, ColumnType::Null)),
        Expression::Arithmetic(Box::new(Expression::Int(5)),
                               ArithmeticType::Divide,
                               Box::new(Expression::Null))
            .get_column_type(&empty_schema));
        assert_eq!(Err(ExpressionError::NotNumericExpr(Expression::Null, ColumnType::Null)),
        Expression::Arithmetic(Box::new(Expression::Null),
                               ArithmeticType::Divide,
                               Box::new(Expression::Int(4)))
            .get_column_type(&empty_schema));
        assert_eq!(Err(ExpressionError::NotBooleanExpr(Expression::Int(6), ColumnType::Integer)),
        Expression::NOT(Box::new(Expression::Int(6)))
            .get_column_type(&empty_schema));
        assert_eq!(Err(ExpressionError::NotBooleanExpr(Expression::Int(3), ColumnType::Integer)),
        Expression::OR(vec![Expression::True, Expression::Int(3)])
            .get_column_type(&empty_schema));
        assert_eq!(Err(ExpressionError::NotBooleanExpr(Expression::Int(3), ColumnType::Integer)),
        Expression::AND(vec![Expression::True, Expression::Int(3)])
            .get_column_type(&empty_schema));
    }
}
