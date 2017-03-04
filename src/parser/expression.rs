use super::literal::literal;
use super::super::expressions::{ArithmeticType, Expression};
use super::utils::*;

named!(base_expr (&[u8]) -> Expression, alt_complete!(
    literal_expr |
    column_name_expr |
    do_parse!(
        ws!(tag!("(")) >>
        e: logical_or_expr >>
        ws!(tag!(")")) >>
        (e)
    )
));

named!(literal_expr (&[u8]) -> Expression, map!(literal, Into::into));
named!(column_name_expr (&[u8]) -> Expression, map!(column_name, Into::into));

named!(unary_op_expr (&[u8]) -> Expression, alt_complete!(
    do_parse!(
        tag!("-") >>
        e: unary_op_expr >>
        (Expression::Arithmetic(Box::new(Expression::Int(0)), ArithmeticType::Minus, Box::new(e)))
    ) |
    do_parse!(
        tag!("+") >>
        e: unary_op_expr >>
        (e)
    ) |
    base_expr
));

named!(mult_expr (&[u8]) -> Expression, do_parse!(
    first: unary_op_expr >>
    result: fold_many0!(do_parse!(
        arith_type: map!(ws!(alt!(tag!("*") | tag!("/") | tag!("%"))), ArithmeticType::from) >>
        expr: ws!(unary_op_expr) >>
        (arith_type, expr)
    ), first, |acc: Expression, (at, e)| {
        Expression::Arithmetic(Box::new(acc.clone()), at, Box::new(e))
    }) >>
    (result)
));

named!(additive_expr (&[u8]) -> Expression, do_parse!(
    first: mult_expr >>
    result: fold_many0!(do_parse!(
        arith_type: map!(ws!(alt!(tag!("+") | tag!("-"))), ArithmeticType::from) >>
        expr: ws!(mult_expr) >>
        (arith_type, expr)
    ), first, |acc: Expression, (at, e)| {
        Expression::Arithmetic(Box::new(acc.clone()), at, Box::new(e))
    }) >>
    (result)
));

named!(relational_expr (&[u8]) -> Expression, alt_complete!(
    do_parse!(
        left: additive_expr >>
        compare_type: ws!(alt_complete!(
            alt_complete!(tag!("==") | tag!("=")) |
            alt!(tag!("<>") | tag!("!=")) |
            tag!(">=") |
            tag!("<=") |
            tag!(">") |
            tag!("<")
        )) >>
        right: additive_expr >>
        (Expression::Compare(Box::new(left), compare_type.into(), Box::new(right)))
    ) |
     do_parse!(
        e: additive_expr >>
        ws!(tag_no_case!("IS")) >>
        invert: opt!(ws!(tag_no_case!("NOT"))) >>
        ws!(tag_no_case!("NULL")) >>
        ({
            let null_res = Expression::IsNull(Box::new(e));
            if invert.is_some() {
                Expression::NOT(Box::new(null_res))
            } else {
                null_res
            }
        })
    ) |
    // TODO: LIKE, etc.
    additive_expr
));

named!(logical_not_expr (&[u8]) -> Expression, do_parse!(
    not: opt!(tag_no_case!("NOT")) >>
    inner: relational_expr >> // TODO: Handle exists_expr
    ({
        if not.is_some() {
            Expression::NOT(Box::new(inner))
        } else {
            inner
        }
    })
));

named!(logical_and_expr (&[u8]) -> Expression, do_parse!(
    clauses: separated_nonempty_list!(ws!(tag_no_case!("AND")), logical_not_expr) >>
    ({
        let has_plural = clauses.len() > 1;
        if has_plural {
            Expression::AND(clauses)
        } else {
            clauses[0].clone()
        }
    })
));

named!(logical_or_expr (&[u8]) -> Expression, do_parse!(
    clauses: separated_nonempty_list!(ws!(tag_no_case!("OR")), logical_and_expr) >>
    ({
        let has_plural = clauses.len() > 1;
        if has_plural {
            Expression::OR(clauses)
        } else {
            clauses[0].clone()
        }
    })
));

named!(pub expression (&[u8]) -> Expression, ws!(logical_or_expr));

#[cfg(test)]
mod tests {
    use nom::IResult::*;
    use super::*;
    use super::super::super::expressions::{ArithmeticType, CompareType, Expression};

    #[test]
    fn test_base_expr() {
        assert_eq!(Done(&[][..], Expression::Int(234)), base_expr(b"234"));
        assert_eq!(Done(&[][..], Expression::ColumnValue((None, Some("A".into())))), base_expr(b"a"));
        assert_eq!(Done(&[][..], Expression::ColumnValue((Some("B".into()), Some("A".into())))), base_expr(b"b.a"));
    }

    #[test]
    fn test_logical_exprs() {
        assert_eq!(Done(&[][..], Expression::AND(vec![Expression::Int(3), Expression::Int(4)])), logical_and_expr(b"3 AND 4"));
        assert_eq!(Done(&[][..], Expression::AND(vec![Expression::Int(3), Expression::Int(4)])), logical_or_expr(b"3 AND 4"));
        assert_eq!(Done(&[][..], Expression::OR(vec![Expression::Int(3), Expression::Int(4)])), logical_or_expr(b"3 OR 4"));
        assert_eq!(Done(&[][..], Expression::OR(vec![Expression::Int(3), Expression::AND(vec![Expression::Int(4), Expression::Int(5)])])), logical_or_expr(b"3 OR 4 AND 5"));
    }

    #[test]
    fn test_arithmetic_exprs() {
        let three = Box::new(Expression::Int(3));
        let four = Box::new(Expression::Int(4));
        let five = Box::new(Expression::Int(5));
        let minus_five = Box::new(Expression::Arithmetic(Box::new(Expression::Int(0)),
                                                         ArithmeticType::Minus,
                                                         five.clone()));
        let seven = Box::new(Expression::Int(7));
        assert_eq!(Done(&[][..], Expression::Int(67)), additive_expr(b"67"));
        assert_eq!(Done(&[][..], Expression::Arithmetic(three.clone(), ArithmeticType::Plus, four.clone())), additive_expr(b"3 + 4"));
        assert_eq!(Done(&[][..], Expression::Arithmetic(three.clone(), ArithmeticType::Multiply, four.clone())), additive_expr(b"3 * 4"));
        assert_eq!(Done(&[][..], Expression::Arithmetic(Box::new(Expression::Arithmetic(three.clone(),
                                                                                        ArithmeticType::Plus,
                                                                                        four.clone())),
                                                        ArithmeticType::Minus,
                                                        seven.clone(), )), additive_expr(b"3 + 4 - 7"));
        assert_eq!(Done(&[][..], Expression::Arithmetic(Box::new(Expression::Arithmetic(Box::new(Expression::Arithmetic(three.clone(),
                                                                                                                        ArithmeticType::Multiply,
                                                                                                                        five.clone())),
                                                                                        ArithmeticType::Plus,
                                                                                        four.clone())),
                                                        ArithmeticType::Minus,
                                                        seven.clone(), )), additive_expr(b"3 * 5 + 4 - 7"));
        assert_eq!(Done(&[][..], Expression::Arithmetic(Box::new(Expression::Arithmetic(Box::new(Expression::Arithmetic(three.clone(),
                                                                                                                        ArithmeticType::Multiply,
                                                                                                                        minus_five.clone())),
                                                                                        ArithmeticType::Plus,
                                                                                        four.clone())),
                                                        ArithmeticType::Minus,
                                                        seven.clone(), )), additive_expr(b"3 * -5 + 4 - 7"));
    }

    #[test]
    fn test_relational_exprs() {
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::Equals, Box::new(Expression::Int(4)))), relational_expr(b"3 = 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::Equals, Box::new(Expression::Int(4)))), relational_expr(b"3 == 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::NotEquals, Box::new(Expression::Int(4)))), relational_expr(b"3 != 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::NotEquals, Box::new(Expression::Int(4)))), relational_expr(b"3 <> 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::LessThan, Box::new(Expression::Int(4)))), relational_expr(b"3 < 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::LessThanEqual, Box::new(Expression::Int(4)))), relational_expr(b"3 <= 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::GreaterThan, Box::new(Expression::Int(4)))), relational_expr(b"3 > 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::Int(3)), CompareType::GreaterThanEqual, Box::new(Expression::Int(4)))), relational_expr(b"3 >= 4"));
        assert_eq!(Done(&[][..], Expression::IsNull(Box::new(Expression::Int(3)))), relational_expr(b"3 IS NULL"));
        assert_eq!(Done(&[][..], Expression::NOT(Box::new(Expression::IsNull(Box::new(Expression::Int(3)))))), relational_expr(b"3 IS NOT NULL"));

        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::ColumnValue((None, Some("A".into())))), CompareType::LessThanEqual, Box::new(Expression::Int(4)))), relational_expr(b"a <= 4"));
        assert_eq!(Done(&[][..], Expression::Compare(Box::new(Expression::ColumnValue((Some("B".into()), Some("A".into())))), CompareType::GreaterThan, Box::new(Expression::Int(4)))), relational_expr(b"b.a > 4"));
    }
}
