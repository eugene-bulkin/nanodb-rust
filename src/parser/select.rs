//! A module handling the parsing of select clauses.

use std::default::Default;

use super::super::commands::SelectCommand;
use ::expressions::{Expression, SelectClause, FromClause, JoinType, JoinConditionType, SelectValue};
use super::expression::expression;
use super::utils::*;

named!(select_value (&[u8]) -> SelectValue, alt_complete!(
        do_parse!(a:expression >> ws!(tag_no_case!("AS")) >> b:dbobj_ident >> (a,b)) => { |res: (Expression, String)| {
            match res.0 {
                Expression::ColumnValue(ref col_name) => {
                    if col_name.1.is_none() {
                        SelectValue::WildcardColumn { table: col_name.0.clone() }
                    } else {
                        SelectValue::Expression { expression: res.0.clone(), alias: Some(res.1.clone()) }
                    }
                },
                _ => SelectValue::Expression { expression: res.0.clone(), alias: Some(res.1.clone()) }
            }
        } }
    |   expression => { |res: Expression| {
        match res {
            Expression::ColumnValue(ref col_name) => {
                if col_name.1.is_none() {
                    SelectValue::WildcardColumn { table: col_name.0.clone() }
                } else {
                    SelectValue::Expression { expression: res.clone(), alias: None }
                }
            },
            _ => SelectValue::Expression { expression: res.clone(), alias: None }
        }
    } }
));

named!(select_values (&[u8]) -> Vec<SelectValue>, do_parse!(
    result: alt!(
            tag!("*")   => { |_| vec![SelectValue::WildcardColumn { table: None }] }
        | separated_nonempty_list!(tag!(","), ws!(select_value))
    ) >>
    (result)
));

named!(limit (&[u8]) -> Option<i32>, do_parse!(
    ws!(tag_no_case!("LIMIT")) >>
    limit: signed_int >>
    ({
        if limit <= 0 {
            None
        } else {
            Some(limit)
        }
    })
));

named!(offset (&[u8]) -> Option<i32>, do_parse!(
    ws!(tag_no_case!("OFFSET")) >>
    offset: signed_int >>
    ({
        if offset <= 0 {
            None
        } else {
            Some(offset)
        }
    })
));

named!(from_expr (&[u8]) -> FromClause, alt!(
    do_parse!(
        name: dbobj_ident >>
        alias: opt!(complete!(preceded!(ws!(tag_no_case!("AS")), dbobj_ident))) >>
        (FromClause::base_table(name, alias))
    ) |
    do_parse!(
        ws!(tag!("(")) >>
        fc: from_clause >>
        ws!(tag!(")")) >>
        (fc)
    )
)
);

named!(join_type (&[u8]) -> (JoinType, Option<JoinConditionType>), alt_complete!(
        map!(tag_no_case!("CROSS"), |_| (JoinType::Cross, None)) |
        do_parse!(
            natural: opt!(complete!(ws!(tag_no_case!("NATURAL")))) >>
            join: alt_complete!(
                map!(tag_no_case!("INNER"), |_| JoinType::Inner) |
                do_parse!(
                    direction: alt_complete!(
                        map!(tag_no_case!("LEFT"), |_| JoinType::LeftOuter) |
                        map!(tag_no_case!("RIGHT"), |_| JoinType::RightOuter) |
                        map!(tag_no_case!("FULL"), |_| JoinType::FullOuter)
                    ) >>
                    opt!(complete!(ws!(tag_no_case!("OUTER")))) >>
                    (direction)
                ) |
                map!(tag!(""), |_| JoinType::Cross)
            ) >>
            ({
                let mut cond_type: Option<JoinConditionType> = None;
                if let Some(_) = natural {
                    cond_type = Some(JoinConditionType::NaturalJoin);
                }
                (join, cond_type)
            })
        )
    )
);

named!(join_expr (&[u8]) -> FromClause, complete!(do_parse!(
    fc: from_expr >>
    joins: many0!(do_parse!(
        join: opt!(ws!(join_type)) >>
        ws!(tag_no_case!("JOIN")) >>
        right: from_expr >>
        on_expr: opt!(preceded!(ws!(tag_no_case!("ON")), expression)) >>
        ({
            let mut jt = JoinType::Cross;
            let mut ct: JoinConditionType = Default::default();
            if let Some((t, cond_type)) = join {
                jt = t;
                if let Some(cond) = cond_type {
                    ct = cond;
                }
            }
            if let Some(expr) = on_expr {
                ct = JoinConditionType::OnExpr(expr);
            }
            (right, jt, ct)
        })
    )) >>
    ({
        let mut result = fc;
        for &(ref next, ref join, ref cond) in joins.iter() {
            result = FromClause::join_expression(
                Box::new(result),
                Box::new(next.clone()),
                join.clone(),
                cond.clone()
            );
        }
        result
    })
)));

named!(from_clause (&[u8]) -> FromClause, do_parse!(
    exprs: separated_nonempty_list!(ws!(tag!(",")), join_expr) >>
    ({
        let mut result = exprs[0].clone();
        for next in exprs.iter().skip(1) {
            result = FromClause::join_expression(
                Box::new(result),
                Box::new(next.clone()),
                JoinType::Cross,
                Default::default()
            );
        }
        result
    })
));

/// Parses a `SELECT` statement into a `SelectCommand`.
named!(pub parse (&[u8]) -> Box<SelectCommand>, do_parse!(
    ws!(tag_no_case!("SELECT")) >>
    distinct: opt!(ws!(alt!(
            tag_no_case!("ALL")         => { |_| false }
        |   tag_no_case!("DISTINCT")    => { |_| true }
    ))) >>
    select_values: select_values >>
    ws!(tag_no_case!("FROM")) >>
    from_clause: ws!(from_clause) >>
    where_expr: opt!(complete!(do_parse!(
        ws!(tag_no_case!("WHERE")) >>
        e: dbg!(ws!(expression)) >>
        (e)
    ))) >>
    limit: opt!(complete!(limit)) >>
    offset: opt!(complete!(offset)) >>
    alt!(eof!() | peek!(tag!(";"))) >>
    ({
        let clause = SelectClause::new(from_clause, match distinct {
                Some(modifier) => modifier,
                None => false
            }, select_values,
            limit.and_then(|v| v).map(|v| v as u32),
            offset.and_then(|v| v).map(|v| v as u32),
            where_expr,
        );
        Box::new(SelectCommand::new(clause))
    })
));

#[cfg(test)]
mod tests {
    use nom::IResult::*;
    use ::commands::SelectCommand;
    use ::expressions::{Expression, SelectClause, FromClause, JoinConditionType, JoinType, SelectValue};
    use super::*;

    #[test]
    fn test_from_expr() {
        assert_eq!(Done(&b""[..], FromClause::base_table("FOO".into(), None)), from_expr(b"foo"));
        assert_eq!(Done(&b""[..], FromClause::base_table("FOO".into(), Some("BAR".into()))), from_expr(b"foo as bar"));
        assert_eq!(Done(&b" JOIN"[..], FromClause::base_table("FOO".into(), None)), from_expr(b"foo JOIN"));
    }

    #[test]
    fn test_join_type() {
        assert_eq!(Done(&b""[..], (JoinType::Cross, None)), join_type(b""));
        assert_eq!(Done(&b""[..], (JoinType::Cross, None)), join_type(b"CROSS"));
        assert_eq!(Done(&b""[..], (JoinType::Cross, Some(JoinConditionType::NaturalJoin))), join_type(b"NATURAL"));
        assert_eq!(Done(&b""[..], (JoinType::Inner, Some(JoinConditionType::NaturalJoin))), join_type(b"NATURAL INNER"));
        assert_eq!(Done(&b""[..], (JoinType::Inner, None)), join_type(b"INNER"));
        assert_eq!(Done(&b""[..], (JoinType::LeftOuter, Some(JoinConditionType::NaturalJoin))), join_type(b"NATURAL LEFT"));
        assert_eq!(Done(&b""[..], (JoinType::LeftOuter, None)), join_type(b"LEFT OUTER"));
        assert_eq!(Done(&b""[..], (JoinType::RightOuter, Some(JoinConditionType::NaturalJoin))), join_type(b"NATURAL RIGHT"));
        assert_eq!(Done(&b""[..], (JoinType::RightOuter, None)), join_type(b"RIGHT OUTER"));
        assert_eq!(Done(&b""[..], (JoinType::FullOuter, None)), join_type(b"FULL"));
        assert_eq!(Done(&b""[..], (JoinType::FullOuter, None)), join_type(b"FULL OUTER"));
    }

    #[test]
    fn test_limit() {
        assert_eq!(Done(&b""[..], Some(10)), limit(b"LIMIT 10"));
        assert_eq!(Done(&b""[..], Some(15)), limit(b"LIMIT   15"));
        assert_eq!(Done(&b""[..], None), limit(b"LIMIT   0"));
        assert_eq!(Done(&b""[..], None), limit(b"LIMIT   -5"));
    }

    #[test]
    fn test_select_values() {
        let kw1 = "FOO";
        let kw2 = "BAR";
        let kw3 = "BAZ";

        let cn1: Expression = (None, Some(kw1.into())).into();
        let cn2: Expression = (None, Some(kw2.into())).into();
        let cn3: Expression = (Some(kw1.into()), Some(kw2.into())).into();

        assert_eq!(Done(&b""[..], SelectValue::Expression { expression: cn1.clone(), alias: None }), select_value(b"foo"));
        assert_eq!(Done(&b""[..], SelectValue::Expression { expression: cn1.clone(), alias: Some(kw2.into()) }), select_value(b"foo AS bar"));
        assert_eq!(Done(&b""[..], SelectValue::Expression { expression: cn3.clone(), alias: None }), select_value(b"foo.bar"));
        assert_eq!(Done(&b""[..], SelectValue::Expression { expression: cn3.clone(), alias: Some(kw3.into()) }), select_value(b"foo.bar as baz"));

        assert_eq!(Done(&b""[..], vec![SelectValue::WildcardColumn { table: None }]), select_values(b"*"));
        assert_eq!(Done(&b""[..], vec![SelectValue::WildcardColumn { table: Some(kw1.into()) }]), select_values(b"foo.*"));
        assert_eq!(Done(&b""[..], vec![SelectValue::Expression { expression: cn1.clone(), alias: None },
                                                     SelectValue::Expression { expression: cn2.clone(), alias: None }
        ]), select_values(b"foo,bar"));
        assert_eq!(Done(&b""[..], vec![SelectValue::Expression { expression: cn2.clone(), alias: None },
                                                     SelectValue::Expression { expression: cn1.clone(), alias: None }
        ]), select_values(b"bar, foo"));
        assert_eq!(Done(&b""[..], vec![SelectValue::Expression { expression: cn1.clone(), alias: None },
                                                     SelectValue::Expression { expression: cn2.clone(), alias: Some(kw3.into()) }
        ]), select_values(b"foo, bar AS baz"));
    }
    #[test]
    fn test_parse() {

        let kw1 = String::from("FOO");
        let kw2 = String::from("BAR");

        let fc1 = FromClause::base_table(kw1, None);
        let fc2 = FromClause::base_table(kw2, None);

        let result1 = SelectCommand::new(SelectClause::new(fc1, false, vec![SelectValue::WildcardColumn { table: None }], None, None, None));
        let result2 = SelectCommand::new(SelectClause::new(fc2, false, vec![SelectValue::WildcardColumn { table: None }], None, None, None));
        //        let result3 = Statement::Select {
        //            value: Value::All,
        //            distinct: false,
        //            table: "baz".to_owned(),
        //            limit: None,
        //            offset: None,
        //        };
        //        let result4 = Statement::Select {
        //            value: Value::All,
        //            distinct: true,
        //            table: kw1.clone(),
        //            limit: None,
        //            offset: None,
        //        };
        //        let result5 = Statement::Select {
        //            value: Value::Values(vec![(kw2.clone(), None)]),
        //            distinct: false,
        //            table: kw1.clone(),
        //            limit: None,
        //            offset: None,
        //        };
        //        let result6 = Statement::Select {
        //            value: Value::Values(vec![(kw2.clone(), None)]),
        //            distinct: true,
        //            table: kw1.clone(),
        //            limit: None,
        //            offset: None,
        //        };
        //        let result7 = Statement::Select {
        //            value: Value::All,
        //            distinct: false,
        //            table: kw1.clone(),
        //            limit: Some(10),
        //            offset: None,
        //        };
        //        let result8 = Statement::Select {
        //            value: Value::All,
        //            distinct: false,
        //            table: kw1.clone(),
        //            limit: Some(10),
        //            offset: Some(4),
        //        };
        {
            let (left, output) = parse(b"SELECT  * FROM   foo").unwrap();
            assert_eq!((&b""[..], &result1), (left, &*output));
        }
        {
            let (left, output) = parse(b"  SELECT  * FROM  bar").unwrap();
            assert_eq!((&b""[..], &result2), (left, &*output));
        }
        // assert_eq!(Done(&b""[..], result3.clone()), parse(b"SELECT  * FROM
        // baz  "));
        // assert_eq!(Done(&b""[..], result4.clone()), parse(b"SELECT DISTINCT *
        // FROM foo"));
        // assert_eq!(Done(&b""[..], result5.clone()), parse(b"SELECT bar FROM
        // foo"));
        // assert_eq!(Done(&b""[..], result6.clone()), parse(b"SELECT DISTINCT
        // bar FROM foo"));
        // assert_eq!(Done(&b""[..], result7.clone()), parse(b"SELECT * FROM foo
        // LIMIT 10"));
        // assert_eq!(Done(&b""[..], result8.clone()), parse(b"SELECT * FROM foo
        // LIMIT 10 OFFSET 4"));
        // assert_eq!(Error(error_position!(ErrorKind::Alt, &b"4"[..])),
        // parse(b"SELECT * FROM   fo4"));
    }
}
