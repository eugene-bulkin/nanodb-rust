//! A module handling the parsing of select clauses.

use super::super::commands::SelectCommand;
use super::super::expressions::{Expression, SelectClause};
use super::expression::expression;
use super::utils::*;

#[derive(Debug, PartialEq, Clone)]
/// An enum describing a select value.
pub enum Value {
    /// Represents the wildcard `*`.
    All,
    /// Represents multiple select values. For example, the select value in `SELECT a, b AS c FROM
    /// ...` would be represented by `Values::Values(vec![("A", None), ("B", Some("C"))])`.
    Values(Vec<(Expression, Option<String>)>),
}

named!(select_value (&[u8]) -> (Expression, Option<String>), alt_complete!(
        do_parse!(a:expression >> ws!(tag_no_case!("AS")) >> b:dbobj_ident >> (a,b)) => { |res: (Expression, String)| (res.0, Some(res.1)) }
    |   expression => { |res: Expression| (res, None) }
));

named!(select_values (&[u8]) -> Value, do_parse!(
    result: alt!(
            tag!("*")   => { |_| Value::All }
        | separated_nonempty_list!(tag!(","), ws!(select_value)) => { |res: Vec<(Expression, Option<String>)>| Value::Values(res) }
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

/// Parses a `SELECT` statement into a `SelectCommand`.
named!(pub parse (&[u8]) -> Box<SelectCommand>, do_parse!(
    ws!(tag_no_case!("SELECT")) >>
    distinct: opt!(ws!(alt!(
            tag_no_case!("ALL")         => { |_| false }
        |   tag_no_case!("DISTINCT")    => { |_| true }
    ))) >>
    select_values: select_values >>
    ws!(tag_no_case!("FROM")) >>
    table_name: ws!(dbobj_ident) >>
    where_expr: opt!(complete!(do_parse!(
        ws!(tag_no_case!("WHERE")) >>
        e: dbg!(ws!(expression)) >>
        (e)
    ))) >>
    limit: opt!(complete!(limit)) >>
    offset: opt!(complete!(offset)) >>
    alt!(eof!() | peek!(tag!(";"))) >>
    ({
        let clause = SelectClause::new(table_name, match distinct {
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
    use ::expressions::{Expression, SelectClause};
    use super::{Value, limit, parse, select_value, select_values};
    use super::super::super::commands::SelectCommand;

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

        assert_eq!(Done(&b""[..], (cn1.clone(), None)), select_value(b"foo"));
        assert_eq!(Done(&b""[..], (cn1.clone(), Some(kw2.into()))), select_value(b"foo AS bar"));
        assert_eq!(Done(&b""[..], (cn3.clone(), None)), select_value(b"foo.bar"));
        assert_eq!(Done(&b""[..], (cn3.clone(), Some(kw3.into()))), select_value(b"foo.bar as baz"));

        assert_eq!(Done(&b""[..], Value::All), select_values(b"*"));
        assert_eq!(Done(&b""[..], Value::Values(vec![(cn1.clone(), None), (cn2.clone(), None)])), select_values(b"foo,bar"));
        assert_eq!(Done(&b""[..], Value::Values(vec![(cn2.clone(), None), (cn1.clone(), None)])), select_values(b"bar, foo"));
        assert_eq!(Done(&b""[..], Value::Values(vec![(cn1.clone(), None), (cn2.clone(), Some("BUZ".into()))])), select_values(b"foo, bar AS buz"));
    }
    #[test]
    fn test_parse() {

        let kw1 = String::from("FOO");
        let kw2 = String::from("BAR");

        let result1 = SelectCommand::new(SelectClause::new(kw1, false, Value::All, None, None, None));
        let result2 = SelectCommand::new(SelectClause::new(kw2, false, Value::All, None, None, None));
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
