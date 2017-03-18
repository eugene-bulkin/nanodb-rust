use std::str::{self, FromStr};

use nom::{alpha, digit};

use ::relations::ColumnName;

#[derive(Debug, Clone, PartialEq)]
/// An enum representing a SQL literal.
pub enum Literal {
    Int(i32),
    Long(i64),
    Double(f64),
    Float(f32),
    String(String),
    Null,
    True,
    False,
}

named!(pub ident (&[u8]) -> String, do_parse!(
    head: map_res!(alt!(alpha | tag!("_")), str::from_utf8) >>
    tail: many0!(map_res!(alt!(alpha | tag!("_") | digit), str::from_utf8)) >>
    ({
        let tail: String = tail.iter().flat_map(|s| s.chars()).collect();
        (String::from(head) + &tail).to_uppercase()
    })
));

named!(pub quoted_ident (&[u8]) -> String, do_parse!(
    tag!("\"") >>
    head: map_res!(alt!(alpha | tag!("_")), str::from_utf8) >>
    tail: many0!(map_res!(alt!(alpha | tag!("_") | digit), str::from_utf8)) >>
    tag!("\"") >>
    ({
        let tail: String = tail.iter().flat_map(|s| s.chars()).collect();
        (String::from(head) + &tail)
    })
));

named!(pub dbobj_ident (&[u8]) -> String, alt_complete!(ident | quoted_ident));

named!(pub column_name (&[u8]) -> ColumnName, do_parse!(
    n1: dbobj_ident >>
    n2: opt!(complete!(preceded!(tag!("."), alt!(map!(dbobj_ident, |i| Some(i)) |
                                                 map!(tag!("*"), |_| None)
                                                )))) >>
    ({
        match n2 {
            Some(col_name) => (Some(n1), col_name),
            None => (None, Some(n1))
        }
    })
));

named!(pub alpha_s (&[u8]) -> String, map!(map_res!(alpha, str::from_utf8), String::from));

named!(pub digit_u16 (&[u8]) -> u16, map_res!(map_res!(digit, str::from_utf8), u16::from_str));

named!(pub digit_u32 (&[u8]) -> u32, map_res!(map_res!(digit, str::from_utf8), u32::from_str));

named!(pub signed_int (&[u8]) -> i32, do_parse!(
    sign: opt!(alt!(tag!("-") | tag!("+"))) >>
    mag: map_res!(map_res!(digit, str::from_utf8), i32::from_str) >>
    ({
        match sign {
            Some(b"-") => -mag,
            None | Some(_) => mag,
        }
    })
));

named!(string_literal (&[u8]) -> Literal, do_parse!(
    tag!("'") >>
    str: map!(many0!(none_of!("'\r\n")), |chars: Vec<char>| {
        let result: String = chars.into_iter().collect();
        result
    }) >>
    tag!("'") >>
    (Literal::String(str))
));

named!(num_literal (&[u8]) -> Literal, alt_complete!(
// Floats or Doubles
    do_parse!(
        ipart: opt!(map_res!(digit, str::from_utf8)) >>
        tag!(".") >>
        dpart: opt!(map_res!(digit, str::from_utf8)) >>
        float: opt!(complete!(tag_no_case!("F"))) >>
        ({
            let ipart = ipart.unwrap_or("0");
            let string = match dpart {
                Some(d) => format!("{}.{}", ipart, d),
                None => ipart.into()
            };
            if float.is_some() {
                Literal::Float(f32::from_str(&string).unwrap())
            } else {
                Literal::Double(f64::from_str(&string).unwrap())
            }
        })
    ) |
// Int or Long
    do_parse!(
        ipart: map_res!(digit, str::from_utf8) >>
        long: opt!(complete!(tag!("L"))) >>
        ({
            if long.is_some() {
                Literal::Long(i64::from_str(ipart).unwrap())
            } else {
                Literal::Int(i32::from_str(ipart).unwrap())
            }
        })
    )
));

named!(pub literal (&[u8]) -> Literal, alt_complete!(
    map!(tag_no_case!("NULL"), |_| Literal::Null) |
    map!(tag_no_case!("TRUE"), |_| Literal::True) |
    map!(tag_no_case!("FALSE"), |_| Literal::False) |
    string_literal |
    num_literal
));

#[cfg(test)]
mod tests {
    use nom::IResult::*;

    use super::*;

    #[test]
    fn test_ident() {
        assert_eq!(Done(&b""[..], "FOO".into()), ident(b"foo"));
        assert_eq!(Done(&b""[..], "FOO_BAR".into()), ident(b"foo_BAR"));
        assert_eq!(Done(&b""[..], "_BUZ".into()), ident(b"_buz"));
        assert_eq!(Done(&b""[..], "_BUZ3".into()), ident(b"_buz3"));
        assert!(ident(b"3foo").is_err());
    }

    #[test]
    fn test_quoted_ident() {
        assert_eq!(Done(&b""[..], "foo".into()), quoted_ident(b"\"foo\""));
        assert_eq!(Done(&b""[..], "foo_BAR".into()), quoted_ident(b"\"foo_BAR\""));
        assert_eq!(Done(&b""[..], "_buz".into()), quoted_ident(b"\"_buz\""));
        assert_eq!(Done(&b""[..], "_buz3".into()), quoted_ident(b"\"_buz3\""));
        assert!(quoted_ident(b"foo").is_err());
    }

    #[test]
    fn test_column_name() {
        assert_eq!(Done(&b""[..], (None, Some("FOO".into()))), column_name(b"foo"));
    }

    #[test]
    fn test_num_literal() {
        assert_eq!(Done(&b""[..], Literal::Double(64.21)), num_literal(b"64.21"));
        assert_eq!(Done(&b""[..], Literal::Float(64.21)), num_literal(b"64.21f"));
        assert_eq!(Done(&b""[..], Literal::Float(0.0)), num_literal(b"0.f"));
        assert_eq!(Done(&b""[..], Literal::Float(0.0)), num_literal(b".f"));
        assert_eq!(Done(&b""[..], Literal::Float(0.34)), num_literal(b".34f"));
        assert_eq!(Done(&b""[..], Literal::Double(0.33)), num_literal(b".33"));
        assert_eq!(Done(&b""[..], Literal::Int(345)), num_literal(b"345"));
        assert_eq!(Done(&b""[..], Literal::Long(763)), num_literal(b"763L"));
    }

    #[test]
    fn test_string_literal() {
        assert_eq!(Done(&b""[..], Literal::String("bar".into())), string_literal(b"'bar'"));
        assert_eq!(Done(&b""[..], Literal::String("".into())), string_literal(b"''"));
        assert!(string_literal(b"'\nfoo'").is_err());
    }

    #[test]
    fn test_literal() {
        assert_eq!(Done(&b""[..], Literal::Float(64.21)), literal(b"64.21f"));
        assert_eq!(Done(&b""[..], Literal::String("bar".into())), literal(b"'bar'"));
        assert_eq!(Done(&b""[..], Literal::Long(763)), literal(b"763L"));
        assert_eq!(Done(&b""[..], Literal::Null), literal(b"null"));
        assert_eq!(Done(&b""[..], Literal::True), literal(b"TRUE"));
        assert_eq!(Done(&b""[..], Literal::False), literal(b"FAlse"));

    }
}
