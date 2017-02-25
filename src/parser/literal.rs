

use nom::digit;
use std::str::{self, FromStr};

use super::super::expressions::Literal;

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
