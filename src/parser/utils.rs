use nom::{alpha, digit};

use std::str::{self, FromStr};

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

named!(pub alpha_s (&[u8]) -> String, map!(map_res!(alpha, str::from_utf8), String::from));

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

#[cfg(test)]
mod tests {

    use nom::IResult::*;
    use super::{ident, quoted_ident};

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
}
