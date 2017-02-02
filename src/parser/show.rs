use super::super::commands::ShowCommand;

named!(pub parse (&[u8]) -> Box<ShowCommand>, do_parse!(
    ws!(tag_no_case!("SHOW")) >>
    result: alt!(
            tag_no_case!("TABLES")      => { |_| ShowCommand::Tables }
        |   tag_no_case!("VARIABLES")   => { |_| ShowCommand::Variables }
    ) >>
    alt!(eof!() | peek!(tag!(";"))) >>
    (Box::new(result))
));

#[cfg(test)]
mod tests {
    use nom::ErrorKind;

    use nom::IResult::*;
    use super::parse;
    use super::super::super::commands::ShowCommand;

    #[test]
    fn test_show() {
        {
            let (left, output) = parse(b"SHOW TABLES").unwrap();
            assert_eq!((&b""[..], ShowCommand::Tables), (left, *output));
        }
        {
            let (left, output) = parse(b"SHOW   VARIABLES").unwrap();
            assert_eq!((&b""[..], ShowCommand::Variables), (left, *output));
        }
        assert_eq!(Error(error_position!(ErrorKind::Alt, &b"f"[..])), parse(b"SHOW VARIABLESf"));
    }
}
