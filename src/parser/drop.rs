use super::super::commands::DropCommand;
use super::utils::*;

named!(pub parse (&[u8]) -> Box<DropCommand>, do_parse!(
    ws!(tag_no_case!("DROP")) >>
    result: ws!(tag_no_case!("TABLE")) >>
    table_name: ws!(dbobj_ident) >>
    alt!(eof!() | peek!(tag!(";"))) >>
    (Box::new(DropCommand::Table(table_name)))
));

#[cfg(test)]
mod tests {

    use nom::IResult::*;
    use nom::Needed;
    use super::*;

    #[test]
    fn test_drop_parse() {
        {
            let (left, output) = parse(b"DROP TABLE foo").unwrap();
            assert_eq!((&b""[..], DropCommand::Table("FOO".into())), (left, *output));
        }
        assert_eq!(Incomplete(Needed::Size(11)), parse(b"DROP TABLE"));
    }
}
