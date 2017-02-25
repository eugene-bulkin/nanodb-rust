
use super::expression::expression;
use super::super::commands::InsertCommand;
use super::super::expressions::Expression;
use super::utils::*;

named!(insert_cols (&[u8]) -> Vec<String>, do_parse!(
    ws!(tag!("(")) >>
    cols: separated_list!(tag!(","), ws!(dbobj_ident)) >>
    ws!(tag!(")")) >>
    ({
        cols
    })
));

named!(insert_vals (&[u8]) -> Vec<Expression>, do_parse!(
    ws!(tag_no_case!("VALUES")) >>
    ws!(tag!("(")) >>
    values: separated_nonempty_list!(tag!(","), ws!(expression)) >>
    ws!(tag!(")")) >>
    (values)
));

named!(pub parse (&[u8]) -> Box<InsertCommand>, do_parse!(
    ws!(tag_no_case!("INSERT")) >>
    ws!(tag_no_case!("INTO")) >>
    table_name: ws!(dbobj_ident) >>
    cols: opt!(complete!(insert_cols)) >>
    values: insert_vals >>
    alt!(eof!() | peek!(tag!(";"))) >>
    ({
        let col_names = cols.unwrap_or(vec![]);
        Box::new(InsertCommand::new(table_name, col_names, values))
    })
));

#[cfg(test)]
mod tests {

    use nom::IResult::*;
    use nom::Needed;
    use super::*;
    use super::super::super::commands::InsertCommand;

    #[test]
    fn test_insert_cols() {
        assert_eq!(Done(&[][..], vec!["A".into(), "B".into()]), insert_cols(b"(  A,  B )"));
        assert_eq!(Done(&[][..], vec![]), insert_cols(b"()"));
    }

    #[test]
    fn test_parse() {
        assert_eq!(Done(&[][..], Box::new(InsertCommand::new("FOO".into(), vec![]))), parse(b"INSERT INTO foo () VALUES ()"));
        assert_eq!(Done(&[][..], Box::new(InsertCommand::new("FOO".into(), vec!["A".into(), "B".into()]))), parse(b"INSERT INTO foo (A, B)"));
        assert_eq!(Incomplete(Needed::Size(19)), parse(b"INSERT    INTO foo"));

    }
}
