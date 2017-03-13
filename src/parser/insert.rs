use ::commands::InsertCommand;
use ::expressions::Expression;
use ::parser::expression::expression;
use ::parser::utils::*;

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
    values: separated_list!(tag!(","), ws!(expression)) >>
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
    use ::commands::InsertCommand;
    use ::expressions::Expression;

    #[test]
    fn test_insert_cols() {
        assert_eq!(Done(&[][..], vec!["A".into(), "B".into()]), insert_cols(b"(  A,  B )"));
        assert_eq!(Done(&[][..], vec![]), insert_cols(b"()"));
    }

    #[test]
    fn test_parse() {
        assert_eq!(Done(&[][..], Box::new(InsertCommand::new("FOO".into(), vec![], vec![]))), parse(b"INSERT INTO foo () VALUES ()"));
        assert_eq!(Done(&[][..], Box::new(InsertCommand::new("FOO".into(), vec!["A".into(), "B".into()], vec![Expression::Int(2), Expression::Int(3)]))), parse(b"INSERT INTO foo (A, B) VALUES (2, 3)"));
        assert_eq!(Incomplete(Needed::Size(24)), parse(b"INSERT    INTO foo"));
        assert_eq!(Incomplete(Needed::Size(31)), parse(b"INSERT    INTO foo (A, B)"));
    }
}
