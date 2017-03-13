#![allow(missing_docs)]
//! A module for parsing SQL statements used for NanoDB.

pub mod select;
pub mod utils;
pub mod drop;
pub mod show;
pub mod create;
pub mod literal;
pub mod insert;
pub mod expression;

use self::create::parse as create_parse;
use self::drop::parse as drop_parse;
use self::insert::parse as insert_parse;
use self::select::parse as select_parse;
use self::show::parse as show_parse;

use super::commands::Command;

fn as_boxed_command(c: Box<Command>) -> Box<Command> {
    c as Box<Command>
}

named!(pub statements (&[u8]) -> Vec<Box<Command>>, separated_nonempty_list!(
    tag!(";"),
    alt_complete!(map!(select_parse, as_boxed_command) |
                  map!(show_parse, as_boxed_command) |
                  map!(create_parse, as_boxed_command) |
                  map!(drop_parse, as_boxed_command) |
                  map!(insert_parse, as_boxed_command))
));

#[cfg(test)]
mod tests {
    use std::any::Any;

    use super::statements;
    use super::super::commands::SelectCommand;
    use super::super::expressions::{FromClause, SelectClause, SelectValue};

    #[test]
    fn test_multiple_stmts() {
        let result1 = SelectCommand::new(SelectClause::new(FromClause::base_table("FOO".into(), None),
                                                           false,
                                                           vec![SelectValue::WildcardColumn { table: None }],
                                                           None,
                                                           None,
                                                           None));
        let result2 = SelectCommand::new(SelectClause::new(FromClause::base_table("BAR".into(), None),
                                                           false,
                                                           vec![SelectValue::WildcardColumn { table: None }],
                                                           None,
                                                           None,
                                                           None));

        let parsed = statements(b"SELECT * FROM foo; SELECT * FROM bar");
        assert!(parsed.is_done());
        let (left, parsed_vec) = parsed.unwrap();
        assert_eq!(&b""[..], left);
        assert_eq!(Some(&result1), Any::downcast_ref::<SelectCommand>(parsed_vec[0].as_any()));
        assert_eq!(Some(&result2), Any::downcast_ref::<SelectCommand>(parsed_vec[1].as_any()));
    }
}
