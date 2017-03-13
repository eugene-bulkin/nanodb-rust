use ::column::ColumnType;
use ::commands::CreateCommand;
use ::parser::utils::*;

named!(col_type_len (&[u8]) -> u16, do_parse!(
    ws!(tag!("(")) >>
    length: digit_u16 >>
    ws!(tag!(")")) >>
    (length)
));

named!(col_type (&[u8]) -> ColumnType, do_parse!(
    result: alt_complete!(
// Integers
        map!(tag_no_case!("TINYINT"), |_| ColumnType::TinyInt) |
        map!(tag_no_case!("SMALLINT"), |_| ColumnType::SmallInt) |
        map!(alt_complete!(tag_no_case!("INTEGER") | tag_no_case!("INT")), |_| ColumnType::Integer) |
        map!(tag_no_case!("BIGINT"), |_| ColumnType::BigInt) |
// Numerics
        map!(tag_no_case!("FLOAT"), |_| ColumnType::Float) |
        map!(tag_no_case!("DOUBLE"), |_| ColumnType::Double) |
        do_parse!(
            alt!(tag_no_case!("DECIMAL") | tag_no_case!("NUMERIC")) >>
            ws!(tag!("(")) >>
            precision: digit_u16 >>
            ws!(tag!(",")) >>
            scale: digit_u16 >>
            ws!(tag!(")")) >>
            (ColumnType::Numeric { scale: scale, precision: precision })
        ) |
// String types
        do_parse!(
            tag_no_case!("CHARACTER") >>
            varying: ws!(opt!(tag_no_case!("VARYING"))) >>
            length: col_type_len >>
            ({
                match varying {
                    Some(_) => ColumnType::VarChar { length: length },
                    _ => ColumnType::Char { length: length }
                }
            })
        ) |
        do_parse!(
            tag_no_case!("CHAR") >>
            length: col_type_len >>
            (ColumnType::Char { length: length })
        ) |
        do_parse!(
            tag_no_case!("VARCHAR") >>
            length: col_type_len >>
            (ColumnType::VarChar { length: length })
        ) |
        map!(tag_no_case!("TEXT"), |_| ColumnType::Text) |
        map!(tag_no_case!("BLOB"), |_| ColumnType::Blob) |
// Date/Time types
        map!(tag_no_case!("DATETIME"), |_| ColumnType::DateTime) |
        map!(tag_no_case!("DATE"), |_| ColumnType::Date) |
        map!(tag_no_case!("TIMESTAMP"), |_| ColumnType::Timestamp) |
        map!(tag_no_case!("TIME"), |_| ColumnType::Time)
    ) >>
    (result)
));

named!(column_col_decl (&[u8]) -> (String, ColumnType), do_parse!(
    name: ws!(ident) >>
    col_type: ws!(col_type) >>
    (name, col_type)
));

named!(table_col_decls (&[u8]) -> Vec<(String, ColumnType)>, do_parse!(
    tag!("(") >>
    decls: separated_nonempty_list!(tag!(","), ws!(column_col_decl)) >>
    tag!(")") >>
    (decls)
));

named!(pub create_table (&[u8]) -> Box<CreateCommand>, do_parse!(
    ws!(tag_no_case!("CREATE")) >>
    temp: opt!(ws!(tag_no_case!("TEMPORARY"))) >>
    ws!(tag_no_case!("TABLE")) >>
    if_not_exists: opt!(do_parse!(
        ws!(tag_no_case!("IF")) >>
        ws!(tag_no_case!("NOT")) >>
        ws!(tag_no_case!("EXISTS")) >>
        ()
    )) >>
    table_name: ws!(dbobj_ident) >>
    decls: table_col_decls >>
    alt!(eof!() | peek!(tag!(";"))) >>
    ({
        Box::new(CreateCommand::Table {
            name: table_name,
            temp: temp.is_some(),
            if_not_exists: if_not_exists.is_some(),
            decls: decls,
        })
    })
));

named!(pub create_view (&[u8]) -> Box<CreateCommand>, do_parse!(
    ws!(tag_no_case!("CREATE")) >>
    ws!(tag_no_case!("VIEW")) >>
    alt!(eof!() | peek!(tag!(";"))) >>
    ({
        Box::new(CreateCommand::View)
    })
));

named!(pub parse (&[u8]) -> Box<CreateCommand>, alt_complete!(create_table | create_view));

#[cfg(test)]
mod tests {
    use nom::IResult::*;

    use super::*;
    use ::column::ColumnType;
    use ::commands::CreateCommand;

    #[test]
    fn test_col_type() {
        assert_eq!(Done(&b""[..], ColumnType::TinyInt), col_type(b"tinyint"));
        assert_eq!(Done(&b""[..], ColumnType::SmallInt), col_type(b"smallINT"));
        assert_eq!(Done(&b""[..], ColumnType::Integer), col_type(b"INTEGER"));
        assert_eq!(Done(&b""[..], ColumnType::Integer), col_type(b"INT"));
        assert_eq!(Done(&b""[..], ColumnType::BigInt), col_type(b"BIGINT"));

        assert_eq!(Done(&b""[..], ColumnType::Float), col_type(b"float"));
        assert_eq!(Done(&b""[..], ColumnType::Double), col_type(b"DOUBLE"));
        assert_eq!(Done(&b""[..], ColumnType::Numeric { scale: 2, precision: 5 }), col_type(b"NUMERIC(5, 2)"));
        assert_eq!(Done(&b""[..], ColumnType::Numeric { scale: 7, precision: 4 }), col_type(b"DECIMAL ( 4 , 7)"));

        assert_eq!(Done(&b""[..], ColumnType::Char { length: 30 }), col_type(b"CHAR(30)"));
        assert_eq!(Done(&b""[..], ColumnType::VarChar { length: 20 }), col_type(b"VARCHAR(20)"));
        assert_eq!(Done(&b""[..], ColumnType::Char { length: 15 }), col_type(b"CHARACTER (15)"));
        assert_eq!(Done(&b""[..], ColumnType::VarChar { length: 16 }), col_type(b"CHARACTER VARYING (16)"));
        assert_eq!(Done(&b""[..], ColumnType::Text), col_type(b"TEXT"));
        assert_eq!(Done(&b""[..], ColumnType::Blob), col_type(b"BLOB"));

        assert_eq!(Done(&b""[..], ColumnType::Date), col_type(b"DATE"));
        assert_eq!(Done(&b""[..], ColumnType::DateTime), col_type(b"datetime"));
        assert_eq!(Done(&b""[..], ColumnType::Time), col_type(b"Time"));
        assert_eq!(Done(&b""[..], ColumnType::Timestamp), col_type(b"TIMEstamp"));
    }

    #[test]
    fn test_col_decls() {
        assert_eq!(Done(&b""[..], ("A".into(), ColumnType::Integer)), column_col_decl(b"  a   INTEGER"));
    }

    #[test]
    fn test_table_col_decls() {
        assert_eq!(Done(&b""[..], vec![("A".into(), ColumnType::Integer), ("B".into(), ColumnType::BigInt)]), table_col_decls(b"(\na INTEGER,\nb BIGINT\n)"));
    }

    #[test]
    fn test_create_table() {
        {
            let decl = b"CREATE TABLE foo (\
                     a INTEGER,\
                     b CHAR(30),\
                     c VARCHAR(50),\
                     d NUMERIC(9, 4)\
                    )";
            let expected = CreateCommand::Table {
                name: "FOO".into(),
                temp: false,
                if_not_exists: false,
                decls: vec![
                    ("A".into(), ColumnType::Integer),
                    ("B".into(), ColumnType::Char { length: 30 }),
                    ("C".into(), ColumnType::VarChar { length: 50 }),
                    ("D".into(), ColumnType::Numeric { precision: 9, scale: 4 })
                ],
            };
            let (left, output) = create_table(decl).unwrap();
            assert_eq!((&b""[..], expected), (left, *output));
        }
        {
            let decl = b"CREATE TEMPORARY TABLE bar (\
                     a INTEGER
                    )";
            let expected = CreateCommand::Table {
                name: "BAR".into(),
                temp: true,
                if_not_exists: false,
                decls: vec![
                    ("A".into(), ColumnType::Integer)
                ],
            };
            let (left, output) = create_table(decl).unwrap();
            assert_eq!((&b""[..], expected), (left, *output));
        }
        {
            let decl = b"CREATE TABLE IF NOT EXISTS \"buz\" (a INTEGER)";
            let expected = CreateCommand::Table {
                name: "buz".into(),
                temp: false,
                if_not_exists: true,
                decls: vec![
                    ("A".into(), ColumnType::Integer)
                ],
            };
            let (left, output) = create_table(decl).unwrap();
            assert_eq!((&b""[..], expected), (left, *output));
        }
        assert!(create_table(b"CREATE TABLE (a INTEGER)").is_err());
    }
}
