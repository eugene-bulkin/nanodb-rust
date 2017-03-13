//! A module containing utilities for commands, such as pretty printing.

use std::io::{self, Write};
use std::iter::IntoIterator;

/// Prints a table with a header row (padding to ensure) and the provided rows.
pub fn print_table<S1: Into<String>, S2: Into<String>, H: IntoIterator<Item = S1>, R: IntoIterator<Item = Vec<S2>>>
    (out: &mut Write,
     headers: H,
     rows: R)
     -> io::Result<()> {
    let headers: Vec<String> = headers.into_iter().map(Into::into).collect();
    let rows: Vec<Vec<String>> = rows.into_iter().map(|r| r.into_iter().map(Into::into).collect()).collect();
    let mut max_lengths: Vec<usize> = headers.iter().map(String::len).collect();
    for row in rows.iter() {
        let mut i = 0;
        while i < headers.len() {
            let ref val = row[i];
            if val.len() >= max_lengths[i] {
                max_lengths[i] = val.len();
            }
            i += 1;
        }
    }
    let divider = {
        let mut result = String::new();
        for l in max_lengths.iter() {
            result += &format!("+{}", vec!["-".to_string(); *l + 2].join(""));
        }
        result += "+\n";
        result
    };
    // Write headers
    try!(write!(out, "{}", divider));
    for (i, header) in headers.iter().enumerate() {
        try!(write!(out, "| {: <1$} ", header, max_lengths[i]));
    }
    try!(write!(out, "|\n"));
    try!(write!(out, "{}", divider));

    // Write rows
    for row in rows.iter() {
        for (i, value) in row.iter().enumerate() {
            try!(write!(out, "| {: <1$} ", value, max_lengths[i]));
        }
        try!(write!(out, "|\n"));
    }
    write!(out, "{}", divider)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print_table() {
        let headers = vec!["FOO", "BAR BAR BAZ"];

        {
            let rows = vec![
                vec!["1", "2"],
                vec!["4", "45"],
            ];
            let expected = "+-----+-------------+\n| FOO | BAR BAR BAZ |\n+-----+-------------+\n| 1   | 2           \
                            |\n| 4   | 45          |\n+-----+-------------+\n";
            let mut out: Vec<u8> = Vec::new();
            print_table(&mut out, headers.clone(), rows).unwrap();
            assert_eq!(expected.to_string(),
                       String::from_utf8(out.clone()).unwrap());
        }

        {
            let rows = vec![
                vec!["111111", "2"],
                vec!["1", "45234234234234234"],
            ];
            let expected = "+--------+-------------------+\n| FOO    | BAR BAR BAZ       \
                            |\n+--------+-------------------+\n| 111111 | 2                 |\n| 1      | \
                            45234234234234234 |\n+--------+-------------------+\n";
            let mut out: Vec<u8> = Vec::new();
            print_table(&mut out, headers.clone(), rows).unwrap();
            assert_eq!(expected.to_string(),
                       String::from_utf8(out.clone()).unwrap());
        }
    }
}
