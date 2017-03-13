//! A module which stores utilities for a tuple literal.


use super::{PinError, Pinnable, Tuple, TupleError};
use super::super::expressions::Literal;

/// A simple implementation of the {@link Tuple} interface for storing literal tuple values.
#[derive(Clone, Debug, PartialEq)]
pub struct TupleLiteral {
    values: Vec<Literal>,
}

impl TupleLiteral {
    /// Construct a new tuple-literal that initially has zero columns. Column values can be added
    /// with the {@link #addValue} method, or entire tuples can be appended using the
    /// {@link #appendTuple} method.
    pub fn new() -> TupleLiteral {
        TupleLiteral { values: vec![] }
    }

    /// Appends the specified value to the end of the tuple-literal.
    ///
    /// # Arguments
    /// * value - The value to append. This is allowed to be `NULL`.
    pub fn add_value(&mut self, value: Literal) {
        self.values.push(value);
    }

    /// Constructs a new tuple-literal that is a copy of the specified tuple. After construction,
    /// the new tuple-literal object can be manipulated in various ways, just like all
    /// tuple-literals.
    ///
    /// *Note: this is not a `From<Tuple>` trait because `Tuple` is not necessarily sized. When
    /// `impl Trait` is stabilized, then that should be fine.*
    ///
    /// # Arguments
    /// * tuple - the tuple to make a copy of
    pub fn from_tuple<T: Tuple + ?Sized>(tuple: &mut T) -> TupleLiteral {
        let mut result = TupleLiteral::new();
        result.append_tuple(tuple);
        result
    }

    /// Appends the specified tuple's contents to this tuple-literal object.
    ///
    /// # Arguments
    /// * tuple - the tuple data to copy into this tuple-literal
    pub fn append_tuple<T: Tuple + ?Sized>(&mut self, tuple: &mut T) {
        for i in 0..tuple.get_column_count() {
            self.values.push(tuple.get_column_value(i).unwrap())
        }
    }
}

impl Pinnable for TupleLiteral {
    fn pin(&mut self) {}

    fn unpin(&mut self) -> Result<(), PinError> {
        Ok(())
    }

    fn get_pin_count(&self) -> u32 {
        0
    }
}

impl Tuple for TupleLiteral {
    fn is_disk_backed(&self) -> bool {
        false
    }

    fn is_null_value(&self, col_index: usize) -> Result<bool, TupleError> {
        let num_values = self.values.len();
        if col_index >= num_values {
            Err(TupleError::InvalidColumnIndex(col_index, num_values))
        } else {
            Ok(self.values[col_index] != Literal::Null)
        }
    }

    fn get_column_value(&mut self, col_index: usize) -> Result<Literal, TupleError> {
        Ok(self.values[col_index].clone())
    }

    fn get_column_count(&self) -> usize {
        self.values.len()
    }
}

impl ::std::fmt::Display for TupleLiteral {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        try!(write!(f, "TL["));
        let num_columns = self.get_column_count();
        for i in 0..num_columns {
            try!(write!(f, "{}", self.values[i]));
            if i < num_columns - 1 {
                try!(write!(f, ","));
            }
        }
        write!(f, "]")
    }
}

impl From<TupleLiteral> for Vec<String> {
    fn from(tl: TupleLiteral) -> Vec<String> {
        let mut result = Vec::new();
        let num_columns = tl.get_column_count();
        for i in 0..num_columns {
            match tl.values[i] {
                Literal::String(ref s) => {
                    result.push(s.clone());
                },
                _ => {
                    result.push(format!("{}", tl.values[i]));
                }
            }
        }
        result
    }
}