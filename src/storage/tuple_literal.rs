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
    pub fn from_tuple<T: Tuple>(tuple: T) -> TupleLiteral {
        let mut result = TupleLiteral::new();
        result.append_tuple(tuple);
        result
    }

    /// Appends the specified tuple's contents to this tuple-literal object.
    ///
    /// # Arguments
    /// * tuple - the tuple data to copy into this tuple-literal
    pub fn append_tuple<T: Tuple>(&mut self, tuple: T) {
        for i in 0..tuple.get_column_count() {
            self.values.push(tuple.get_column_value(i))
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
        if col_index >= self.values.len() {
            Err(TupleError::InvalidColumnIndex)
        } else {
            Ok(self.values[col_index] != Literal::Null)
        }
    }

    fn get_column_value(&self, col_index: usize) -> Literal {
        self.values[col_index].clone()
    }

    fn get_column_count(&self) -> usize {
        self.values.len()
    }
}
