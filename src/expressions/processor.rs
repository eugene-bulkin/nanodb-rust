//! This module contains utilities for processing expressions.

use ::expressions::{Expression, ExpressionError};

/// This trait is used to implement scans or transformations of expression trees by specifying what
/// to do when entering or leaving each expression node. When leaving an expression node, a
/// replacement expression may be returned that will replace the expression node that was just left.
pub trait Processor {
    /// This method is called when expression-traversal is entering a particular node in the
    /// expression tree. It is not possible to replace a node when entering it, because this would
    /// unnecessarily complicate the semantics of expression-tree traversal.
    ///
    /// # Arguments
    /// * node - the `Expression` node being entered
    fn enter(&mut self, node: &Expression) -> Result<(), ExpressionError>;


    /// This method is called when expression-traversal is leaving a particular node in the
    /// expression tree. To facilitate mutation of expression trees, this method must return an
    /// `Expression` object: If the expression processor wants to replace the node being left with
    /// a different node, this method can return the replacement node; otherwise, the method should
    /// return the passed-in node.
    ///
    /// # Arguments
    /// * node - the `Expression` node being left
    fn leave(&mut self, node: &Expression) -> Result<Expression, ExpressionError>;
}
