//! This module provides the rename plan node.

use ::relations::{Schema};
use ::queries::{PlanNode, PlanResult, PlanError};
use ::storage::Tuple;

/// PlanNode representing the `SELECT` clause in a SQL query. This is the relational algebra Project
/// operator.
pub struct RenameNode<'a> {
    child: Box<PlanNode + 'a>,
    table_name: String,
    input_schema: Schema,
    output_schema: Option<Schema>,
}

impl<'a> RenameNode<'a> {
    /// Instantiate a new rename node.
    ///
    /// # Argument
    /// * child - The child of the node.
    /// * table_name - The new table name for the node.
    pub fn new<S: Into<String>>(child: Box<PlanNode + 'a>, table_name: S) -> RenameNode<'a> {
        let schema = child.get_schema();
        RenameNode {
            child: child,
            table_name: table_name.into(),
            input_schema: schema,
            // This will only be Some(...) if the node has been prepared!
            output_schema: None,
        }
    }
}

impl<'a> PlanNode for RenameNode<'a> {
    fn get_schema(&self) -> Schema {
        self.output_schema.clone().unwrap_or(Schema::new())
    }

    #[inline]
    fn get_next_tuple(&mut self) -> PlanResult<Option<&mut Tuple>> {
        self.child.get_next_tuple()
    }

    fn prepare(&mut self) -> PlanResult<()> {
        if self.input_schema.is_empty() {
            return Err(PlanError::NodeNotPrepared);
        }

        let mut new_schema = self.input_schema.clone();
        try!(new_schema.set_table_name(self.table_name.as_ref()));

        self.output_schema = Some(new_schema);

        Ok(())
    }

    #[inline]
    fn initialize(&mut self) {
        self.child.initialize();
    }
}