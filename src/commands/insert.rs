use ::Server;
use ::commands::{Command, ExecutionError};
use ::expressions::Expression;
use ::storage::TupleLiteral;

#[derive(Debug, Clone, PartialEq)]
/// A command for inserting rows into a table.
pub struct InsertCommand {
    table_name: String,
    col_names: Vec<String>,
    values: Vec<Expression>,
}

impl InsertCommand {
    /// Creates a new insert command.
    ///
    /// # Arguments
    pub fn new(table_name: String, col_names: Vec<String>, values: Vec<Expression>) -> InsertCommand {
        InsertCommand {
            table_name: table_name,
            col_names: col_names,
            values: values,
        }
    }
}

impl Command for InsertCommand {
    fn execute(&mut self, server: &mut Server) -> Result<(), ExecutionError> {
        match server.table_manager.get_table(&server.file_manager, self.table_name.as_ref()) {
            Ok(ref mut table) => {
                // Try to evaluate expressions.
                let expr_values = {
                    let mut expr_values = Vec::new();
                    for expr in &self.values {
                        let value = try!(expr.evaluate(&mut None));
                        expr_values.push(value);
                    }
                    expr_values
                };
                // Verify all columns exist in the schema.
                for (i, col_name) in self.col_names.iter().enumerate() {
                    match table.get_schema().get_column(col_name.as_ref()) {
                        Some(column) => {
                            let ref expr_value = expr_values[i];
                            let ref expr = self.values[i];
                            if !column.column_type.can_store_literal(expr_value.clone()) {
                                return Err(ExecutionError::CannotStoreExpression(col_name.clone(), expr.clone()));
                            }
                        }
                        _ => {
                            return Err(ExecutionError::ColumnDoesNotExist(col_name.clone()));
                        }
                    }
                }
                // Create the tuple literal.
                let mut tuple = TupleLiteral::new();
                for value in expr_values {
                    tuple.add_value(value);
                }
                match table.add_tuple(tuple) {
                    Ok(mut page_tuple) => {
                        try!(page_tuple.unpin());
                        Ok(())
                    }
                    Err(e) => {
                        error!("{:?}", e);
                        Err(ExecutionError::Unimplemented)
                    }
                }
            }
            Err(e) => Err(ExecutionError::CouldNotOpenTable(self.table_name.clone(), e)),
        }
    }

    fn as_any(&self) -> &::std::any::Any {
        self
    }
}
