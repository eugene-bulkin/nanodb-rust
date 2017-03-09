//! This module contains `FROM` clause information.

/// An enumeration specifying the different types of join operation.
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum JoinType {
    /// Inner joins, where only matching rows are included in the result.
    Inner,
    /// Left outer joins, where non-matching rows from the left table are included in the results.
    LeftOuter,
    /// Right outer joins, where non-matching rows from the right table are included in the results.
    RightOuter,
    /// Full outer joins, where non-matching rows from either the left or right table are included
    /// in the results.
    FullOuter,
    /// Cross joins, which are simply a Cartesian product.
    Cross,
    /// Semijoin, where the left table's rows are included when they match one or more rows from the
    /// right table.
    Semijoin,
    /// Antijoin (aka anti-semijoin), where the left table's rows are included when they match none
    /// of the rows from the right table.
    Antijoin,
}

#[derive(Clone, Debug, PartialEq)]
/// This enum represents a hierarchy of one or more base and derived relations that produce the rows
/// considered by `SELECT` clauses.
pub enum FromClause {
    /// A `FROM` clause that just selects a base table and possibly an alias.
    BaseTable {
        /// The name of the table being selected from.
        table: String,
        /// An optional alias to rename the table with.
        alias: Option<String>
    },
    /// A `FROM` clause that is a join expression (may be nested).
    JoinExpression {
        /// The left child of the join.
        left: Box<FromClause>,
        /// The right child of the join.
        right: Box<FromClause>,
        /// The join type.
        join_type: JoinType
    }
}