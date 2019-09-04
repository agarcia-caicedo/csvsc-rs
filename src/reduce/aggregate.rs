use std::fmt::Debug;
use crate::{Headers, Row};

mod avg;
mod last;
mod max;
mod min;
mod sum;
mod count;

pub use avg::Avg;
pub use last::Last;
pub use max::Max;
pub use min::Min;
pub use sum::Sum;
pub use count::Count;

/// Kinds of errors that could happend when creating an AggregatedCol.
#[derive(Debug)]
pub enum AggregateParseError {
    /// Aggregates need at least the new column name and aggregate type to be
    /// parsed.
    TooFewParts,

    /// The given aggregate is not in the known list
    UnknownAggregate(String),

    /// Some Aggregates need parameters like the source column, this error
    /// indicates that some where missing
    MissingParameters,
}

#[derive(Debug)]
pub enum AggregateError {
    /// Specified column does not exist
    MissingColumn(String),

    /// Could not parse value to required type
    ValueError(String),
}

pub trait Aggregate: AggregateClone + Debug {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError>;

    fn value(&self) -> String;
}

// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
pub trait AggregateClone {
    fn clone_box(&self) -> Box<dyn Aggregate>;
}

impl<T> AggregateClone for T
where
    T: 'static + Aggregate + Clone,
{
    fn clone_box(&self) -> Box<dyn Aggregate> {
        Box::new(self.clone())
    }
}

// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<dyn Aggregate> {
    fn clone(&self) -> Box<dyn Aggregate> {
        self.clone_box()
    }
}
