use std::fmt::Debug;
use crate::{Headers, Row};

mod avg;
mod last;
mod max;
mod min;
mod sum;

pub use avg::Avg;
pub use last::Last;
pub use max::Max;
pub use min::Min;
pub use sum::Sum;

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

pub trait Aggregate: AggregateClone + Debug {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()>;

    fn value(&self) -> String;
}

// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
pub trait AggregateClone {
    fn clone_box(&self) -> Box<Aggregate>;
}

impl<T> AggregateClone for T
where
    T: 'static + Aggregate + Clone,
{
    fn clone_box(&self) -> Box<Aggregate> {
        Box::new(self.clone())
    }
}

// We can now implement Clone manually by forwarding to clone_box.
impl Clone for Box<Aggregate> {
    fn clone(&self) -> Box<Aggregate> {
        self.clone_box()
    }
}
