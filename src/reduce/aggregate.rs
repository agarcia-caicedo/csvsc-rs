use std::fmt::Debug;
use crate::{Headers, Row};

mod avg;
mod count;
mod default_max;
mod default_min;
mod last;
mod max;
mod min;
mod sum;

pub use avg::Avg;
pub use count::Count;
pub use last::Last;
pub use max::Max;
pub use default_max::DefaultMax;
pub use min::Min;
pub use default_min::DefaultMin;
pub use sum::Sum;

#[derive(Debug)]
pub enum AggregateError {
    /// Specified column does not exist
    MissingColumn(String),

    /// Could not parse value to required type
    ValueError(String),
}

/// Aggregates used while reducing must implement this trait.
pub trait Aggregate: Debug {
    /// Updates the current value with the next row of data.
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError>;

    /// Gets the current value.
    fn value(&self) -> String;

    /// Gets this aggregate's colname
    fn colname(&self) -> &str;
}
