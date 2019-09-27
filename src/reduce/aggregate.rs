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
