use std::fmt::Debug;

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

#[derive(Debug)]
pub enum AggregateError {
    Parse,
}

pub trait Aggregate: AggregateClone + Debug {
    fn update(&mut self, data: &str) -> Result<(), AggregateError>;

    fn value(&self) -> String;

    fn source(&self) -> &str;
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
