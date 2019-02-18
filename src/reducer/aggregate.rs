#[derive(Debug)]
pub enum AggregateParseError {
}

pub fn parse(def: &str) -> Result<Box<dyn Aggregate>, AggregateParseError> {
    unimplemented!()
}

pub trait Aggregate: AggregateClone {
    fn update(&mut self, data: &str);

    fn colname(&self) -> &str;
}

// https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-boxed-trait-object
trait AggregateClone {
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
