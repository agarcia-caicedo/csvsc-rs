#[derive(Debug)]
pub enum AggregateParseError {
}

pub fn parse(def: &str) -> Result<Box<dyn Aggregate>, AggregateParseError> {
    unimplemented!()
}

pub trait Aggregate {
    fn update(&mut self, data: &str);

    fn colname(&self) -> &str;
}
