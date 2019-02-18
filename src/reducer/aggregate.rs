#[derive(Debug)]
pub enum AggregateError {
    Parse,
}

pub trait Aggregate: AggregateClone {
    fn update(&mut self, data: &str) -> Result<(), AggregateError>;

    fn value(&self) -> String;
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

#[derive(Default)]
pub struct Sum {
    total: f64,
}

impl Sum {
    pub fn new() -> Sum {
        Default::default()
    }
}

impl Clone for Sum {
    fn clone(&self) -> Sum {
        Sum { total: 0.0 }
    }
}

impl Aggregate for Sum {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => Ok(self.total += num),
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        self.total.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Sum, Aggregate};

    #[test]
    fn test_sum() {
        let mut sum = Sum::new();

        sum.update("3.0");
        sum.update("2");
        sum.update(".5");

        assert_eq!(sum.value(), "5.5");
    }
}
