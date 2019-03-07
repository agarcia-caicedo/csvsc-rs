use super::{Aggregate, AggregateError};
use std::rc::Rc;

#[derive(Default, Debug)]
pub struct Sum {
    source: Rc<String>,
    total: f64,
}

impl Sum {
    pub fn new(source: Rc<String>) -> Sum {
        Sum {
            source,
            ..Default::default()
        }
    }
}

impl Clone for Sum {
    fn clone(&self) -> Sum {
        Sum::new(Rc::clone(&self.source))
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

    fn source(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Sum};
    use std::rc::Rc;

    #[test]
    fn test_sum() {
        let mut sum = Sum::new(Rc::new("".to_string()));

        sum.update("3.0").unwrap();
        sum.update("2").unwrap();
        sum.update(".5").unwrap();

        assert_eq!(sum.value(), "5.5");
    }
}
