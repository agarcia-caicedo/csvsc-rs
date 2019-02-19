use std::rc::Rc;
use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Avg {
    source: Rc<String>,
    sum: f64,
    count: u64,
}

impl Avg {
    pub fn new(source: Rc<String>) -> Avg {
        Avg {
            source,
            ..Default::default()
        }
    }
}

impl Clone for Avg {
    fn clone(&self) -> Avg {
        Avg::new(Rc::clone(&self.source))
    }
}

impl Aggregate for Avg {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => {
                self.sum += num;
                self.count += 1;

                Ok(())
            },
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        (self.sum / self.count as f64).to_string()
    }

    fn source(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::{Avg, Aggregate};
    use std::rc::Rc;

    #[test]
    fn test_avg() {
        let mut Avg = Avg::new(Rc::new("".to_string()));

        Avg.update("3.0");
        Avg.update("2");
        Avg.update(".5");
        Avg.update(".5");

        assert_eq!(Avg.value(), "1.5");
    }
}
