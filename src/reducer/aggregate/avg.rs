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
        let mut avg = Avg::new(Rc::new("".to_string()));

        avg.update("3.0").unwrap();
        avg.update("2").unwrap();
        avg.update(".5").unwrap();
        avg.update(".5").unwrap();

        assert_eq!(avg.value(), "1.5");
    }
}
