use std::rc::Rc;
use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Max {
    source: Rc<String>,
    current: f64,
}

impl Max {
    pub fn new(source: Rc<String>) -> Max {
        Max {
            source,
            ..Default::default()
        }
    }
}

impl Clone for Max {
    fn clone(&self) -> Max {
        Max::new(Rc::clone(&self.source))
    }
}

impl Aggregate for Max {
    fn update(&mut self, data: &str) -> Result<(), AggregateError> {
        match data.parse::<f64>() {
            Ok(num) => {
                if num < self.current {
                    self.current = num;
                }

                Ok(())
            },
            Err(_) => Err(AggregateError::Parse),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }

    fn source(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
mod tests {
    use super::{Max, Aggregate};
    use std::rc::Rc;

    #[test]
    fn test_max() {
        let mut max = Max::new(Rc::new("".to_string()));

        max.update("3.0");
        max.update("2");
        max.update(".5");

        assert_eq!(max.value(), "3");
    }
}
