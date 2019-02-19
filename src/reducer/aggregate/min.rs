use std::rc::Rc;
use super::{AggregateError, Aggregate};

#[derive(Default,Debug)]
pub struct Min {
    source: Rc<String>,
    current: f64,
}

impl Min {
    pub fn new(source: Rc<String>) -> Min {
        Min {
            source,
            ..Default::default()
        }
    }
}

impl Clone for Min {
    fn clone(&self) -> Min {
        Min::new(Rc::clone(&self.source))
    }
}

impl Aggregate for Min {
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
    use super::{Min, Aggregate};
    use std::rc::Rc;

    #[test]
    fn test_min() {
        let mut min = Min::new(Rc::new("".to_string()));

        min.update("3.0");
        min.update("2");
        min.update(".5");

        assert_eq!(min.value(), ".5");
    }
}
