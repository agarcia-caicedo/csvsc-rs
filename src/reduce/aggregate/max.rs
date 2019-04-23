use super::{Aggregate, AggregateError};
use std::f64;
use std::rc::Rc;

#[derive(Debug)]
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

impl Default for Max {
    fn default() -> Max {
        Max {
            source: Rc::new(String::new()),
            current: f64::NEG_INFINITY,
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
                if num > self.current {
                    self.current = num;
                }

                Ok(())
            }
            // FIXME think seriously about this ones
            Err(_) => Ok(()),
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
    use super::{Aggregate, Max};
    use std::rc::Rc;

    #[test]
    fn test_max() {
        let mut max = Max::new(Rc::new("".to_string()));

        max.update("3.0").unwrap();
        max.update("2").unwrap();
        max.update(".5").unwrap();

        assert_eq!(max.value(), "3");
    }
}
