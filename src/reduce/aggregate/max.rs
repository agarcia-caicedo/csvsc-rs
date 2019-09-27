use std::f64;
use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct Max {
    source: String,
    current: f64,
}

impl Max {
    pub fn new(source: &str) -> Max {
        Max {
            source: source.to_string(),
            ..Default::default()
        }
    }
}

impl Default for Max {
    fn default() -> Max {
        Max {
            source: String::new(),
            current: f64::NEG_INFINITY,
        }
    }
}

impl Clone for Max {
    fn clone(&self) -> Max {
        Max::new(&self.source)
    }
}

impl Aggregate for Max {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num > self.current {
                        self.current = num;
                    }

                    Ok(())
                }
                Err(_) => Err(AggregateError::ValueError(data.to_string())),
            },
            None => Err(AggregateError::MissingColumn(self.source.to_string())),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, AggregateError, Max};
    use crate::{Headers, Row};

    #[test]
    fn test_max() {
        let mut max = Max::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        max.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        max.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        max.update(&h, &r).unwrap();

        assert_eq!(max.value(), "3");
    }

    #[test]
    fn test_missing_column() {
        let mut max = Max::new("a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);

        match max.update(&h, &r) {
            Err(AggregateError::MissingColumn(val)) => assert_eq!(val, "a"),
            Err(AggregateError::ValueError(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }

    #[test]
    fn test_value_error() {
        let mut max = Max::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);

        match max.update(&h, &r) {
            Err(AggregateError::ValueError(val)) => assert_eq!(val, "chicken"),
            Err(AggregateError::MissingColumn(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }
}
