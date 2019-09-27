use std::f64;
use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct Min {
    source: String,
    current: f64,
}

impl Min {
    pub fn new(source: &str) -> Min {
        Min {
            source: source.to_string(),
            ..Default::default()
        }
    }
}

impl Default for Min {
    fn default() -> Min {
        Min {
            source: String::new(),
            current: f64::INFINITY,
        }
    }
}

impl Clone for Min {
    fn clone(&self) -> Min {
        Min::new(&self.source)
    }
}

impl Aggregate for Min {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num < self.current {
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
    use super::{Aggregate, AggregateError, Min};
    use crate::{Headers, Row};

    #[test]
    fn test_min() {
        let mut min = Min::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        min.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        min.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        min.update(&h, &r).unwrap();

        assert_eq!(min.value(), "0.5");
    }

    #[test]
    fn test_missing_column() {
        let mut min = Min::new("a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);

        match min.update(&h, &r) {
            Err(AggregateError::MissingColumn(val)) => assert_eq!(val, "a"),
            Err(AggregateError::ValueError(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }

    #[test]
    fn test_value_error() {
        let mut min = Min::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);

        match min.update(&h, &r) {
            Err(AggregateError::ValueError(val)) => assert_eq!(val, "chicken"),
            Err(AggregateError::MissingColumn(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }
}
