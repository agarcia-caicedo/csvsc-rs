use std::f64;
use super::{Aggregate, AggregateError, AggregateParseError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct DefaultMax {
    source: String,
    current: f64,
}

impl DefaultMax {
    pub fn new(params: &[&str]) -> Result<DefaultMax, AggregateParseError> {
        Ok(DefaultMax {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
    }
}

impl Default for DefaultMax {
    fn default() -> DefaultMax {
        DefaultMax {
            source: String::new(),
            current: f64::NEG_INFINITY,
        }
    }
}

impl Clone for DefaultMax {
    fn clone(&self) -> DefaultMax {
        DefaultMax::new(&[&self.source]).unwrap()
    }
}

impl Aggregate for DefaultMax {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num > self.current {
                        self.current = num;
                    }

                    Ok(())
                }
                Err(_) => Ok(()),
            },
            None => Ok(()),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, DefaultMax};
    use crate::{Headers, Row};

    #[test]
    fn test_max() {
        let mut max = DefaultMax::new(&["a"]).unwrap();
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
        let mut max = DefaultMax::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);
        max.update(&h, &r).unwrap();

        assert_eq!(max.value(), "-inf");
    }

    #[test]
    fn test_value_error() {
        let mut max = DefaultMax::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);
        max.update(&h, &r).unwrap();

        assert_eq!(max.value(), "-inf");
    }
}
