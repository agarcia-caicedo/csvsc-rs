use std::f64;
use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct DefaultMax {
    source: String,
    current: f64,
    colname: String,
}

impl DefaultMax {
    pub fn new(colname: &str, source: &str) -> DefaultMax {
        DefaultMax {
            source: source.to_string(),
            colname: colname.to_string(),
            ..Default::default()
        }
    }
}

impl Default for DefaultMax {
    fn default() -> DefaultMax {
        DefaultMax {
            colname: String::new(),
            source: String::new(),
            current: f64::NEG_INFINITY,
        }
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

    fn colname(&self) -> &str {
        &self.colname
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, DefaultMax};
    use crate::{Headers, Row};

    #[test]
    fn test_max() {
        let mut max = DefaultMax::new("new", "a");
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
        let mut max = DefaultMax::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);
        max.update(&h, &r).unwrap();

        assert_eq!(max.value(), "-inf");
    }

    #[test]
    fn test_value_error() {
        let mut max = DefaultMax::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);
        max.update(&h, &r).unwrap();

        assert_eq!(max.value(), "-inf");
    }
}
