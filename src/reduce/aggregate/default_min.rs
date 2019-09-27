use std::f64;
use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct DefaultMin {
    source: String,
    current: f64,
    colname: String,
}

impl DefaultMin {
    pub fn new(colname: &str, source: &str) -> DefaultMin {
        DefaultMin {
            source: source.to_string(),
            colname: colname.to_string(),
            ..Default::default()
        }
    }
}

impl Default for DefaultMin {
    fn default() -> DefaultMin {
        DefaultMin {
            colname: String::new(),
            source: String::new(),
            current: f64::INFINITY,
        }
    }
}

impl Aggregate for DefaultMin {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num < self.current {
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
    use super::{Aggregate, DefaultMin};
    use crate::{Headers, Row};

    #[test]
    fn test_min() {
        let mut min = DefaultMin::new("new", "a");
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
        let mut min = DefaultMin::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);
        min.update(&h, &r).unwrap();

        assert_eq!(min.value(), "inf");
    }

    #[test]
    fn test_value_error() {
        let mut min = DefaultMin::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);
        min.update(&h, &r).unwrap();

        assert_eq!(min.value(), "inf");
    }
}
