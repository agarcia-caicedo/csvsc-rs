use std::f64;
use super::{Aggregate, AggregateError, AggregateParseError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct Min {
    source: String,
    current: f64,
}

impl Min {
    pub fn new(params: &[&str]) -> Result<Min, AggregateParseError> {
        Ok(Min {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
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
        Min::new(&[&self.source]).unwrap()
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
            None => Err(AggregateError::UnexistentColumn(self.source.to_string())),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Min};
    use crate::{Headers, Row};

    #[test]
    fn test_min() {
        let mut min = Min::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        min.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        min.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        min.update(&h, &r).unwrap();

        assert_eq!(min.value(), "0.5");
    }
}
