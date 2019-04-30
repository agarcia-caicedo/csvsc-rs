use std::f64;
use super::{Aggregate, AggregateParseError};
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
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num < self.current {
                        self.current = num;
                    }

                    Ok(())
                }
                Err(_) => Err(()),
            },
            None => Err(()),
        }
    }

    fn value(&self) -> String {
        self.current.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Min};

    #[test]
    fn test_min() {
        let mut min = Min::new(&[""]);

        min.update("3.0").unwrap();
        min.update("2").unwrap();
        min.update(".5").unwrap();

        assert_eq!(min.value(), "0.5");
    }
}
