use std::f64;
use super::{Aggregate, AggregateParseError};
use crate::{Headers, Row};

#[derive(Debug)]
pub struct Max {
    source: String,
    current: f64,
}

impl Max {
    pub fn new(params: &[&str]) -> Result<Max, AggregateParseError> {
        Ok(Max {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
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
        Max::new(&[&self.source]).unwrap()
    }
}

impl Aggregate for Max {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    if num > self.current {
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
    use super::{Aggregate, Max};

    #[test]
    fn test_max() {
        let mut max = Max::new(&[""]);

        max.update("3.0").unwrap();
        max.update("2").unwrap();
        max.update(".5").unwrap();

        assert_eq!(max.value(), "3");
    }
}
