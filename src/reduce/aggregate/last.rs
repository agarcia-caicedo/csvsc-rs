use super::{Aggregate, AggregateParseError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Last {
    source: String,
    current: String,
}

impl Last {
    pub fn new(params: &[&str]) -> Result<Last, AggregateParseError> {
        Ok(Last {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
    }
}

impl Clone for Last {
    fn clone(&self) -> Last {
        Last::new(&[&self.source]).unwrap()
    }
}

impl Aggregate for Last {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()> {
        match headers.get_field(row, &self.source) {
            Some(data) => Ok(self.current.replace_range(.., data)),
            None => Err(()),
        }
    }

    fn value(&self) -> String {
        self.current.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Last};

    #[test]
    fn test_last() {
        let mut sum = Last::new(&[""]);

        sum.update("3.0").unwrap();
        sum.update("2").unwrap();
        sum.update(".5").unwrap();

        assert_eq!(sum.value(), ".5");
    }
}
