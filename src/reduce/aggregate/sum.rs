use super::{Aggregate, AggregateParseError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Sum {
    source: String,
    total: f64,
}

impl Sum {
    pub fn new(params: &[&str]) -> Result<Sum, AggregateParseError> {
        Ok(Sum {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
    }
}

impl Clone for Sum {
    fn clone(&self) -> Sum {
        Sum::new(&[&self.source]).unwrap()
    }
}

impl Aggregate for Sum {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => Ok(self.total += num),
                Err(_) => Err(()),
            },
            None => Err(()),
        }
    }

    fn value(&self) -> String {
        self.total.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Sum};

    #[test]
    fn test_sum() {
        let mut sum = Sum::new(&[""]);

        sum.update("3.0").unwrap();
        sum.update("2").unwrap();
        sum.update(".5").unwrap();

        assert_eq!(sum.value(), "5.5");
    }
}
