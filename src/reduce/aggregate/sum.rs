use super::{Aggregate, AggregateError, AggregateParseError};
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
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => Ok(self.total += num),
                Err(_) => Err(AggregateError::ValueError(data.to_string())),
            },
            None => Err(AggregateError::MissingColumn(self.source.to_string())),
        }
    }

    fn value(&self) -> String {
        self.total.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, AggregateError, Sum};
    use crate::{Headers, Row};

    #[test]
    fn test_sum() {
        let mut sum = Sum::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        sum.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        sum.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        sum.update(&h, &r).unwrap();

        assert_eq!(sum.value(), "5.5");
    }

    #[test]
    fn test_missing_column() {
        let mut sum = Sum::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);

        match sum.update(&h, &r) {
            Err(AggregateError::MissingColumn(val)) => assert_eq!(val, "a"),
            Err(AggregateError::ValueError(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }

    #[test]
    fn test_value_error() {
        let mut sum = Sum::new(&["a"]).unwrap();
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);

        match sum.update(&h, &r) {
            Err(AggregateError::ValueError(val)) => assert_eq!(val, "chicken"),
            Err(AggregateError::MissingColumn(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }
}
