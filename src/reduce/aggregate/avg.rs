use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Avg {
    source: String,
    sum: f64,
    count: u64,
}

impl Avg {
    pub fn new(source: &str) -> Avg {
        Avg {
            source: source.to_string(),
            ..Default::default()
        }
    }
}

impl Clone for Avg {
    fn clone(&self) -> Avg {
        Avg::new(&self.source)
    }
}

impl Aggregate for Avg {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    self.sum += num;
                    self.count += 1;

                    Ok(())
                }
                Err(_) => Err(AggregateError::ValueError(data.to_string())),
            },
            None => Err(AggregateError::MissingColumn(self.source.to_string())),
        }
    }

    fn value(&self) -> String {
        if self.count != 0 {
            (self.sum / self.count as f64).to_string()
        } else {
            "NaN".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, AggregateError, Avg};
    use crate::{Headers, Row};

    #[test]
    fn test_avg() {
        let mut avg = Avg::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        avg.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        avg.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        avg.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        avg.update(&h, &r).unwrap();

        assert_eq!(avg.value(), "1.5");
    }

    #[test]
    fn test_missing_column() {
        let mut avg = Avg::new("a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);

        match avg.update(&h, &r) {
            Err(AggregateError::MissingColumn(val)) => assert_eq!(val, "a"),
            Err(AggregateError::ValueError(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }

    #[test]
    fn test_value_error() {
        let mut avg = Avg::new("a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["chicken"]);

        match avg.update(&h, &r) {
            Err(AggregateError::ValueError(val)) => assert_eq!(val, "chicken"),
            Err(AggregateError::MissingColumn(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }
}
