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
}
