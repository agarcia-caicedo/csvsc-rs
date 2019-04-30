use super::{Aggregate, AggregateParseError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Avg {
    source: String,
    sum: f64,
    count: u64,
}

impl Avg {
    pub fn new(params: &[&str]) -> Result<Avg, AggregateParseError> {
        Ok(Avg {
            source: match params.get(0) {
                Some(s) => s.to_string(),
                None => return Err(AggregateParseError::MissingParameters),
            },
            ..Default::default()
        })
    }
}

impl Clone for Avg {
    fn clone(&self) -> Avg {
        Avg::new(&[&self.source]).unwrap()
    }
}

impl Aggregate for Avg {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), ()> {
        match headers.get_field(row, &self.source) {
            Some(data) => match data.parse::<f64>() {
                Ok(num) => {
                    self.sum += num;
                    self.count += 1;

                    Ok(())
                }
                Err(_) => Err(()),
            },
            None => Err(()),
        }
    }

    fn value(&self) -> String {
        (self.sum / self.count as f64).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Avg};
    use crate::{Headers, Row};

    #[test]
    fn test_avg() {
        let mut avg = Avg::new(&["a"]).unwrap();
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
}
