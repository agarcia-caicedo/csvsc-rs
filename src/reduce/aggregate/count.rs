use super::{Aggregate, AggregateError, AggregateParseError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Count {
    total: u64,
}

impl Count {
    pub fn new(_params: &[&str]) -> Result<Count, AggregateParseError> {
        Ok(Count {
            ..Default::default()
        })
    }
}

impl Clone for Count {
    fn clone(&self) -> Count {
        Count::new(&[]).unwrap()
    }
}

impl Aggregate for Count {
    fn update(&mut self, _h: &Headers, _r: &Row) -> Result<(), AggregateError> {
        self.total += 1;

        Ok(())
    }

    fn value(&self) -> String {
        self.total.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Count};
    use crate::{Headers, Row};

    #[test]
    fn test_count() {
        let mut count = Count::new(&[""]).unwrap();
        let h = Headers::from_row(Row::new());
        let r = Row::new();

        count.update(&h, &r).unwrap();
        count.update(&h, &r).unwrap();
        count.update(&h, &r).unwrap();

        assert_eq!(count.value(), "3");
    }
}
