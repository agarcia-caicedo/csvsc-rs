use super::{Aggregate, AggregateParseError};
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
    fn update(&mut self, _h: &Headers, _r: &Row) -> Result<(), ()> {
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

    #[test]
    fn test_count() {
        let mut count = Count::new(&[""]);

        count.update("3.0").unwrap();
        count.update("2").unwrap();
        count.update(".5").unwrap();

        assert_eq!(count.value(), "3");
    }
}
