use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Count {
    total: u64,
    colname: String,
}

impl Count {
    pub fn new(colname: &str) -> Count {
        Count {
            colname: colname.to_string(),
            ..Default::default()
        }
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

    fn colname(&self) -> &str {
        &self.colname
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, Count};
    use crate::{Headers, Row};

    #[test]
    fn test_count() {
        let mut count = Count::new("new");
        let h = Headers::from_row(Row::new());
        let r = Row::new();

        count.update(&h, &r).unwrap();
        count.update(&h, &r).unwrap();
        count.update(&h, &r).unwrap();

        assert_eq!(count.value(), "3");
    }
}
