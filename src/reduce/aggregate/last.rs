use super::{Aggregate, AggregateError};
use crate::{Headers, Row};

#[derive(Default, Debug)]
pub struct Last {
    source: String,
    current: String,
    colname: String,
}

impl Last {
    pub fn new(colname: &str, source: &str) -> Last {
        Last {
            source: source.to_string(),
            colname: colname.to_string(),
            ..Default::default()
        }
    }
}

impl Aggregate for Last {
    fn update(&mut self, headers: &Headers, row: &Row) -> Result<(), AggregateError> {
        match headers.get_field(row, &self.source) {
            Some(data) => Ok(self.current.replace_range(.., data)),
            None => Err(AggregateError::MissingColumn(self.source.to_string())),
        }
    }

    fn value(&self) -> String {
        self.current.clone()
    }

    fn colname(&self) -> &str {
        &self.colname
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregate, AggregateError, Last};
    use crate::{Headers, Row};

    #[test]
    fn test_last() {
        let mut last = Last::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["a"]));

        let r = Row::from(vec!["3.0"]);
        last.update(&h, &r).unwrap();
        let r = Row::from(vec!["2"]);
        last.update(&h, &r).unwrap();
        let r = Row::from(vec![".5"]);
        last.update(&h, &r).unwrap();

        assert_eq!(last.value(), ".5");
    }

    #[test]
    fn test_missing_column() {
        let mut last = Last::new("new", "a");
        let h = Headers::from_row(Row::from(vec!["b"]));

        let r = Row::from(vec!["3.0"]);

        match last.update(&h, &r) {
            Err(AggregateError::MissingColumn(val)) => assert_eq!(val, "a"),
            Err(AggregateError::ValueError(_)) => panic!("wrong error"),
            Ok(_) => panic!("Test failed"),
        }
    }
}
