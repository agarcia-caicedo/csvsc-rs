use crate::error::RowResult;

use super::Row;

pub struct ColSpec {}

impl ColSpec {
    pub fn new(spec: &str) -> ColSpec {
        ColSpec {}
    }

    pub fn compute(&self, data: &Row) -> Vec<&str> {
        Vec::new()
    }
}

pub struct AddColumns<T> {
    iter: T,
    columns: Vec<ColSpec>,
}

impl<T: Iterator<Item = RowResult>> AddColumns<T> {
    pub fn new(iter: T, columns: Vec<ColSpec>) -> AddColumns<T> {
        AddColumns { iter, columns }
    }
}

impl<T: Iterator<Item = RowResult>> Iterator for AddColumns<T> {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|mut val| {
                for spec in self.columns.iter() {
                    let new_fields = spec.compute(&val);

                    for field in new_fields {
                        val.push_field(field);
                    }
                }

                Ok(val)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AddColumns, ColSpec, Row};

    #[test]
    #[ignore]
    fn test_add_columns() {
        let mut add_columns = AddColumns::new(
            vec![
                Row::from(vec!["1", "40", "/tmp/a1m.csv"]),
                Row::from(vec!["2", "39", "/tmp/a1m.csv"]),
                Row::from(vec!["3", "38", "/tmp/a2m.csv"]),
                Row::from(vec!["4", "37", "/tmp/a2m.csv"]),
            ]
            .into_iter()
            .map(|r| Ok(r)),
            vec![ColSpec::new("regex:_source:$1:a([0-9]+)m\\.csv$")],
        );

        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["1", "40", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["2", "39", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["3", "38", "2"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["4", "37", "2"])
        );
    }
}
