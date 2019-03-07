use super::{
    reducer::{AggregatedCol, ReducerBuildError},
    AddColumns, ColSpec, Flusher, Headers, Inspect, Reducer, Row, RowResult,
};

pub fn get_field<'r>(headers: &Headers, row: &'r Row, field: &str) -> Option<&'r str> {
    headers.get(field).and_then(|i| row.get(i))
}

pub trait RowStream: IntoIterator<Item = RowResult> {
    fn headers(&self) -> &Headers;

    fn add_columns(self, columns: Vec<ColSpec>) -> AddColumns<Self>
    where
        Self: Sized,
    {
        AddColumns::new(self, columns)
    }

    fn reduce(
        self,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<Reducer<Self>, ReducerBuildError>
    where
        Self: Sized,
    {
        Reducer::new(self, grouping, columns)
    }

    fn flush(self) -> Flusher<Self>
    where
        Self: Sized,
    {
        Flusher::new(self)
    }

    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        Self: Sized,
        F: FnMut(&RowResult),
    {
        Inspect::new(self, f)
    }
}

#[cfg(test)]
mod tests {
    use super::{get_field, Headers, Row};

    #[test]
    fn test_get_field() {
        let headers = Headers::from_row(Row::from(vec!["id", "val"]));
        let row = Row::from(vec!["1", "40"]);

        assert_eq!(get_field(&headers, &row, "id"), Some("1"));
        assert_eq!(get_field(&headers, &row, "val"), Some("40"));
        assert_eq!(get_field(&headers, &row, "foo"), None);
    }
}
