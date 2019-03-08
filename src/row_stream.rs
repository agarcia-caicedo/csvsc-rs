use super::{
    reducer::{AggregatedCol, ReducerBuildError},
    adjacent_sort::AdjacentSortBuildError,
    Add, ColSpec, Flusher, Headers, Inspect, Reducer, Row, RowResult, AddWith,
    AdjacentReduce, Del, AdjacentSort,
    add::ColBuildError,
};

/// Helper function that retrieves a field from a row given it's header name
pub fn get_field<'r>(headers: &Headers, row: &'r Row, field: &str) -> Option<&'r str> {
    headers.get(field).and_then(|i| row.get(i))
}

/// This trait describes de behaviour of every component in the CSV transformation
/// chain
pub trait RowStream: IntoIterator<Item = RowResult> {
    fn headers(&self) -> &Headers;

    fn add(self, columns: Vec<ColSpec>) -> Add<Self>
    where
        Self: Sized,
    {
        Add::new(self, columns)
    }

    fn del(self, columns: Vec<&str>) -> Del<Self>
    where
        Self: Sized,
    {
        Del::new(self, columns)
    }

    fn add_with<F>(self, colname: &str, f: F) -> AddWith<Self, F>
    where
        Self: Sized,
        F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
    {
        AddWith::new(self, colname, f)
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

    fn adjacent_reduce(
        self,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<AdjacentReduce<Self>, ReducerBuildError>
    where
        Self: Sized,
    {
        AdjacentReduce::new(self, grouping, columns)
    }

    fn adjacent_sort(
        self,
        grouping: Vec<&str>,
        sort_by: &str,
    ) -> Result<AdjacentSort<Self>, AdjacentSortBuildError>
    where
        Self: Sized,
    {
        AdjacentSort::new(self, grouping, sort_by)
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
