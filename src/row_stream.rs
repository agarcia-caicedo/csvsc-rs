use std::{
    vec,
    collections::HashMap,
};

use crate::{
    reduce::{AggregatedCol, ReduceBuildError},
    adjacent_group::AdjacentGroupBuildError,
    add::{ColBuildError, AddBuildError},
    add_with::AddWithBuildError,
    Add, ColSpec, Flush, Headers, Inspect, Reduce, Row, RowResult, AddWith,
    Del, AdjacentGroup, MockStream, Rename,
    error,
    flush::FlushTarget,
};

/// Helper function that retrieves a field from a row given it's header name
pub fn get_field<'r>(headers: &Headers, row: &'r Row, field: &str) -> Option<&'r str> {
    headers.get(field).and_then(|i| row.get(i))
}

/// This trait describes de behaviour of every component in the CSV transformation
/// chain. Functions provided by this trait help construct the chain and can be
/// _chained_.
///
/// Implement this trait to extend `csvsc` with your own processors.
pub trait RowStream: IntoIterator<Item = RowResult> {

    /// Must return headers as they are in this point of the chain. For example
    /// if implementor adds a column, its `headers()` function must return the
    /// new headers including the one just added.
    fn headers(&self) -> &Headers;

    /// Allows adding columns to each row of the stream.
    fn add(self, columns: Vec<ColSpec>) -> Result<Add<Self>, AddBuildError>
    where
        Self: Sized,
    {
        Add::new(self, columns)
    }

    /// Deletes the specified columns from each row of the stream
    fn del(self, columns: Vec<&str>) -> Del<Self>
    where
        Self: Sized,
    {
        Del::new(self, columns)
    }

    /// Adds a column to each row of the stream using a closure to compute its
    /// value
    fn add_with<F>(self, colname: &str, f: F) -> Result<AddWith<Self, F>, AddWithBuildError>
    where
        Self: Sized,
        F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
    {
        AddWith::new(self, colname, f)
    }

    /// Group by one or more columns, compute aggregates and output the
    /// resulting columns
    fn reduce(
        self,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<Reduce<Self>, ReduceBuildError>
    where
        Self: Sized,
    {
        Reduce::new(self, grouping, columns)
    }

    fn adjacent_group<H, F, R>(
        self,
        header_map: H,
        f: F,
        grouping: &[&str],
    ) -> Result<AdjacentGroup<Self, F>, AdjacentGroupBuildError>
    where
        H: FnMut(Headers) -> Headers,
        F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
        R: RowStream,
        Self: Sized,
    {
        AdjacentGroup::new(self, header_map, f, grouping)
    }

    /// When consumed, writes to destination specified by the column given in
    /// the first argument. Other than that this behaves like an `id(x)`
    /// function so you can specify more links in the chain and even more
    /// flushers.
    fn flush(self, target: FlushTarget) -> error::Result<Flush<Self>>
    where
        Self: Sized,
    {
        Ok(Flush::new(self, target)?)
    }

    /// Mostly for debugging, calls a closure on each element. Behaves like the
    /// identity function on the stream returning each row untouched.
    fn inspect<F>(self, f: F) -> Inspect<Self, F>
    where
        Self: Sized,
        F: FnMut(&Headers, &RowResult),
    {
        Inspect::new(self, f)
    }

    /// Renombra algunas columnas
    fn rename(self, name_map: &HashMap<&str, &str>) -> Rename<Self>
    where
        Self: Sized,
    {
        Rename::new(self, name_map)
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
