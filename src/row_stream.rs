use std::{
    vec,
    collections::HashMap,
};

use crate::{
    add, col, add_with, error, reduce,

    Add, ColSpec, Flush, Headers, Inspect, Reduce, Row, RowResult, AddWith,
    Del, Group, AdjacentGroup, MockStream, Rename, GroupBuildError,

    aggregate::Aggregate,
    flush::FlushTarget,
};

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
    fn add(self, column: ColSpec) -> Result<Add<Self>, add::BuildError>
    where
        Self: Sized,
    {
        Add::new(self, column)
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
    fn add_with<F>(self, colname: &str, f: F) -> Result<AddWith<Self, F>, add_with::BuildError>
    where
        Self: Sized,
        F: FnMut(&Headers, &Row) -> Result<String, col::BuildError>,
    {
        AddWith::new(self, colname, f)
    }

    /// Group by one or more columns, compute aggregates and output the
    /// resulting columns
    fn reduce(
        self,
        columns: Vec<Box<dyn Aggregate>>,
    ) -> Result<Reduce<Self>, reduce::BuildError>
    where
        Self: Sized,
    {
        Reduce::new(self, columns)
    }

    fn adjacent_group<H, F, R>(
        self,
        header_map: H,
        f: F,
        grouping: &[&str],
    ) -> Result<AdjacentGroup<Self, F>, GroupBuildError>
    where
        H: FnMut(Headers) -> Headers,
        F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
        R: RowStream,
        Self: Sized,
    {
        AdjacentGroup::new(self, header_map, f, grouping)
    }

    fn group<H, F, R>(
        self,
        header_map: H,
        f: F,
        grouping: &[&str],
    ) -> Result<Group<Self, F>, GroupBuildError>
    where
        H: FnMut(Headers) -> Headers,
        F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
        R: RowStream,
        Self: Sized,
    {
        Group::new(self, header_map, f, grouping)
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
