use crate::{Error, Headers, RowResult, RowStream};
use std::collections::hash_map;
use std::collections::HashMap;
use std::rc::Rc;

pub mod aggregate;
pub mod group;
pub mod aggregated_col;

use aggregate::Aggregate;
use group::Group;
use aggregated_col::AggregatedCol;

/// Kinds of errors that can happen when building a Reduce processor.
#[derive(Debug)]
pub enum BuildError {
    GroupingKeyError(String),
    AggregateSourceError(String),
    DuplicatedHeader(String),
}

/// Used to group and aggregate the rows, yielding the results as a new stream
/// of rows with potentially new columns.
pub struct Reduce<I> {
    iter: I,
    group_by: Vec<String>,
    columns: Vec<AggregatedCol>,
    headers: Headers,
}

impl<I> Reduce<I>
where
    I: RowStream,
{
    /// Creates a new Reduce from an implementor of the RowStream trait, a set
    /// of column names for grouping and a set of aggregates to calculate and
    /// add as columns.
    pub fn new(
        iter: I,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<Reduce<I>, BuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(BuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        let mut whole_columns = Vec::with_capacity(headers.len() + columns.len());

        for header in headers.iter() {
            let source = Rc::new(header.to_string());

            whole_columns.push(AggregatedCol::new(
                header,
                Rc::clone(&source),
                Box::new(aggregate::Last::new(Rc::clone(&source))),
            ));
        }

        for col in columns.iter() {
            if !headers.contains_key(col.source()) {
                return Err(BuildError::AggregateSourceError(
                    col.source().to_string(),
                ));
            }
        }

        for col in columns.iter() {
            if let Err(_) = headers.add(col.colname()) {
                return Err(BuildError::DuplicatedHeader(col.colname().to_string()));
            }
        }

        for column in columns {
            whole_columns.push(column);
        }

        Ok(Reduce {
            iter,
            group_by,
            columns: whole_columns,
            headers,
        })
    }
}

pub struct IntoIter {
    error: Option<Error>,
    iter: hash_map::IntoIter<u64, Group>,
}

impl Iterator for IntoIter {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.error.take() {
            Some(e) => Some(Err(e)),
            None => self.iter.next().map(|g| Ok(g.1.as_row())),
        }
    }
}

impl<I> IntoIterator for Reduce<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut groups = HashMap::new();
        let mut error: Option<Error> = None;
        let aggregates = self.columns;
        let headers = self.headers;
        let iter = self.iter.into_iter();

        for result in iter {
            match result {
                Ok(item) => {
                    let row_hash = headers.hash(&item, &self.group_by).unwrap();

                    groups
                        .entry(row_hash)
                        .and_modify(|group: &mut Group| {
                            group.update(&headers, &item);
                        })
                        .or_insert_with(|| {
                            let mut g = Group::from(&aggregates);

                            g.update(&headers, &item);

                            g
                        });
                }
                Err(e) => {
                    error.get_or_insert(e);
                }
            }
        }

        IntoIter {
            iter: groups.into_iter(),
            error,
        }
    }
}

impl<I> RowStream for Reduce<I>
where
    Reduce<I>: IntoIterator<Item = RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{hash_row, Error, HashError, Headers, Reduce};
    use crate::{Row, col, mock::MockStream};

    #[test]
    fn test_reduce_id_function() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["name", "_target"])),
                Ok(Row::from(vec!["a", "a"])),
                Ok(Row::from(vec!["b", "a"])),
                Ok(Row::from(vec!["c", "a"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let re = Reduce::new(iter, Vec::new(), Vec::new()).unwrap();
        let r = re.into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["a", "a"]),
                Row::from(vec!["b", "a"]),
                Row::from(vec!["c", "a"]),
            ]
        );
    }

    #[test]
    fn test_reduce_avg() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = Reduce::new(iter, vec!["a"], vec!["new:avg:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "3"]),
                Row::from(vec!["2", "9", "8"]),
            ]
        );
    }

    #[test]
    fn test_reduce_min() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = Reduce::new(iter, vec!["a"], vec!["new:min:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "2"]),
                Row::from(vec!["2", "9", "7"]),
            ]
        );
    }

    #[test]
    fn test_reduce_max() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = Reduce::new(iter, vec!["a"], vec!["new:max:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "4"]),
                Row::from(vec!["2", "9", "9"]),
            ]
        );
    }

    #[test]
    fn test_reduce_sum() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = Reduce::new(iter, vec!["a"], vec!["new:sum:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "6"]),
                Row::from(vec!["2", "9", "16"]),
            ]
        );
    }

    #[test]
    fn test_reduce_error() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Err(Error::ColBuildError(col::BuildError::InvalidFormat)),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let mut r = Reduce::new(iter, vec!["a"], vec!["new:sum:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        match r.next().unwrap().unwrap_err() {
            Error::ColBuildError(col::BuildError::InvalidFormat) => {}
            _ => panic!("didn't expect this"),
        }

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "6"]),
                Row::from(vec!["2", "9", "16"]),
            ]
        );
    }

    #[test]
    fn test_hash() {
        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string()];

        assert_eq!(
            hash_row(&header, &data, &cols).unwrap(),
            16569625464242099095
        );

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string(), "b".to_string()];

        assert_eq!(
            hash_row(&header, &data, &cols).unwrap(),
            15633344752900483833
        );

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["d".to_string()];

        assert_eq!(
            hash_row(&header, &data, &cols),
            Err(HashError("d".to_string()))
        );
    }
}
