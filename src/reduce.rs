use crate::{Headers, RowResult, RowStream};
use std::rc::Rc;
use std::vec;

pub mod aggregate;
pub mod group;
pub mod aggregated_col;

use aggregate::Aggregate;
use group::Group;
use aggregated_col::AggregatedCol;

/// Kinds of errors that can happen when building a Reduce processor.
#[derive(Debug)]
pub enum BuildError {
    DuplicatedHeader(String),
}

/// Used to group and aggregate the rows, yielding the results as a new stream
/// of rows with potentially new columns.
pub struct Reduce<I> {
    iter: I,
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
        columns: Vec<AggregatedCol>,
    ) -> Result<Reduce<I>, BuildError> {
        let mut headers = iter.headers().clone();
        let mut whole_columns = Vec::with_capacity(headers.len() + columns.len());

        for header in headers.iter() {
            let source = Rc::new(header.to_string());

            whole_columns.push(AggregatedCol::new(
                header,
                Box::new(aggregate::Last::new(&[&source]).unwrap()),
            ));
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
            columns: whole_columns,
            headers,
        })
    }
}

pub struct IntoIter {
    iter: vec::IntoIter<RowResult>,
}

impl Iterator for IntoIter
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I> IntoIterator for Reduce<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut errors = vec![];
        let mut onlygroup = Group::from(&self.columns);

        for item in self.iter {
            match item {
                Ok(row) => onlygroup.update(&self.headers, &row),
                Err(e) => errors.push(Err(e)),
            }
        }

        errors.push(Ok(onlygroup.as_row()));

        IntoIter {
            iter: errors.into_iter(),
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
    use super::{Reduce};
    use crate::{Row, Error, col, mock::MockStream};

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

        let re = Reduce::new(iter, Vec::new()).unwrap();
        let r = re.into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
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

        let r = Reduce::new(iter, vec!["new:avg:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["2", "9", "5.5"]),
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

        let r = Reduce::new(iter, vec!["new:min:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["2", "9", "2"]),
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

        let r = Reduce::new(iter, vec!["new:max:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
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

        let r = Reduce::new(iter, vec!["new:sum:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let mut results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        results.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));

        assert_eq!(
            results,
            vec![
                Row::from(vec!["2", "9", "22"]),
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

        let mut r = Reduce::new(iter, vec!["new:sum:b".parse().unwrap()])
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
                Row::from(vec!["2", "9", "22"]),
            ]
        );
    }
}
