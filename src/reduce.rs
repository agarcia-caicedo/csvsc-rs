use crate::{Error, Headers, Row, RowResult, RowStream};
use std::collections::hash_map::{self, DefaultHasher};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::str::FromStr;

pub mod aggregate;
pub mod group;

use aggregate::Aggregate;
use group::Group;

#[derive(Debug)]
pub enum ReduceBuildError {
    GroupingKeyError(String),
    AggregateSourceError(String),
    DuplicatedHeader(String),
}

#[derive(Debug, PartialEq)]
pub struct HashError(String);

#[derive(Debug)]
pub enum ConsumeError {
    KeyError(String),
}

impl From<HashError> for ConsumeError {
    fn from(column: HashError) -> ConsumeError {
        ConsumeError::KeyError(column.0)
    }
}

/// Creates a hash for the data described by the three arguments as follows:
/// `columns` are used to locate the values in `row` as specified by `headers`.
///
/// If no columns are specified, a random hash is chosen.
pub fn hash_row(headers: &Headers, row: &Row, columns: &[String]) -> Result<u64, HashError> {
    if columns.len() == 0 {
        return Ok(rand::random());
    }

    let mut hasher = DefaultHasher::new();

    for col in columns {
        match headers.get_field(row, col) {
            Some(field) => field.hash(&mut hasher),
            None => return Err(HashError(col.to_string())),
        }
    }

    Ok(hasher.finish())
}

#[derive(Debug)]
pub enum AggregatedColParseError {
    /// The specified string is not composed of exactly three words separated
    /// by two colons
    NotEnoughParts,
    UnknownAggregate(String),
}

#[derive(Clone)]
pub struct AggregatedCol {
    colname: String,
    source: Rc<String>,
    aggregate: Box<dyn Aggregate>,
}

impl AggregatedCol {
    pub fn new(colname: &str, source: Rc<String>, aggregate: Box<dyn Aggregate>) -> AggregatedCol {
        AggregatedCol {
            colname: colname.to_string(),
            source,
            aggregate,
        }
    }

    pub fn colname(&self) -> &str {
        &self.colname
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    fn aggregate(&self) -> &Box<dyn Aggregate> {
        &self.aggregate
    }
}

impl FromStr for AggregatedCol {
    type Err = AggregatedColParseError;

    fn from_str(def: &str) -> Result<AggregatedCol, Self::Err> {
        let pieces: Vec<&str> = def.split(':').collect();

        if pieces.len() != 3 {
            return Err(AggregatedColParseError::NotEnoughParts);
        }

        let source = Rc::new(pieces[2].to_string());

        Ok(AggregatedCol {
            colname: pieces[0].to_string(),
            aggregate: match pieces[1] {
                "sum" => Box::new(aggregate::Sum::new(Rc::clone(&source))),
                "last" => Box::new(aggregate::Last::new(Rc::clone(&source))),
                "avg" => Box::new(aggregate::Avg::new(Rc::clone(&source))),
                "min" => Box::new(aggregate::Min::new(Rc::clone(&source))),
                "max" => Box::new(aggregate::Max::new(Rc::clone(&source))),
                s => return Err(AggregatedColParseError::UnknownAggregate(s.to_string())),
            },
            source,
        })
    }
}

/// Used to aggregate the rows, yielding the results as a new stream of rows.
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
    pub fn new(
        iter: I,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<Reduce<I>, ReduceBuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(ReduceBuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        let mut whole_columns = Vec::with_capacity(headers.len() + columns.len());

        for header in headers.iter() {
            let source = Rc::new(header.to_string());

            whole_columns.push(AggregatedCol {
                colname: header.to_string(),
                source: Rc::clone(&source),
                aggregate: Box::new(aggregate::Last::new(Rc::clone(&source))),
            });
        }

        for col in columns.iter() {
            if !headers.contains_key(col.source()) {
                return Err(ReduceBuildError::AggregateSourceError(
                    col.source().to_string(),
                ));
            }
        }

        for col in columns.iter() {
            if let Err(_) = headers.add(col.colname()) {
                return Err(ReduceBuildError::DuplicatedHeader(col.colname().to_string()));
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
                    let row_hash = hash_row(&headers, &item, &self.group_by).unwrap();

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
    use crate::{Row, add::ColBuildError, mock::MockStream};

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
                Err(Error::ColBuildError(ColBuildError::InvalidFormat)),
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
            Error::ColBuildError(ColBuildError::InvalidFormat) => {}
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
