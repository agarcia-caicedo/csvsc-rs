use std::str::FromStr;
use std::collections::HashMap;
use super::{Headers, RowStream, RowResult, Row, get_field};
use std::collections::hash_map::{self, DefaultHasher};
use std::hash::{Hash, Hasher};

mod aggregate;
mod groups;

use aggregate::Aggregate;
use groups::{Group, Groups};

#[derive(Debug)]
pub enum ReducerBuildError {
    KeyError(String),
}

#[derive(Debug,PartialEq)]
struct HashError(String);

#[derive(Debug)]
pub enum ConsumeError {
    KeyError(String),
}

impl From<HashError> for ConsumeError {
    fn from(column: HashError) -> ConsumeError {
        ConsumeError::KeyError(column.0)
    }
}

fn hash(headers: &Headers, row: &Row, columns: &[String]) -> Result<u64, HashError> {
    let mut hasher = DefaultHasher::new();

    for col in columns {
        match get_field(headers, row, col) {
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
    source: String,
    aggregate: Box<dyn Aggregate>,
}

impl AggregatedCol {
    fn colname(&self) -> &str {
        &self.colname
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

        Ok(AggregatedCol {
            colname: pieces[0].to_string(),
            aggregate: Box::new(match pieces[1] {
                "sum" => aggregate::Sum::new(),
                s => return Err(AggregatedColParseError::UnknownAggregate(s.to_string())),
            }),
            source: pieces[2].to_string(),
        })
    }
}

pub struct Reducer<I> {
    iter: I,
    group_by: Vec<String>,
    columns: Vec<AggregatedCol>,
    headers: Headers,
}

impl<I> Reducer<I>
    where I: Iterator<Item = RowResult> + RowStream
{
    pub fn new(iter: I, grouping: Vec<&str>, columns: Vec<AggregatedCol>) -> Result<Reducer<I>, ReducerBuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(ReducerBuildError::KeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        for col in columns.iter() {
            headers.add(col.colname());
        }

        Ok(Reducer {
            iter,
            group_by,
            columns,
            headers,
        })
    }

    fn groups(self) -> Result<Groups, ConsumeError> {
        let mut groups = HashMap::new();
        let aggregates = self.columns;

        for item in self.iter.filter_map(|c| c.ok()) {
            let item_hash = hash(&self.headers, &item, &self.group_by)?;

            groups.entry(item_hash)
                .and_modify(|group: &mut Group| {
                    group.update(&item);
                })
                .or_insert_with(|| {
                    let mut g = Group::from(&aggregates);

                    g.update(&item);

                    g
                });
        }

        Ok(Groups::from(groups))
    }
}

impl<I> RowStream for Reducer<I> {
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{Reducer, Headers, hash, HashError};
    use crate::mock::MockStream;
    use crate::Row;

    #[test]
    fn test_reducer_id_function() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["name", "_target"])),
            Ok(Row::from(vec!["a", "a"])),
            Ok(Row::from(vec!["b", "a"])),
            Ok(Row::from(vec!["c", "a"])),
        ].into_iter()).unwrap();

        let mut r = Reducer::new(iter, Vec::new(), Vec::new()).unwrap().groups().unwrap();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["a"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["b"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["c"]));
    }

    #[test]
    fn test_reducer_avg() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["a", "b", "_target"])),
            Ok(Row::from(vec!["1", "2", "a"])),
            Ok(Row::from(vec!["1", "4", "a"])),
            Ok(Row::from(vec!["2", "7", "a"])),
            Ok(Row::from(vec!["2", "9", "a"])),
        ].into_iter()).unwrap();

        let mut r = Reducer::new(iter, vec!["0"], vec!["new:avg:1".parse().unwrap()]).unwrap().groups().unwrap();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2", "3.0"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "7", "8.0"]));
    }

    #[test]
    fn test_reducer_min() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["a", "b", "_target"])),
            Ok(Row::from(vec!["1", "2", "a"])),
            Ok(Row::from(vec!["1", "4", "a"])),
            Ok(Row::from(vec!["2", "7", "a"])),
            Ok(Row::from(vec!["2", "9", "a"])),
        ].into_iter()).unwrap();

        let mut r = Reducer::new(iter, vec!["0"], vec!["new:min:1".parse().unwrap()]).unwrap().groups().unwrap();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2", "2.0"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "7", "7.0"]));
    }

    #[test]
    fn test_reducer_max() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["a", "b", "_target"])),
            Ok(Row::from(vec!["1", "2", "a"])),
            Ok(Row::from(vec!["1", "4", "a"])),
            Ok(Row::from(vec!["2", "7", "a"])),
            Ok(Row::from(vec!["2", "9", "a"])),
        ].into_iter()).unwrap();

        let mut r = Reducer::new(iter, vec!["0"], vec!["new:max:1".parse().unwrap()]).unwrap().groups().unwrap();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2", "4.0"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "7", "9.0"]));
    }

    #[test]
    fn test_reducer_sum() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["a", "b", "_target"])),
            Ok(Row::from(vec!["1", "2", "a"])),
            Ok(Row::from(vec!["1", "4", "a"])),
            Ok(Row::from(vec!["2", "7", "a"])),
            Ok(Row::from(vec!["2", "9", "a"])),
        ].into_iter()).unwrap();

        let mut r = Reducer::new(iter, vec!["0"], vec!["new:sum:1".parse().unwrap()]).unwrap().groups().unwrap();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2", "6.0"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "7", "16.0"]));
    }

    #[test]
    fn test_hash() {
        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string()];

        assert_eq!(hash(&header, &data, &cols).unwrap(), 16569625464242099095);

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["a".to_string(), "b".to_string()];

        assert_eq!(hash(&header, &data, &cols).unwrap(), 15633344752900483833);

        let header = Headers::from_row(Row::from(vec!["a", "b"]));
        let data = Row::from(vec!["1", "2"]);
        let cols = vec!["d".to_string()];

        assert_eq!(hash(&header, &data, &cols), Err(HashError("d".to_string())));
    }
}
