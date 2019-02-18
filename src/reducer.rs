use std::str::FromStr;
use std::collections::HashMap;
use super::{Headers, RowStream, RowResult, Row, get_field};
use std::collections::hash_map::{self, DefaultHasher};
use std::hash::{Hash, Hasher};

mod aggregate;

use aggregate::Aggregate;

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

struct Group {
    contents: Vec<Box<dyn Aggregate>>,
}

impl Group {
    fn update(&mut self, row: &Row) {
        for (agg, data) in self.contents.iter_mut().zip(row.iter()) {
            agg.update(data);
        }
    }

    fn as_row(self) -> Row {
        unimplemented!()
    }
}

impl<'a> From<&'a Vec<Box<dyn Aggregate>>> for Group {
    fn from(row: &'a Vec<Box<dyn Aggregate>>) -> Group {
        let mut contents = Vec::with_capacity(row.len());

        for item in row {
            contents.push((*item).clone());
        }

        Group {
            contents,
        }
    }
}

struct Groups {
    groups: hash_map::IntoIter<u64, Group>,
}

impl From<HashMap<u64, Group>> for Groups {
    fn from(data: HashMap<u64, Group>) -> Groups {
        Groups {
            groups: data.into_iter(),
        }
    }
}

impl Iterator for Groups {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.groups.next().map(|g| {
            Ok(g.1.as_row())
        })
    }
}

pub struct Reducer<I> {
    iter: I,
    group_by: Vec<String>,
    columns: Vec<Box<dyn Aggregate>>,
    headers: Headers,
}

impl<I> Reducer<I>
    where I: Iterator<Item = RowResult> + RowStream
{
    pub fn new(iter: I, grouping: Vec<&str>, columns: Vec<Box<dyn Aggregate>>) -> Result<Reducer<I>, ReducerBuildError> {
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
    use super::aggregate::parse;

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

        let mut r = Reducer::new(iter, vec!["0"], vec![parse("avg:1").unwrap()]).unwrap().groups().unwrap();

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

        let mut r = Reducer::new(iter, vec!["0"], vec![parse("min:1").unwrap()]).unwrap().groups().unwrap();

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

        let mut r = Reducer::new(iter, vec!["0"], vec![parse("max:1").unwrap()]).unwrap().groups().unwrap();

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

        let mut r = Reducer::new(iter, vec!["0"], vec![parse("sum:1").unwrap()]).unwrap().groups().unwrap();

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
