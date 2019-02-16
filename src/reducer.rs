use std::str::FromStr;
use std::collections::HashMap;
use super::{Headers, RowStream, RowResult, Row};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub enum AggregateParseError {
}

pub enum Aggregate {
    Sum{
        colname: String,
    },
}

impl FromStr for Aggregate {
    type Err = AggregateParseError;

    fn from_str(spec: &str) -> Result<Aggregate, Self::Err> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum ReducerBuildError {
    KeyError(String),
}

enum ReducerState {
    Processed,
    Unprocessed,
}

pub struct Reducer<I> {
    iter: I,
    group_by: Vec<String>,
    columns: Vec<Aggregate>,
    headers: Headers,
}

impl<I> Reducer<I>
    where I: Iterator<Item = RowResult> + RowStream
{
    pub fn new(iter: I, grouping: Vec<&str>, columns: Vec<Aggregate>) -> Result<Reducer<I>, ReducerBuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(ReducerBuildError::KeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        for col in columns.iter() {
            headers.add(match col {
                Aggregate::Sum{colname, ..} => colname,
            });
        }

        Ok(Reducer {
            iter,
            group_by,
            columns,
            headers,
        })
    }
}

impl<I> RowStream for Reducer<I> {
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

fn hash(headers: &Headers, row: &Row, columns: &[String]) -> u64 {
    unimplemented!()
}

struct Group {
}

impl Group {
    fn update(&mut self, row: &Row) {
        unimplemented!()
    }
}

impl From<Row> for Group {
    fn from(row: Row) -> Group {
        unimplemented!()
    }
}

impl<I> IntoIterator for Reducer<I>
    where I: Iterator<Item = RowResult>
{
    type Item = RowResult;
    type IntoIter = IntoIter;

    /// Calling this triggers consumption of the source iterator and consumes
    /// the reducer itself, yielding an interator over the aggregated results.
    fn into_iter(self) -> Self::IntoIter {
        let mut groups = HashMap::new();

        for item in self.iter.filter_map(|c| c.ok()) {
            let item_hash = hash(&self.headers, &item, &self.group_by);

            groups.entry(item_hash)
                .and_modify(|group: &mut Group| {
                    group.update(&item);
                })
                .or_insert_with(|| {
                    Group::from(item)
                });
        }

        IntoIter::from(groups)
    }
}

pub struct IntoIter {
}

impl From<HashMap<u64, Group>> for IntoIter {
    fn from(data: HashMap<u64, Group>) -> IntoIter {
        unimplemented!()
    }
}

impl Iterator for IntoIter {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::Reducer;
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

        let mut r = Reducer::new(iter, Vec::new(), Vec::new()).unwrap().into_iter().into_iter();

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

        let mut r = Reducer::new(iter, vec!["0"], vec!["avg:1".parse().unwrap()]).unwrap().into_iter();

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

        let mut r = Reducer::new(iter, vec!["0"], vec!["min:1".parse().unwrap()]).unwrap().into_iter();

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

        let mut r = Reducer::new(iter, vec!["0"], vec!["max:1".parse().unwrap()]).unwrap().into_iter();

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

        let mut r = Reducer::new(iter, vec!["0"], vec!["sum:1".parse().unwrap()]).unwrap().into_iter();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2", "6.0"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "7", "16.0"]));
    }
}
