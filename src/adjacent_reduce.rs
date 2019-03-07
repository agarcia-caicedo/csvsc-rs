use super::{Error, Headers, RowResult, RowStream};
use std::collections::hash_map;
use std::collections::HashMap;
use std::rc::Rc;
use crate::reducer::{
    aggregate, group::Group, ReducerBuildError, AggregatedCol, hash_row
};

/// This reducer assumes that the grouping criteria will match contiguous groups
/// in the original data, aplying reducers to them and yielding the results when
/// a different group is found.
///
/// A group with the same hash can be found later in the data and it's going to
/// be treated as a different group if it's not contiguous to a previous one.
pub struct AdjacentReduce<I> {
    iter: I,
    group_by: Vec<String>,
    columns: Vec<AggregatedCol>,
    headers: Headers,
}

impl<I> AdjacentReduce<I>
where
    I: RowStream,
{
    pub fn new(
        iter: I,
        grouping: Vec<&str>,
        columns: Vec<AggregatedCol>,
    ) -> Result<AdjacentReduce<I>, ReducerBuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(ReducerBuildError::GroupingKeyError(key.to_string()));
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
                return Err(ReducerBuildError::AggregateSourceError(
                    col.source().to_string(),
                ));
            }
        }

        for col in columns.iter() {
            headers.add(col.colname());
        }

        for column in columns {
            whole_columns.push(column);
        }

        Ok(AdjacentReduce {
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

impl<I> IntoIterator for AdjacentReduce<I>
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

impl<I> RowStream for AdjacentReduce<I>
where
    AdjacentReduce<I>: IntoIterator<Item = RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::AdjacentReduce;
    use crate::mock::MockStream;
    use crate::Row;

    #[test]
    fn test_reducer_id_function() {
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

        let re = AdjacentReduce::new(iter, Vec::new(), Vec::new()).unwrap();
        let r = re.into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

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
    fn test_reducer_avg() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
                Ok(Row::from(vec!["1", "4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = AdjacentReduce::new(iter, vec!["a"], vec!["new:avg:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "3"]),
                Row::from(vec!["2", "9", "8"]),
                Row::from(vec!["1", "4", "4"]),
            ]
        );
    }

    #[test]
    fn test_reducer_min() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
                Ok(Row::from(vec!["1", "4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = AdjacentReduce::new(iter, vec!["a"], vec!["new:min:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "2"]),
                Row::from(vec!["2", "9", "7"]),
                Row::from(vec!["1", "4", "4"]),
            ]
        );
    }

    #[test]
    fn test_reducer_max() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
                Ok(Row::from(vec!["1", "4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = AdjacentReduce::new(iter, vec!["a"], vec!["new:max:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "4"]),
                Row::from(vec!["2", "9", "9"]),
                Row::from(vec!["1", "4", "4"]),
            ]
        );
    }

    #[test]
    fn test_reducer_sum() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
                Ok(Row::from(vec!["1", "4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let r = AdjacentReduce::new(iter, vec!["a"], vec!["new:sum:b".parse().unwrap()])
            .unwrap()
            .into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["1", "4", "6"]),
                Row::from(vec!["2", "9", "16"]),
                Row::from(vec!["1", "4", "4"]),
            ]
        );
    }
}
