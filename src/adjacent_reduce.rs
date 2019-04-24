use std::rc::Rc;
use crate::{
    Error, Headers, Row, RowResult, RowStream,
    reduce::{
        aggregate, ReduceBuildError, AggregatedCol, hash_row,
        group::Group,
    },
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
    aggregates: Vec<AggregatedCol>,
    headers: Headers,
}

impl<I> AdjacentReduce<I>
where
    I: RowStream,
{
    pub fn new(
        iter: I,
        grouping: Vec<&str>,
        aggregates: Vec<AggregatedCol>,
    ) -> Result<AdjacentReduce<I>, ReduceBuildError> {
        let mut headers = iter.headers().clone();
        let mut group_by = Vec::with_capacity(grouping.len());

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(ReduceBuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        let mut whole_columns = Vec::with_capacity(headers.len() + aggregates.len());

        for header in headers.iter() {
            let source = Rc::new(header.to_string());

            whole_columns.push(AggregatedCol::new(
                header,
                Rc::clone(&source),
                Box::new(aggregate::Last::new(Rc::clone(&source))),
            ));
        }

        for col in aggregates.iter() {
            if !headers.contains_key(col.source()) {
                return Err(ReduceBuildError::AggregateSourceError(
                    col.source().to_string(),
                ));
            }
        }

        for col in aggregates.iter() {
            headers.add(col.colname());
        }

        for column in aggregates {
            whole_columns.push(column);
        }

        Ok(AdjacentReduce {
            iter,
            group_by,
            aggregates: whole_columns,
            headers,
        })
    }
}

pub struct IntoIter<I> {
    current_group: Option<Result<(u64, Group), Error>>,
    headers: Headers,
    group_by: Vec<String>,
    aggregates: Vec<AggregatedCol>,
    iter: I,
}

impl<I> IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    /// places the next available value of the source iterator as current value
    fn place_next(&mut self) {
        self.current_group = self.iter.next().map(|result| result.map(|row| {
            let row_hash = hash_row(&self.headers, &row, &self.group_by).unwrap();
            let mut g = Group::from(&self.aggregates);

            g.update(&self.headers, &row);

            (row_hash, g)
        }));
    }

    /// places the given row as the current group
    fn place_row(&mut self, hash: u64, row: &Row) {
        let mut g = Group::from(&self.aggregates);

        g.update(&self.headers, &row);

        self.current_group = Some(Ok((hash, g)));
    }

    fn place_err(&mut self, error: Error) {
        self.current_group = Some(Err(error));
    }

    fn place_none(&mut self) {
        self.current_group = None;
    }
}

impl<I> Iterator for IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.current_group.take().map(|result| match result {
            Ok((hash, mut group)) => {
                loop {
                    match self.iter.next() {
                        Some(Ok(row)) => {
                            let row_hash = hash_row(&self.headers, &row, &self.group_by).unwrap();

                            if row_hash == hash {
                                group.update(&self.headers, &row);
                            } else {
                                self.place_row(row_hash, &row);

                                break Ok(group.as_row());
                            }
                        },
                        Some(Err(e)) => {
                            self.place_err(e);

                            break Ok(group.as_row());
                        },
                        None => {
                            self.place_none();

                            break Ok(group.as_row());
                        },
                    }
                }
            },
            Err(e) => {
                self.place_next();

                Err(e)
            },
        })
    }
}

impl<I> IntoIterator for AdjacentReduce<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        let aggregates = self.aggregates;
        let headers = self.headers;
        let group_by = self.group_by;
        let mut iter = self.iter.into_iter();

        let current_group = iter.next().map(|result| result.map(|row| {
            let row_hash = hash_row(&headers, &row, &group_by).unwrap();
            let mut g = Group::from(&aggregates);

            g.update(&headers, &row);

            (row_hash, g)
        }));

        IntoIter {
            current_group,
            headers,
            group_by,
            aggregates,
            iter,
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
    use crate::{
        Row,
        mock::MockStream,
        error::Error,
        add::ColBuildError,
    };

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
    fn test_reduce_avg() {
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
    fn test_reduce_min() {
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
    fn test_reduce_max() {
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
    fn test_reduce_sum() {
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

    #[test]
    fn test_reduce_errors() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Err(Error::ColBuildError(ColBuildError::InvalidFormat)),
                Err(Error::ColBuildError(ColBuildError::UnknownSource)),
                Ok(Row::from(vec!["1", "4"])),
                Ok(Row::from(vec!["2", "7"])),
                Ok(Row::from(vec!["2", "9"])),
                Ok(Row::from(vec!["1", "4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let mut r = AdjacentReduce::new(iter, vec!["a"], vec![])
            .unwrap()
            .into_iter();

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "2"]));

        match r.next().unwrap().unwrap_err() {
            Error::ColBuildError(ColBuildError::InvalidFormat) => {},
            _ => panic!("failed"),
        }

        match r.next().unwrap().unwrap_err() {
            Error::ColBuildError(ColBuildError::UnknownSource) => {},
            _ => panic!("failed"),
        }

        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "4"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["2", "9"]));
        assert_eq!(r.next().unwrap().unwrap(), Row::from(vec!["1", "4"]));

        assert!(r.next().is_none());
    }
}
