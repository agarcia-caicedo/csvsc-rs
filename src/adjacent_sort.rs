use std::iter::Peekable;
use crate::{
    Row, RowResult, RowStream, Headers, get_field,
    reduce::hash_row,
};

#[derive(Debug)]
pub enum AdjacentSortBuildError {
    GroupingKeyError(String),
    SortKeyError(String),
}

pub struct AdjacentSort<I> {
    iter: I,
    group_by: Vec<String>,
    sort_by: String,
    headers: Headers,
}

impl<I> AdjacentSort<I>
where
    I: RowStream,
{
    pub fn new(
        iter: I,
        grouping: &[&str],
        sort_by: &str,
    ) -> Result<AdjacentSort<I>, AdjacentSortBuildError> {
        let mut group_by = Vec::with_capacity(grouping.len());
        let headers = iter.headers().clone();

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(AdjacentSortBuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        if !iter.headers().contains_key(sort_by) {
            return Err(AdjacentSortBuildError::SortKeyError(sort_by.to_string()));
        }

        Ok(AdjacentSort {
            iter,
            group_by,
            sort_by: sort_by.to_string(),
            headers,
        })
    }
}

pub struct IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    iter: Peekable<I>,
    current_group: std::vec::IntoIter<Row>,
    sort_by: String,
    group_by: Vec<String>,
    headers: Headers,
}

impl<I> Iterator for IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        let current_next = self.current_group.next().map(|r| Ok(r));

        match current_next {
            Some(result) => Some(result),
            None => {
                let mut new_group: Vec<Row> = Vec::new();
                let mut new_hash: Option<u64> = None;

                loop {
                    let cur = self.iter.peek();

                    match cur {
                        Some(Ok(row)) => {
                            let row_hash = hash_row(&self.headers, &row, &self.group_by).unwrap();
                            new_hash.get_or_insert(row_hash);

                            if row_hash == *new_hash.as_ref().unwrap() {
                                new_group.push(self.iter.next().unwrap().unwrap());
                            } else {
                                break;
                            }
                        },
                        // TODO errors are returned as they are found
                        _ => break,
                    }
                }

                new_group.sort_unstable_by(|r1, r2| {
                    get_field(&self.headers, r1, &self.sort_by).unwrap().cmp(get_field(&self.headers, r2, &self.sort_by).unwrap())
                });

                self.current_group = new_group.into_iter();
                self.current_group.next().map(|r| Ok(r))
            }
        }
    }
}

impl<I> IntoIterator for AdjacentSort<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        let mut iter = self.iter.into_iter().peekable();
        let headers = self.headers;
        let group_by = self.group_by;
        let sort_by = self.sort_by;
        let mut current_group: Vec<Row> = Vec::new();
        let mut current_hash: Option<u64> = None;

        loop {
            let cur = iter.peek();

            match cur {
                Some(Ok(row)) => {
                    let row_hash = hash_row(&headers, &row, &group_by).unwrap();
                    current_hash.get_or_insert(row_hash);

                    if row_hash == *current_hash.as_ref().unwrap() {
                        current_group.push(iter.next().unwrap().unwrap());
                    } else {
                        break;
                    }
                },
                _ => break,
            }
        }

        current_group.sort_unstable_by(|r1, r2| {
            get_field(&headers, r1, &sort_by).unwrap().cmp(get_field(&headers, r2, &sort_by).unwrap())
        });

        IntoIter {
            current_group: current_group.into_iter(),
            headers,
            sort_by,
            group_by,
            iter,
        }
    }
}

impl<I> RowStream for AdjacentSort<I>
where
    AdjacentSort<I>: IntoIterator<Item = RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{AdjacentSort, Row, Headers, RowStream};
    use crate::mock::MockStream;

    #[test]
    fn test_sort() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "1"])),
                Ok(Row::from(vec!["1", "3"])),
                Ok(Row::from(vec!["2", "3"])),
                Ok(Row::from(vec!["2", "2"])),
                Ok(Row::from(vec!["2", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let add = AdjacentSort::new(iter, &["a"], "b").unwrap();

        assert_eq!(
            *add.headers(),
            Headers::from_row(Row::from(vec!["a", "b"])),
        );

        let mut add= add.into_iter();

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "2"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "3"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "2"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "3"])
        );
    }

    #[test]
    fn test_sort_with_error() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a", "b"])),
                Ok(Row::from(vec!["1", "2"])),
                Ok(Row::from(vec!["1", "1"])),
                Ok(Row::from(vec!["1", "3"])),
                Ok(Row::from(vec!["2", "3"])),
                Ok(Row::from(vec!["2", "2"])),
                Ok(Row::from(vec!["2", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let add = AdjacentSort::new(iter, &["a"], "b").unwrap();

        assert_eq!(
            *add.headers(),
            Headers::from_row(Row::from(vec!["a", "b"])),
        );

        let mut add= add.into_iter();

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "2"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "3"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "2"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "3"])
        );
    }
}
