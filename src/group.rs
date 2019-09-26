use std::vec;
use std::iter::Peekable;
use std::collections::{HashMap, hash_map};
use crate::{
    RowStream, Headers, RowResult, GroupBuildError, Row,
    mock::MockStream,
    error::Error,
};

/// Groups data by a set of columns.
///
/// The groups are passed to you as a RowStream
/// object that you can use to manipulate them. Data is group globally from the
/// stream so if the stream is huge you'll have all the rows in memory at the
/// same time.
///
/// If you add or delete headers you're responsible for modifying the headers also,
/// which will be given to you as a parameter of the first closure.
pub struct Group<I, F> {
    iter: I,
    f: F,
    headers: Headers,
    old_headers: Headers,
    group_by: Vec<String>,
}

impl<I, F, R> Group<I, F>
where
    I: RowStream,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    pub fn new<H>(
        iter: I,
        mut header_map: H,
        f: F,
        grouping: &[&str],
    ) -> Result<Group<I, F>, GroupBuildError>
    where
        H: FnMut(Headers) -> Headers,
    {
        let mut group_by = Vec::with_capacity(grouping.len());
        let old_headers = iter.headers().clone();
        let headers = old_headers.clone();

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(GroupBuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        let headers = (header_map)(headers);

        Ok(Group {
            iter,
            f,
            headers,
            old_headers,
            group_by,
        })
    }
}

#[derive(Hash,PartialEq,Eq)]
enum GroupKey {
    Rows(u64),
    Errors,
}

pub struct IntoIter<F, R>
where
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    iter: hash_map::IntoIter<GroupKey, Vec<RowResult>>,
    f: F,
    headers: Headers,
    old_headers: Headers,
    current_group: Option<R::IntoIter>,
}

impl<F, R> Iterator for IntoIter<F, R>
where
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_group.as_mut() {
            Some(group) => match group.next() {
                Some(item) => Some(item),
                None => {
                    self.current_group = None;

                    self.next()
                },
            },
            None => match self.iter.next() {
                None => None,
                Some((key, vec)) => {
                    let output_stream = (self.f)(
                        MockStream::new(vec.into_iter(), self.old_headers.clone())
                    );

                    if *output_stream.headers() != self.headers {
                        return Some(Err(Error::InconsistentHeaders));
                    }

                    self.current_group = Some(output_stream.into_iter());

                    self.next()
                },
            },
        }
    }
}

impl<I, F, R> IntoIterator for Group<I, F>
where
    I: RowStream,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<F, R>;

    fn into_iter(self) -> Self::IntoIter {
        let mut groups = HashMap::new();
        let iter = self.iter.into_iter();

        for result in iter {
            match result {
                Ok(item) => {
                    let row_hash = self.headers.hash(&item, &self.group_by).unwrap();

                    groups
                        .entry(GroupKey::Rows(row_hash))
                        .or_insert(vec![])
                        .push(Ok(item));
                }
                Err(error) => {
                    groups
                        .entry(GroupKey::Errors)
                        .or_insert(vec![])
                        .push(Err(error));
                }
            }
        }

        IntoIter {
            iter: groups.into_iter(),
            f: self.f,
            headers: self.headers,
            old_headers: self.old_headers,
            current_group: None,
        }
    }
}

impl<I, F, R> RowStream for Group<I, F>
where
    I: RowStream,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Headers,
        Row, RowStream,
        mock::MockStream,
        error::Error,
    };
    use super::Group;

    #[test]
    fn test_group() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["name", "value"])),
                Ok(Row::from(vec!["a", "1"])),
                Ok(Row::from(vec!["a", "2"])),
                Ok(Row::from(vec!["a", "3"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["b", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let re = Group::new(iter, |mut headers| {
            headers.add("sum").unwrap();

            headers
        }, |row_stream| {
            let headers = row_stream.headers().clone();
            let rows: Vec<_> = row_stream.into_iter().collect();
            let mut sum = 0.0;

            for row in rows.iter() {
                let value: f64 = headers.get_field(
                    &row.as_ref().unwrap(),
                    "value"
                ).unwrap().parse().unwrap();

                sum += value;
            }

            MockStream::new(rows.into_iter(), headers)
                .add(vec![format!("value:sum:{}", sum).parse().unwrap()])
                .unwrap()
        }, &["name"]).unwrap();

        assert_eq!(
            *re.headers(),
            Headers::from_row(Row::from(vec!["name", "value", "sum"])),
        );

        let r = re.into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["a", "1", "6"]),
                Row::from(vec!["a", "2", "6"]),
                Row::from(vec!["a", "3", "6"]),
                Row::from(vec!["b", "1", "3"]),
                Row::from(vec!["b", "1", "3"]),
                Row::from(vec!["b", "1", "3"]),
            ]
        );
    }

    #[test]
    fn test_group_noadjacent() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["name", "value"])),
                Ok(Row::from(vec!["a", "1"])),
                Ok(Row::from(vec!["a", "2"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["a", "3"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["b", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let re = Group::new(iter, |mut headers| {
            headers.add("sum").unwrap();

            headers
        }, |row_stream| {
            let headers = row_stream.headers().clone();
            let rows: Vec<_> = row_stream.into_iter().collect();
            let mut sum = 0.0;

            for row in rows.iter() {
                let value: f64 = headers.get_field(
                    &row.as_ref().unwrap(),
                    "value"
                ).unwrap().parse().unwrap();

                sum += value;
            }

            MockStream::new(rows.into_iter(), headers)
                .add(vec![format!("value:sum:{}", sum).parse().unwrap()])
                .unwrap()
        }, &["name"]).unwrap();

        assert_eq!(
            *re.headers(),
            Headers::from_row(Row::from(vec!["name", "value", "sum"])),
        );

        let r = re.into_iter();

        let results: Vec<Row> = r.map(|i| i.unwrap()).collect();

        assert_eq!(
            results,
            vec![
                Row::from(vec!["a", "1", "6"]),
                Row::from(vec!["a", "2", "6"]),
                Row::from(vec!["a", "3", "6"]),
                Row::from(vec!["b", "1", "3"]),
                Row::from(vec!["b", "1", "3"]),
                Row::from(vec!["b", "1", "3"]),
            ]
        );
    }

    #[test]
    fn test_nonmatching_headers() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["name", "value"])),
                Ok(Row::from(vec!["a", "1"])),
                Ok(Row::from(vec!["a", "2"])),
                Ok(Row::from(vec!["a", "3"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["b", "1"])),
                Ok(Row::from(vec!["b", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let re = Group::new(iter, |headers| headers, |row_stream| {
            row_stream
                .add(vec!["value:sum:2".parse().unwrap()])
                .unwrap()
        }, &["name"]).unwrap();

        assert_eq!(
            *re.headers(),
            Headers::from_row(Row::from(vec!["name", "value"])),
        );

        let mut r = re.into_iter();

        match r.next() {
            Some(Err(Error::InconsistentHeaders)) => {},
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_some_errs_in_stream() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["name", "value"])),
                Ok(Row::from(vec!["a", "1"])),
                Err(Error::InconsistentHeaders),
                Ok(Row::from(vec!["b", "1"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let re = Group::new(iter, |headers| headers, |row_stream| row_stream, &["name"]).unwrap();

        assert_eq!(
            *re.headers(),
            Headers::from_row(Row::from(vec!["name", "value"])),
        );

        let mut r = re.into_iter();

        match r.next() {
            Some(Ok(ref r)) if *r == Row::from(vec!["a", "1"]) => {},
            _ => unreachable!(),
        }
        match r.next() {
            Some(Err(Error::InconsistentHeaders)) => {},
            _ => unreachable!(),
        }
        match r.next() {
            Some(Ok(ref r)) if *r == Row::from(vec!["b", "1"]) => {},
            _ => unreachable!(),
        }
        if let Some(_) = r.next() {
            unreachable!()
        }
    }
}
