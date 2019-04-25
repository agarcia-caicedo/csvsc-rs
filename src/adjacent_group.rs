use std::vec;
use std::iter::Peekable;
use crate::{
    RowStream, Headers, RowResult,
    mock::MockStream,
    reduce::hash_row,
};

#[derive(Debug)]
pub enum AdjacentGroupBuildError {
    GroupingKeyError(String),
    SortKeyError(String),
}

pub struct AdjacentGroup<I, F> {
    iter: I,
    f: F,
    headers: Headers,
    group_by: Vec<String>,
}

impl<I, F, R> AdjacentGroup<I, F>
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
    ) -> Result<AdjacentGroup<I, F>, AdjacentGroupBuildError>
    where
        H: FnMut(Headers) -> Headers,
    {
        let mut group_by = Vec::with_capacity(grouping.len());
        let headers = iter.headers().clone();

        for key in grouping.iter() {
            if !headers.contains_key(key) {
                return Err(AdjacentGroupBuildError::GroupingKeyError(key.to_string()));
            }

            group_by.push(key.to_string());
        }

        let headers = (header_map)(headers);

        Ok(AdjacentGroup {
            iter,
            f,
            headers,
            group_by,
        })
    }
}

pub struct IntoIter<I, F>
where
    I: Iterator<Item=RowResult>,
{
    iter: Peekable<I>,
    f: F,
    headers: Headers,
    current_group: Option<I>,
    group_by: Vec<String>,
}

impl<I, F, R> Iterator for IntoIter<I, F>
where
    I: Iterator<Item = RowResult>,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        // * I have an already processed group
        //   - Next item of group is some
        //      + return it
        //   - Next item of group is None
        //      + dispose group
        //      + recursive call
        // * There is no group
        //   - Next item is None
        //      + return None
        //   - Next item is Some
        //      + build a group
        //      + store it
        //      + recursive call
        match self.current_group.as_mut() {
            Some(group) => match group.next() {
                Some(item) => Some(item),
                None => {
                    self.current_group = None;

                    self.next()
                },
            },
            None => match self.iter.peek() {
                None => None,
                Some(Ok(_)) => {
                    let first_row = self.iter.next().unwrap().unwrap();
                    let current_hash = hash_row(&self.headers, &first_row, &self.group_by).unwrap();
                    let current_group = vec![first_row];

                    self.next()
                },
                Some(Err(_)) => unimplemented!(),
            },
        }
    }
}

impl<I, F, R> IntoIterator for AdjacentGroup<I, F>
where
    I: RowStream,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter, F>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.iter.into_iter().peekable(),
            f: self.f,
            headers: self.headers,
            group_by: self.group_by,
            current_group: None,
        }
    }
}

impl<I, F, R> RowStream for AdjacentGroup<I, F>
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
        get_field,
        mock::MockStream
    };
    use super::AdjacentGroup;

    #[test]
    fn test_adjacent_group() {
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

        let re = AdjacentGroup::new(iter, |mut headers| {
            headers.add("sum").unwrap();

            headers
        }, |row_stream| {
            let headers = row_stream.headers().clone();
            let rows: Vec<_> = row_stream.into_iter().collect();
            let mut sum = 0.0;

            for row in rows.iter() {
                let value: f64 = get_field(
                    &headers,
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
        unimplemented!()
    }

    #[test]
    fn test_some_errs_in_stream() {
        unimplemented!()
    }
}
