use std::vec;
use crate::{
    RowStream, Headers, RowResult,
    mock::MockStream,
};

pub struct AdjacentGroup<I, F> {
    iter: I,
    f: F,
    headers: Headers,
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
    ) -> AdjacentGroup<I, F>
    where
        H: FnMut(Headers) -> Headers,
    {
        let headers = (header_map)(iter.headers().clone());

        AdjacentGroup {
            iter,
            f,
            headers,
        }
    }
}

pub struct IntoIter<I, F> {
    iter: I,
    f: F,
    headers: Headers,
}

impl<I, F, R> Iterator for IntoIter<I, F>
where
    I: Iterator<Item = RowResult>,
    F: FnMut(MockStream<vec::IntoIter<RowResult>>) -> R,
    R: RowStream,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
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
            iter: self.iter.into_iter(),
            f: self.f,
            headers: self.headers,
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
            headers.add("value");

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
        });

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
}
