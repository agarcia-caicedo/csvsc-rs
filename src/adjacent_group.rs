use crate::{RowStream, Headers, RowResult};

pub struct AdjacentGroup<I, F> {
    iter: I,
    f: F,
    headers: Headers,
}

impl<I, F> AdjacentGroup<I, F>
where
    I: RowStream,
    F: FnMut(Vec<RowResult>) -> Vec<RowResult>,
{
    pub fn new(
        iter: I,
        f: F,
    ) -> AdjacentGroup<I, F> {
        let headers = iter.headers().clone();

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

impl<I, F> Iterator for IntoIter<I, F>
where
    I: Iterator<Item = RowResult>,
    F: FnMut(Vec<RowResult>) -> Vec<RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I, F> IntoIterator for AdjacentGroup<I, F>
where
    I: RowStream,
    F: FnMut(Vec<RowResult>) -> Vec<RowResult>,
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

impl<I, F> RowStream for AdjacentGroup<I, F>
where
    I: RowStream,
    F: FnMut(Vec<RowResult>) -> Vec<RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Row, RowStream,
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

        let re = AdjacentGroup::new(iter, |iter| {
            assert!(false, "compute sum of elements in iter");
            assert!(false, "add column based on the sum");

            unimplemented!()
        });
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
}
