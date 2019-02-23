use super::{Row, Headers, RowStream, RowResult};

#[derive(Debug,PartialEq)]
pub enum BuildError {
    EmptyIterator,
    FailedHeader,
}

pub struct MockStream<I> {
    iter: I,
    headers: Headers,
}

impl<I> MockStream<I>
where
    I: Iterator<Item = RowResult>,
{
    fn new(iter: I, headers: Row) -> MockStream<I> {
        MockStream {
            iter,
            headers: Headers::from_row(headers),
        }
    }

    pub fn from_rows(mut iter: I) -> Result<MockStream<I>, BuildError> {
        match iter.next() {
            Some(Ok(row)) => Ok(MockStream::new(iter, row)),
            Some(Err(_)) => Err(BuildError::FailedHeader),
            None => Err(BuildError::EmptyIterator)
        }
    }
}

pub struct IntoIter<I> {
    iter: I,
}

impl<I> Iterator for IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I> IntoIterator for MockStream<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            iter: self.iter,
        }
    }
}

impl<I> RowStream for MockStream<I>
where
    MockStream<I>: IntoIterator<Item = RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{MockStream, RowStream, Row, Headers};

    #[test]
    fn test_mock_stream() {
        let mut m = MockStream::from_rows(vec![
            Ok(Row::from(vec!["id", "num"])),
            Ok(Row::from(vec!["1", "40"])),
            Ok(Row::from(vec!["2", "39"])),
        ].into_iter()).unwrap();

        assert_eq!(*m.headers(), Headers::from_row(Row::from(vec!["id", "num"])));

        let mut m = m.into_iter();

        assert_eq!(m.next().unwrap().unwrap(), Row::from(vec!["1", "40"]));
        assert_eq!(m.next().unwrap().unwrap(), Row::from(vec!["2", "39"]));
    }
}
