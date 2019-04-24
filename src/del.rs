//! Utilities for deleting rows
use crate::error::RowResult;

use crate::{Headers, Row, RowStream};

/// Deletes the specified columns from each row.
pub struct Del<'a, I> {
    iter: I,
    columns: Vec<&'a str>,
    headers: Headers,
    old_headers: Headers,
}

impl<'a, I> Del<'a, I>
where
    I: RowStream,
{
    pub fn new(iter: I, columns: Vec<&'a str>) -> Del<I> {
        // TODO I'm sure we can allocate here the exact amount of memory
        let mut header_row = Row::new();

        for col in iter.headers().iter() {
            if !columns.contains(&col) {
                header_row.push_field(col);
            }
        }

        Del{
            old_headers: iter.headers().clone(),
            iter,
            columns,
            headers: Headers::from_row(header_row),
        }
    }
}

pub struct IntoIter<'a, I> {
    iter: I,
    columns: Vec<&'a str>,
    old_headers: Headers,
}

impl<'a, I> Iterator for IntoIter<'a, I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|val| {
                // TODO I'm sure we can allocate here the exact amount of memory
                let mut new_row = Row::new();

                for (header, col) in self.old_headers.iter().zip(val.iter()) {
                    if !self.columns.contains(&header) {
                        new_row.push_field(col);
                    }
                }

                Ok(new_row)
            })
        })
    }
}

impl<'a, I> IntoIterator for Del<'a, I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<'a, I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.iter.into_iter(),
            columns: self.columns,
            old_headers: self.old_headers,
        }
    }
}

impl<'a, I> RowStream for Del<'a, I>
where
    I: RowStream,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{Del, Headers, Row, RowStream};
    use crate::mock::MockStream;

    #[test]
    fn test_del() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["id", "val", "path"])),
                Ok(Row::from(vec!["1", "40", "/tmp/a1m.csv"])),
                Ok(Row::from(vec!["2", "39", "/tmp/a1m.csv"])),
                Ok(Row::from(vec!["3", "38", "/tmp/a2m.csv"])),
                Ok(Row::from(vec!["4", "37", "/tmp/a2m.csv"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let del= Del::new(
            iter,
            vec!["path"],
        );

        assert_eq!(
            *del.headers(),
            Headers::from_row(Row::from(vec!["id", "val"])),
        );

        let mut del= del.into_iter();

        assert_eq!(
            del.next().unwrap().unwrap(),
            Row::from(vec!["1", "40"])
        );
        assert_eq!(
            del.next().unwrap().unwrap(),
            Row::from(vec!["2", "39"])
        );
        assert_eq!(
            del.next().unwrap().unwrap(),
            Row::from(vec!["3", "38"])
        );
        assert_eq!(
            del.next().unwrap().unwrap(),
            Row::from(vec!["4", "37"])
        );
    }
}
