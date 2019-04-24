use std::collections::HashMap;

use crate::{Headers, RowStream, Row, RowResult};

pub struct Rename<I> {
    iter: I,
    headers: Headers,
}

impl <I> Rename<I>
where
    I: RowStream,
{
    pub fn new(iter: I, name_map: &HashMap<&str, &str>) -> Rename<I> {
        let header_row = iter.headers().as_row();
        let mut new_headers = Row::new();

        for header in header_row {
            if name_map.contains_key(header) {
                new_headers.push_field(name_map.get(header).unwrap());
            } else {
                new_headers.push_field(header);
            }
        }

        Rename{
            iter,
            headers: Headers::from_row(new_headers),
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

impl<I> IntoIterator for Rename<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.iter.into_iter(),
        }
    }
}

impl<I> RowStream for Rename<I>
where
    I: RowStream,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::Rename;
    use crate::{Row, Headers, RowStream, mock::MockStream};

    use std::collections::HashMap;

    #[test]
    fn test_rename() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["id", "val"])),
                Ok(Row::from(vec!["1", "40"])),
                Ok(Row::from(vec!["2", "39"])),
                Ok(Row::from(vec!["3", "38"])),
                Ok(Row::from(vec!["4", "37"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let mapping: HashMap<_, _> = vec![
            ("val", "value"),
        ].into_iter().collect();

        let ren = Rename::new(
            iter,
            &mapping,
        );

        assert_eq!(
            *ren.headers(),
            Headers::from_row(Row::from(vec!["id", "value"])),
        );

        let mut ren = ren.into_iter();

        assert_eq!(
            ren.next().unwrap().unwrap(),
            Row::from(vec!["1", "40"])
        );
        assert_eq!(
            ren.next().unwrap().unwrap(),
            Row::from(vec!["2", "39"])
        );
        assert_eq!(
            ren.next().unwrap().unwrap(),
            Row::from(vec!["3", "38"])
        );
        assert_eq!(
            ren.next().unwrap().unwrap(),
            Row::from(vec!["4", "37"])
        );
    }
}
