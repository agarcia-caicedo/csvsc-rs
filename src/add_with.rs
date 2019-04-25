use crate::{
    Headers, RowStream, Row,
    add::ColBuildError,
    error::{Error, RowResult},
};


#[derive(Debug)]
pub enum AddWithBuildError {
    DuplicatedHeader(String),
}

/// Adds a column to each register using a closure to generate its data.
///
/// The closure is passed the existing headers and current row.
pub struct AddWith<I, F> {
    iter: I,
    f: F,
    headers: Headers,
}

impl<I, F> AddWith<I, F>
where
    I: RowStream,
    F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
{
    pub fn new(iter: I, colname: &str, f: F) -> Result<AddWith<I, F>, AddWithBuildError> {
        let mut headers = iter.headers().clone();

        if let Err(_) = headers.add(colname) {
            return Err(AddWithBuildError::DuplicatedHeader(colname.to_string()));
        }

        Ok(AddWith{
            iter,
            f,
            headers,
        })
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
    F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|mut val| {
                match (self.f)(&self.headers, &val) {
                    Ok(s) => val.push_field(&s),
                    Err(e) => return Err(Error::ColBuildError(e)),
                }

                Ok(val)
            })
        })
    }
}

impl<I, F> IntoIterator for AddWith<I, F>
where
    I: RowStream,
    F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
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

impl<I, F> RowStream for AddWith<I, F>
where
    I: RowStream,
    F: FnMut(&Headers, &Row) -> Result<String, ColBuildError>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{AddWith, Headers, Row, RowStream};
    use crate::{get_field, mock::MockStream};

    #[test]
    fn test_add() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["id"])),
                Ok(Row::from(vec!["1"])),
                Ok(Row::from(vec!["2"])),
                Ok(Row::from(vec!["3"])),
                Ok(Row::from(vec!["4"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let add= AddWith::new(
            iter,
            "col",
            |headers, row| {
                let v: i32 = get_field(headers, row, "id").unwrap().parse().unwrap();

                Ok((v*v).to_string())
            }
        ).unwrap();

        assert_eq!(
            *add.headers(),
            Headers::from_row(Row::from(vec!["id", "col"])),
        );

        let mut add= add.into_iter();

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "4"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["3", "9"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["4", "16"])
        );
    }
}
