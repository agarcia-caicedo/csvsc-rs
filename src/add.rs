use crate::{
    Headers, RowStream,
    error::{Error, RowResult},
};

mod colspec;

pub use colspec::ColSpec;

#[derive(Debug)]
pub enum BuildError {
    DuplicatedHeader(String),
}

/// Adds multiple columns to each register. They can be based on existing ones
/// or the source filename.
pub struct Add<I> {
    iter: I,
    columns: Vec<ColSpec>,
    headers: Headers,
}

impl<I> Add<I>
where
    I: RowStream,
{
    pub fn new(iter: I, columns: Vec<ColSpec>) -> Result<Add<I>, BuildError> {
        let mut headers = iter.headers().clone();

        for col in columns.iter() {
            let colname = match col {
                ColSpec::Regex { colname, .. } => colname,
                ColSpec::Mix { colname, .. } => colname,
            };

            if let Err(_) = headers.add(colname) {
                return Err(BuildError::DuplicatedHeader(colname.to_string()));
            }
        }

        Ok(Add{
            iter,
            columns,
            headers,
        })
    }
}

pub struct IntoIter<I> {
    iter: I,
    columns: Vec<ColSpec>,
    headers: Headers,
}

impl<I> Iterator for IntoIter<I>
where
    I: Iterator<Item = RowResult>,
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|mut val| {
                for spec in self.columns.iter() {
                    match spec.compute(&val, &self.headers) {
                        Ok(s) => val.push_field(&s),
                        Err(e) => return Err(Error::ColBuildError(e)),
                    }
                }

                Ok(val)
            })
        })
    }
}

impl<I> IntoIterator for Add<I>
where
    I: RowStream,
{
    type Item = RowResult;

    type IntoIter = IntoIter<I::IntoIter>;

    fn into_iter(self) -> Self::IntoIter {
        Self::IntoIter {
            iter: self.iter.into_iter(),
            columns: self.columns,
            headers: self.headers,
        }
    }
}

impl<I> RowStream for Add<I>
where
    I: RowStream,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{Add, ColSpec, Headers, Regex, Row, RowStream};
    use crate::{
        SOURCE_FIELD,
        mock::MockStream,
        error::Error,
    };

    #[test]
    fn test_colspec_simplest() {
        let c: ColSpec = "value:new:value".parse().unwrap();
        let data = Row::new();

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["a"])))
                .unwrap(),
            "value",
        );
    }

    #[test]
    fn test_colspec_regex_source() {
        let c: ColSpec = "regex:_source:new:${number}:a(?P<number>[0-9]+)m"
            .parse()
            .unwrap();
        let data = Row::from(vec!["a20m"]);

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec![SOURCE_FIELD])))
                .unwrap(),
            "20",
        );
    }

    #[test]
    fn test_colspec_mix() {
        let c: ColSpec = "value:new:{a}-{b}".parse().expect("could not parse");
        let data = Row::from(vec!["2", "4"]);

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["a", "b"])))
                .unwrap(),
            "2-4",
        );
    }

    #[test]
    fn test_add() {
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

        let add = Add::new(
            iter,
            vec!["regex:path:new:$1:a([0-9]+)m\\.csv$".parse().unwrap()],
        ).unwrap();

        assert_eq!(
            *add.headers(),
            Headers::from_row(Row::from(vec!["id", "val", "path", "new"])),
        );

        let mut add = add.into_iter();

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "40", "/tmp/a1m.csv", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["2", "39", "/tmp/a1m.csv", "1"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["3", "38", "/tmp/a2m.csv", "2"])
        );
        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["4", "37", "/tmp/a2m.csv", "2"])
        );
    }

    #[test]
    fn test_add_doesnt_swallow_errors() {
        let iter = MockStream::from_rows(
            vec![
                Ok(Row::from(vec!["a"])),
                Ok(Row::from(vec!["1"])),
                Err(Error::InconsistentHeaders),
                Ok(Row::from(vec!["3"])),
            ]
            .into_iter(),
        )
        .unwrap();

        let add = Add::new(
            iter,
            vec!["value:b:1".parse().unwrap()],
        ).unwrap();

        assert_eq!(
            *add.headers(),
            Headers::from_row(Row::from(vec!["a", "b"])),
        );

        let mut add = add.into_iter();

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["1", "1"])
        );

        match add.next() {
            Some(Err(Error::InconsistentHeaders)) => {},
            _ => unreachable!(),
        }

        assert_eq!(
            add.next().unwrap().unwrap(),
            Row::from(vec!["3", "1"])
        );
    }
}
