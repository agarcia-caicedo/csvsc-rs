use crate::error::RowResult;
use std::str::FromStr;
use regex::Regex;

use super::{Row, Headers, RowStream};

#[derive(Debug)]
pub enum ColSpecParseError {
    MissingSource,
    MissingColname,
    MissingColDef,
    MissingRegex,
    InvalidRegex,
}

pub enum ColSpec {
    Regex{
        source: String,
        colname: String,
        coldef: String,
        regex: Regex,
    },
    Const(String),
}

impl ColSpec {
    pub fn compute(&self, data: &Row) -> Vec<String> {
        match *self {
            ColSpec::Const(ref val) => vec![val.clone()],
            ColSpec::Regex{..} => Vec::new(),
        }
    }
}

impl FromStr for ColSpec {
    type Err = ColSpecParseError;

    fn from_str(spec: &str) -> Result<ColSpec, Self::Err> {
        if spec.starts_with("regex:") {
            let mut pieces = spec.split(':');
            pieces.next();

            let source;
            let colname;
            let coldef;
            let regex;

            if let Some(s) = pieces.next() {
                source = s.to_string();
            } else {
                return Err(ColSpecParseError::MissingSource);
            }

            if let Some(s) = pieces.next() {
                colname = s.to_string();
            } else {
                return Err(ColSpecParseError::MissingColname);
            }

            if let Some(s) = pieces.next() {
                coldef = s.to_string();
            } else {
                return Err(ColSpecParseError::MissingColDef);
            }

            if let Some(s) = pieces.next() {
                if let Ok(r) = Regex::new(s) {
                    regex = r;
                } else {
                    return Err(ColSpecParseError::InvalidRegex);
                }
            } else {
                return Err(ColSpecParseError::MissingRegex);
            }

            Ok(ColSpec::Regex{
                source,
                colname,
                coldef,
                regex,
            })
        } else {
            Ok(ColSpec::Const(spec.to_string()))
        }
    }
}

pub struct AddColumns<T> {
    iter: T,
    columns: Vec<ColSpec>,
    headers: Headers,
}

impl<T> AddColumns<T>
    where T: Iterator<Item = RowResult> + RowStream
{
    pub fn new(iter: T, columns: Vec<ColSpec>) -> AddColumns<T> {
        let headers = iter.headers().clone();

        AddColumns {
            iter,
            columns,
            headers,
        }
    }
}

impl<T: Iterator<Item = RowResult>> Iterator for AddColumns<T> {
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|mut val| {
                for spec in self.columns.iter() {
                    let new_fields = spec.compute(&val);

                    for field in new_fields {
                        val.push_field(&field);
                    }
                }

                Ok(val)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{AddColumns, ColSpec, Row};
    use crate::mock::MockStream;

    #[test]
    fn test_colspec_simplest() {
        let c: ColSpec = "value".parse().unwrap();
        let data = Row::new();

        assert_eq!(c.compute(&data), ["value"]);
    }

    #[test]
    fn test_colspec_regex_source() {
        let c: ColSpec = "regex:_source:new:1:a([0-9]+)m".parse().unwrap();
        let data = Row::from(vec!["a20m"]);

        assert_eq!(c.compute(&data), ["20"]);
    }

    #[test]
    #[ignore]
    fn test_add_columns() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["1", "40", "/tmp/a1m.csv"])),
            Ok(Row::from(vec!["2", "39", "/tmp/a1m.csv"])),
            Ok(Row::from(vec!["3", "38", "/tmp/a2m.csv"])),
            Ok(Row::from(vec!["4", "37", "/tmp/a2m.csv"])),
        ].into_iter()).unwrap();

        let mut add_columns = AddColumns::new(
            iter,
            vec!["regex:_source:new:$1:a([0-9]+)m\\.csv$".parse().unwrap()],
        );

        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["1", "40", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["2", "39", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["3", "38", "2"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["4", "37", "2"])
        );
    }
}
