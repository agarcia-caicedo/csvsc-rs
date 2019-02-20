use crate::error::RowResult;
use std::str::FromStr;
use regex::{Regex, Captures};
use strfmt::{FmtError, strfmt_map, Formatter};

use super::{Row, Headers, RowStream, get_field};

#[derive(Debug)]
pub enum ColSpecParseError {
    MissingSource,
    MissingColname,
    MissingColDef,
    MissingRegex,
    InvalidRegex,
    InvalidSpec,
}

#[derive(Debug)]
pub enum ColBuildError {
    UnknownSource,
    ReNoMatch,
    InvalidFormat,
    // TODO add the missing key
    KeyError,
}

impl ColBuildError {
    fn to_csv(&self) -> String {
        match *self {
            ColBuildError::UnknownSource => "#NO_SOURCE".to_string(),
            ColBuildError::ReNoMatch => "#RE_NO_MATCH".to_string(),
            ColBuildError::InvalidFormat => "#FORMAT".to_string(),
            ColBuildError::KeyError => "#KEY_ERROR".to_string(),
        }
    }
}

// TODO replaceme with strfmt::format maybe
fn interpolate(template: &str, captures: &Captures) -> String {
    // strfmt(template, captures)
    let mut res = String::new();

    captures.expand(template, &mut res);

    res
}

/// Represents a spec for building a new column.
pub enum ColSpec {
    /// Builds a new column based on a previous one, taking information from
    /// a regular expression.
    Regex{
        source: String,
        colname: String,
        coldef: String,
        regex: Regex,
    },

    /// Adds a new column based on a mix template of existing ones
    Mix{
        colname: String,
        coldef: String,
    },
}

impl ColSpec {
    pub fn compute(&self, data: &Row, headers: &Headers) -> Result<String, ColBuildError> {
        match *self {
            ColSpec::Mix{ref coldef, ..} => {
                match strfmt_map(&coldef, &|mut fmt: Formatter| {
                    let v = match get_field(headers, data, fmt.key) {
                        Some(v) => v,
                        None => {
                            return Err(FmtError::KeyError(fmt.key.to_string()));
                        }
                    };
                    fmt.str(v)
                }) {
                    Ok(s) => Ok(s),
                    Err(FmtError::Invalid(_)) => Err(ColBuildError::InvalidFormat),
                    Err(FmtError::KeyError(_)) => Err(ColBuildError::KeyError),
                    Err(FmtError::TypeError(_)) => Err(ColBuildError::InvalidFormat),
                }
            },
            ColSpec::Regex{ref source, ref coldef, ref regex, ..} => {
                match get_field(headers, data, source) {
                    Some(field) => match regex.captures(field) {
                        Some(captures) => Ok(interpolate(&coldef, &captures)),
                        None => Err(ColBuildError::ReNoMatch),
                    },
                    None => Err(ColBuildError::UnknownSource),
                }
            },
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
        } else if spec.starts_with("value:") {
            let mut pieces = spec.split(':');
            pieces.next();

            let colname;
            let coldef;

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

            Ok(ColSpec::Mix{
                colname,
                coldef,
            })
        } else {
            Err(ColSpecParseError::InvalidSpec)
        }
    }
}

pub struct AddColumns<I> {
    iter: I,
    columns: Vec<ColSpec>,
    headers: Headers,
}

impl<I> AddColumns<I>
where I: RowStream
{
    pub fn new(iter: I, columns: Vec<ColSpec>) -> AddColumns<I> {
        let mut headers = iter.headers().clone();

        for col in columns.iter() {
            headers.add(match col {
                ColSpec::Regex{colname, ..} => colname,
                ColSpec::Mix{colname, ..} => colname,
            });
        }

        AddColumns {
            iter,
            columns,
            headers,
        }
    }
}

struct IntoIter<I> {
    iter: I,
    columns: Vec<ColSpec>,
    headers: Headers,
}

impl<I> Iterator for IntoIter<I>
where I: Iterator<Item = RowResult>
{
    type Item = RowResult;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|result| {
            result.and_then(|mut val| {
                for spec in self.columns.iter() {
                    val.push_field(&match spec.compute(&val, &self.headers) {
                        Ok(s) => s,
                        Err(e) => e.to_csv(),
                    });
                }

                Ok(val)
            })
        })
    }
}

impl<I> IntoIterator for AddColumns<I>
where I: IntoIterator<Item = RowResult>
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

impl<I> RowStream for AddColumns<I>
where
    AddColumns<I>: IntoIterator<Item = RowResult>,
{
    fn headers(&self) -> &Headers {
        &self.headers
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AddColumns, ColSpec, Row, Headers, RowStream, Regex, Captures,
        interpolate,
    };
    use crate::mock::MockStream;

    #[test]
    fn test_colspec_simplest() {
        let c: ColSpec = "value:new:value".parse().unwrap();
        let data = Row::new();

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["a"]))).unwrap(),
            "value",
        );
    }

    #[test]
    fn test_colspec_regex_source() {
        let c: ColSpec = "regex:_source:new:${number}:a(?P<number>[0-9]+)m".parse().unwrap();
        let data = Row::from(vec!["a20m"]);

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["_source"]))).unwrap(),
            "20",
        );
    }

    #[test]
    fn test_colspec_mix() {
        let c: ColSpec = "value:new:{a}-{b}".parse().expect("could not parse");
        let data = Row::from(vec!["2", "4"]);

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["a", "b"]))).unwrap(),
            "2-4",
        );
    }

    #[test]
    fn test_add_columns() {
        let iter = MockStream::from_rows(vec![
            Ok(Row::from(vec!["id", "val", "path"])),
            Ok(Row::from(vec!["1", "40", "/tmp/a1m.csv"])),
            Ok(Row::from(vec!["2", "39", "/tmp/a1m.csv"])),
            Ok(Row::from(vec!["3", "38", "/tmp/a2m.csv"])),
            Ok(Row::from(vec!["4", "37", "/tmp/a2m.csv"])),
        ].into_iter()).unwrap();

        let mut add_columns = AddColumns::new(
            iter,
            vec!["regex:path:new:$1:a([0-9]+)m\\.csv$".parse().unwrap()],
        );

        assert_eq!(
            *add_columns.headers(),
            Headers::from_row(Row::from(vec!["id", "val", "path", "new"])),
        );

        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["1", "40", "/tmp/a1m.csv", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["2", "39", "/tmp/a1m.csv", "1"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["3", "38", "/tmp/a2m.csv", "2"])
        );
        assert_eq!(
            add_columns.next().unwrap().unwrap(),
            Row::from(vec!["4", "37", "/tmp/a2m.csv", "2"])
        );
    }

    #[test]
    fn test_interpolate() {
        let regex = Regex::new(r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})").unwrap();
        let captures = regex.captures("2019-02-09").unwrap();
        let template = String::from("Día: ${day} mes: ${month} año: ${year}");

        assert_eq!(interpolate(&template, &captures), "Día: 09 mes: 02 año: 2019");
    }
}
