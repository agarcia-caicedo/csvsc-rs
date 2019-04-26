//! Utilities for adding rows
use regex::{Captures, Regex};
use std::str::FromStr;
use strfmt::{strfmt_map, FmtError, Formatter};

use crate::{
    Headers, Row, RowStream,
    error::{Error, RowResult},
};

/// Clases de errores que pueden pasar al interpretar la especificación de
/// cómo construir una nueva columna.
#[derive(Debug)]
pub enum ColSpecParseError {
    MissingSource,
    MissingColname,
    MissingColDef,
    MissingRegex,
    InvalidRegex,
    InvalidSpec,
}

/// Clases de errores que se pueden generar al construir una columna para
/// agregar a cada registro.
#[derive(Debug)]
pub enum ColBuildError {
    UnknownSource,
    ReNoMatch(Regex, String),
    InvalidFormat,
    KeyError(String),
}

// TODO replaceme with strfmt::format maybe
fn interpolate(template: &str, captures: &Captures) -> String {
    let mut res = String::new();

    captures.expand(template, &mut res);

    res
}

/// Tipos de especificaciones disponibles para crear una nueva columna.
pub enum ColSpec {
    /// Construye una nueva columna basándose en una columna anterior, usando
    /// una expresión regular para extraer información de la misma.
    Regex {
        source: String,
        colname: String,
        coldef: String,
        regex: Regex,
    },

    /// Crea una nueva columna mezclando columnas existentes mediante el uso de
    /// una plantilla
    ///
    /// E.g. suponiendo que existen las columnas `month` y `day`:
    ///
    /// ```rust
    /// use csvsc::ColSpec;
    ///
    /// let spec = ColSpec::Mix {
    ///     colname: "new_col".to_string(),
    ///     coldef: "{day}/{month}".to_string(),
    /// };
    Mix { colname: String, coldef: String },
}

impl ColSpec {
    pub fn compute(&self, data: &Row, headers: &Headers) -> Result<String, ColBuildError> {
        match *self {
            ColSpec::Mix { ref coldef, .. } => match strfmt_map(&coldef, &|mut fmt: Formatter| {
                let v = match headers.get_field(data, fmt.key) {
                    Some(v) => v,
                    None => {
                        return Err(FmtError::KeyError(fmt.key.to_string()));
                    }
                };
                fmt.str(v)
            }) {
                Ok(s) => Ok(s),
                Err(FmtError::Invalid(_)) => Err(ColBuildError::InvalidFormat),
                Err(FmtError::KeyError(s)) => Err(ColBuildError::KeyError(s)),
                Err(FmtError::TypeError(_)) => Err(ColBuildError::InvalidFormat),
            },
            ColSpec::Regex {
                ref source,
                ref coldef,
                ref regex,
                ..
            } => match headers.get_field(data, source) {
                Some(field) => match regex.captures(field) {
                    Some(captures) => Ok(interpolate(&coldef, &captures)),
                    None => Err(ColBuildError::ReNoMatch(regex.clone(), field.to_string())),
                },
                None => Err(ColBuildError::UnknownSource),
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

            Ok(ColSpec::Regex {
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

            Ok(ColSpec::Mix { colname, coldef })
        } else {
            Err(ColSpecParseError::InvalidSpec)
        }
    }
}

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
    use super::{interpolate, Add, ColSpec, Headers, Regex, Row, RowStream};
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

    #[test]
    fn test_interpolate() {
        let regex =
            Regex::new(r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})").unwrap();
        let captures = regex.captures("2019-02-09").unwrap();
        let template = String::from("Día: ${day} mes: ${month} año: ${year}");

        assert_eq!(
            interpolate(&template, &captures),
            "Día: 09 mes: 02 año: 2019"
        );
    }
}
