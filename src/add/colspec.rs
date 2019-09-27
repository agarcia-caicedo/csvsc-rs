use strfmt::{strfmt_map, FmtError, Formatter};
use regex::{Captures, Regex};
use std::str::FromStr;

use crate::{col, Row, Headers};

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

/// Tipos de especificaciones disponibles para crear una nueva columna.
#[derive(Debug)]
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

fn interpolate(template: &str, captures: &Captures) -> String {
    let mut res = String::new();

    captures.expand(template, &mut res);

    res
}

impl ColSpec {
    pub fn compute(&self, data: &Row, headers: &Headers) -> Result<String, col::BuildError> {
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
                Err(FmtError::Invalid(_)) => Err(col::BuildError::InvalidFormat),
                Err(FmtError::KeyError(s)) => Err(col::BuildError::KeyError(s)),
                Err(FmtError::TypeError(_)) => Err(col::BuildError::InvalidFormat),
            },
            ColSpec::Regex {
                ref source,
                ref coldef,
                ref regex,
                ..
            } => match headers.get_field(data, source) {
                Some(field) => match regex.captures(field) {
                    Some(captures) => Ok(interpolate(&coldef, &captures)),
                    None => Err(col::BuildError::ReNoMatch(regex.clone(), field.to_string())),
                },
                None => Err(col::BuildError::KeyError(source.to_string())),
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

#[cfg(test)]
mod tests {
    use regex::Regex;
    use crate::{Row, Headers};
    use super::{interpolate, ColSpec};

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
            c.compute(&data, &Headers::from_row(Row::from(vec!["_source"])))
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
