use strfmt::{strfmt_map, FmtError, Formatter};
use regex::{Captures, Regex};

use crate::{col, Row, Headers};

/// Tipos de especificaciones disponibles para crear una nueva columna.
#[derive(Debug)]
pub enum ColSpec {
    /// Construye una nueva columna basándose en una columna anterior, usando
    /// una expresión regular para extraer información de la misma.
    Regex {
        source: String, // TODO replace this and other ones with &str
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

#[cfg(test)]
mod tests {
    use regex::Regex;
    use crate::{Row, Headers};
    use super::{interpolate, ColSpec};

    #[test]
    fn test_colspec_simplest() {
        let c: ColSpec = ColSpec::Mix {
            colname: "new".to_string(),
            coldef: "value".to_string(),
        };
        let data = Row::new();

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["a"])))
                .unwrap(),
            "value",
        );
    }

    #[test]
    fn test_colspec_regex_source() {
        let c = ColSpec::Regex {
            source: "_source".to_string(),
            colname: "new".to_string(),
            coldef: "${number}".to_string(),
            regex: Regex::new("a(?P<number>[0-9]+)m").unwrap(),
        };
        let data = Row::from(vec!["a20m"]);

        assert_eq!(
            c.compute(&data, &Headers::from_row(Row::from(vec!["_source"])))
                .unwrap(),
            "20",
        );
    }

    #[test]
    fn test_colspec_mix() {
        let c = ColSpec::Mix {
            colname: "new".to_string(),
            coldef: "{a}-{b}".to_string(),
        };
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
