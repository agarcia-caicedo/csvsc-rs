use regex::Regex;

/// Clases de errores que se pueden generar al construir una columna para
/// agregar a cada registro.
#[derive(Debug)]
pub enum BuildError {
    /// the regular expression provided does not match the target column
    ReNoMatch(Regex, String),

    /// the given format for building the new column is not correct
    InvalidFormat,

    /// One of the specfied sources for creating this column does not exist
    KeyError(String),

    /// Everything else
    Generic(String),
}
