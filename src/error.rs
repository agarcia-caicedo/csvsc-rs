use std::path::PathBuf;
use std::result;

use crate::{Row, add::ColBuildError};

/// An error found somewhere in the transformation chain.
#[derive(Debug)]
pub enum Error {
    // TODO remove the Csv variant and derive partialeq, eq and hash. This will
    // allow for errors to be grouped and streamed in groups
    Csv(csv::Error),
    InconsistentSizeOfRows(PathBuf),
    InconsistentHeaders(PathBuf),
    ColBuildError(ColBuildError),
    ColumnNotFound(String),
}

pub type Result<T> = result::Result<T, Error>;

/// The type that actually flows the transformation chain. Either a row or an
/// error.
pub type RowResult = result::Result<Row, Error>;

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Error {
        Error::Csv(error)
    }
}

impl std::error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Csv(_) => "CSV error",
            Error::InconsistentSizeOfRows(_) => "inconsistent size of rows",
            Error::InconsistentHeaders(_) => "inconsistent headers among files",
            Error::ColBuildError(_) => "Error building a column",
            Error::ColumnNotFound(_) => "Requested unexisten column",
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Error::Csv(ref e) => write!(f, "CSV error: {}", e),
            Error::InconsistentSizeOfRows(ref p) => {
                write!(f, "inconsistent size of rows in {:?}", p)
            }
            Error::InconsistentHeaders(ref p) => {
                write!(f, "inconsistent headers among files in {:?}", p)
            }
            Error::ColBuildError(ref c) => write!(f, "Could not build column with reason: {:?}", c),
            Error::ColumnNotFound(ref c) => write!(f, "Requested column that was not found: {}", c),
        }
    }
}
