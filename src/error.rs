use std::path::PathBuf;

use super::add::ColBuildError;
use super::Row;

#[derive(Debug)]
pub enum Error {
    Csv(csv::Error),
    InconsistentSizeOfRows(PathBuf),
    InconsistentHeaders(PathBuf),
    ColBuildError(ColBuildError),
}

pub type RowResult = Result<Row, Error>;

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
        }
    }
}
