use super::Row;

#[derive(Debug)]
pub enum Error {
    Csv(csv::Error),
}

pub type RowResult = Result<Row, Error>;
