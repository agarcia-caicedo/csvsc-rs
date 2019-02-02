pub mod columns;
mod error;
pub mod input;

pub use columns::AddColumns;
pub use error::{Error, RowResult};
pub use input::InputStream;

type Row = csv::StringRecord;
