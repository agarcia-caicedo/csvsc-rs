pub mod columns;
mod error;
mod headers;
pub mod input;

pub use columns::AddColumns;
pub use error::{Error, RowResult};
pub use input::InputStream;
pub use headers::Headers;

type Row = csv::StringRecord;
