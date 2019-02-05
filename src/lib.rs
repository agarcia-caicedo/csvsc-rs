pub mod columns;
mod error;
mod headers;
pub mod input;

pub use columns::AddColumns;
pub use error::{Error, RowResult};
pub use input::InputStream;
pub use headers::Headers;

mod row_stream;

pub use row_stream::RowStream;

type Row = csv::StringRecord;
