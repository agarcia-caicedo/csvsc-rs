pub mod columns;
mod error;
mod headers;
pub mod input;

pub use columns::AddColumns;
pub use error::{Error, RowResult};
pub use input::InputStream;
pub use headers::Headers;

mod row_stream;
pub mod mock;

pub use row_stream::{RowStream, get_field, pack};

type Row = csv::StringRecord;
