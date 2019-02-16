mod columns;
mod error;
mod headers;
mod reducer;
mod input;
mod row_stream;

pub mod mock;

pub use columns::{AddColumns, ColSpec};
pub use error::{Error, RowResult};
pub use input::{InputStream, ReaderSource};
pub use headers::Headers;
pub use reducer::Reducer;
pub use row_stream::{RowStream, get_field, pack};

type Row = csv::StringRecord;
