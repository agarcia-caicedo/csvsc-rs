mod columns;
mod error;
mod headers;
mod reducer;
mod input;
mod row_stream;
mod flusher;

pub mod mock;

pub use columns::{AddColumns, ColSpec};
pub use error::{Error, RowResult};
pub use input::{InputStream, ReaderSource};
pub use headers::Headers;
pub use reducer::Reducer;
pub use row_stream::{RowStream, get_field, pack};
pub use flusher::Flusher;

type Row = csv::StringRecord;

const TARGET_FIELD: &'static str = "_target";
const SOURCE_FIELD: &'static str = "_source";
