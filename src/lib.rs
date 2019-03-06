mod columns;
mod error;
mod headers;
mod reducer;
mod input;
mod row_stream;
mod flusher;
mod inspect;

pub mod mock;

pub use columns::{AddColumns, ColSpec};
pub use error::{Error, RowResult};
pub use input::{InputStream, ReaderSource};
pub use headers::Headers;
pub use reducer::Reducer;
pub use row_stream::{RowStream, get_field};
pub use flusher::Flusher;
pub use inspect::Inspect;

type Row = csv::StringRecord;

const TARGET_FIELD: &'static str = "_target";
const SOURCE_FIELD: &'static str = "_source";
