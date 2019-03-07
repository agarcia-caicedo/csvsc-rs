mod columns;
mod error;
mod flusher;
mod headers;
mod input;
mod inspect;
mod reducer;
mod row_stream;

pub mod mock;

pub use columns::{AddColumns, ColSpec};
pub use error::{Error, RowResult};
pub use flusher::Flusher;
pub use headers::Headers;
pub use input::{InputStream, ReaderSource};
pub use inspect::Inspect;
pub use reducer::Reducer;
pub use row_stream::{get_field, RowStream};

type Row = csv::StringRecord;

const TARGET_FIELD: &'static str = "_target";
const SOURCE_FIELD: &'static str = "_source";
