pub mod add;
mod add_with;
mod error;
mod flusher;
mod headers;
mod input;
mod inspect;
mod reducer;
mod adjacent_reduce;
mod row_stream;
mod del;
mod adjacent_sort;

pub mod mock;

pub use add::{Add, ColSpec};
pub use error::{Error, RowResult};
pub use flusher::Flusher;
pub use headers::Headers;
pub use input::{InputStream, ReaderSource};
pub use inspect::Inspect;
pub use reducer::Reducer;
pub use row_stream::{get_field, RowStream};
pub use add_with::AddWith;
pub use adjacent_reduce::AdjacentReduce;
pub use del::Del;
// TODO rethink this whole adjacent thing... it might be a good idea to abstract
// it into something better
pub use adjacent_sort::AdjacentSort;

/// Type alias of csv::StringRecord. Represents a row of data.
pub type Row = csv::StringRecord;

// TODO delete this and make it dynamic
const TARGET_FIELD: &'static str = "_target";
const SOURCE_FIELD: &'static str = "_source";
