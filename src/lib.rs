pub mod columns;
pub mod input;

pub use columns::AddColumns;
pub use input::InputStream;

type Row = csv::ByteRecord;
