/*!

`csvsc` is a library for building csv file processors.

Imagine you have N csv files with the same structure and you want to use them to 
make other M csv files whose information depends in some way on the original files.
This is what csvcv is for. With this tool you can build a processing chain that will
modify each of the input files and generate new output files with the modifications.

# Preparation Mode

Start a new binary project with cargo:

```text
$ cargo new --bin miprocesadordecsv
```

Add `csvsc` and `encoding` as a dependency in `Cargo.toml`

```toml
[dependencies]
csvsc = { git = "https://github.com/categulario/csvsc-rs.git" }
encoding = "*"
```

Now build your processing chain. In this example, a processing
chain is built with the following characteristics:

1. It takes files `1.csv` and `2.csv` as input with `UTF-8` encoding,
2. adds virtual column `_target` which will define the output file and uses the `a` column of both input files in its definition,
3. Eliminates column `b`.

```rust
use csvsc::ColSpec;
use csvsc::InputStream;
use csvsc::ReaderSource;
use csvsc::RowStream;
use csvsc::FlushTarget;

use encoding::all::UTF_8;

fn main() {
    let filenames = vec!["test/assets/1.csv", "test/assets/2.csv"];

    let mut chain = InputStream::from_readers(
            filenames
                .iter()
                .map(|f| ReaderSource::from_path(f).unwrap()),
            UTF_8,
        )
        .add(ColSpec::Mix {
            colname: "_target".to_string(),
            coldef: "output/{a}.csv".to_string(),
        }).unwrap()
        .del(vec!["b"])
        .flush(FlushTarget::Column("_target".to_string())).unwrap()
        .into_iter();

    while let Some(item) = chain.next() {
        if let Err(e) = item {
            eprintln!("failed {:?}", e);
        }
    }
}
```

Executing this project will lead to an `output/` folder being created and 
inside there will be as many files as there are different values in column `a`

To know which methods are available in a processing chain, go to the [RowStream](trait.RowStream.html)
documentation.

Columns with names that start with an underscore will not be written to the output files.
*/

mod add;
mod add_with;
mod group;
mod adjacent_group;
mod del;
mod error;
mod flush;
mod headers;
mod input;
mod inspect;
mod mock;
mod reduce;
mod rename;
mod row_stream;
pub mod col;

pub use add::{Add, ColSpec};
pub use add_with::AddWith;
pub use adjacent_group::AdjacentGroup;
pub use group::Group;
pub use del::Del;
pub use error::{Error, RowResult};
pub use flush::{Flush, FlushTarget};
pub use headers::Headers;
pub use input::{InputStream, ReaderSource};
pub use inspect::Inspect;
pub use mock::MockStream;
pub use reduce::Reduce;
pub use rename::Rename;
pub use row_stream::RowStream;
pub use reduce::aggregate;

/// Type alias of csv::StringRecord. Represents a row of data.
pub type Row = csv::StringRecord;

/// A column with this name will be added to each record. The column will
/// have as a value the absolute path to the input file and serves to extract 
/// information that may be contained, for example, in the file name.
/// It is useful in combination with the processor [Add](struct.Add.html).
pub const SOURCE_FIELD: &'static str = "_source";

/// Things that could go wrong while building a group or adjacent group
#[derive(Debug)]
pub enum GroupBuildError {
    GroupingKeyError(String),
}
