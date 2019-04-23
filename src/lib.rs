/*!

`csvsc` es una biblioteca para construir procesadores de archivos csv.

Imagina que tienes N archivos csv con la misma estructura y quieres producir
con ellos otros M archivos csv cuya información depende de alguna manera de los
archivos originales. Para eso es csvcv. Con esta herramienta puedes construir
una cadena de procesamiento que va a modificar cada registro de los archivos de
entrada y generar nuevos archivos de salida con esas modificaciones.

# Modo de preparación

Comienza un nuevo proyecto binario con cargo:

```text
$ cargo new --bin miprocesadordecsv
```

luego agrega `csvsc` y `encoding` como dependencia en `Cargo.toml`

```toml
[dependencies]
csvsc = { git = "https://github.com/categulario/csvsc-rs.git" }
encoding = "*"
```

y ahora construye tu cadena de procesamiento. En el siguiente ejemplo se
construye una cadena de procesamiento con las siguientes características:

1. toma como entrada los archivos `1.csv` y `2.csv` con codificación `UTF-8`,
1. agrega una columna virtual `_target` que definirá el archivo de salida y que utiliza la columna `a` de los archivos de entrada en su definición,
1. elimina la columna `b`.

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
        .add(vec![ColSpec::Mix {
            colname: "_target".to_string(),
            coldef: "output/{a}.csv".to_string(),
        }])
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

Ejecutar este proyecto resultaría en una carpeta `output/` creada y dentro
tantos archivos como diferentes valores haya en la columna `a`.

Para saber qué métodos están disponibles en una cadena de procesamiento ve a la
documentación de [RowStream](trait.RowStream.html).

Las columnas cuyos nombres comienzen con guión bajo no serán escritas en los
archivos de salida.
*/

mod add;
mod add_with;
mod error;
mod flush;
mod headers;
mod input;
mod inspect;
mod reduce;
mod adjacent_reduce;
mod row_stream;
mod del;
mod adjacent_sort;
mod mock;
mod rename;

pub use add::{Add, ColSpec, ColBuildError};
pub use error::{Error, RowResult};
pub use flush::{Flush, FlushTarget};
pub use headers::Headers;
pub use input::{InputStream, ReaderSource};
pub use inspect::Inspect;
pub use reduce::Reduce;
pub use row_stream::{get_field, RowStream};
pub use add_with::AddWith;
pub use del::Del;
pub use rename::Rename;
// TODO rethink this whole adjacent thing... it might be a good idea to abstract
// it into something better that calls a closure with a vector of adjacent rows
// for example, then implement stuff on top of that
pub use adjacent_sort::AdjacentSort;
pub use adjacent_reduce::AdjacentReduce;
pub use mock::MockStream;

/// Type alias of csv::StringRecord. Represents a row of data.
pub type Row = csv::StringRecord;

/// Una columna con este nombre será añadida a cada registro. Dicha columna
/// tiene por valor la ruta absoluta al archivo de entrada y sirve para extraer
/// información que pueda estar contenida por ejemplo en el nombre del archivo.
///
/// Es útil en combinación con el procesador [Add](struct.Add.html).
pub const SOURCE_FIELD: &'static str = "_source";
