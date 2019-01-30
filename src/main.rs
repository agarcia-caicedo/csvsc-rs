use clap::{App, Arg};
use std::io;

use csvsc::input::ReaderSource;
use csvsc::AddColumns;
use csvsc::InputStream;

fn main() {
    let matches = App::new("csvsc")
        .version("1.0")
        .author("Abraham Toriz <categulario@gmail.com>")
        .about("Converts M csv files in N files using aggregates and other rules")
        .arg(
            Arg::with_name("input")
                .value_name("INPUT")
                .help("Input files")
                .multiple(true)
                .required(true),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .value_name("OUTPUT")
                .help("Output filename")
                .required(true),
        )
        .arg(
            Arg::with_name("encoding")
                .short("e")
                .long("input-encoding")
                .value_name("ENCODING")
                .default_value("utf-8")
                .help("The encoding to use while reading the files")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("add_columns")
                .short("a")
                .long("add-columns")
                .value_name("COLUMN_SPEC")
                .help("Columns to add to the input")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("reduce")
                .short("r")
                .long("reduce")
                .value_name("REDUCE_SPEC")
                .help("Reduce using the specified rules")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("group")
                .short("g")
                .long("group-by")
                .value_name("GROUP_SPEC")
                .help("How to group for aggregates")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("filter")
                .short("f")
                .long("filter")
                .value_name("FILTER_SPEC")
                .help("Exclude some rows from the output")
                .takes_value(true),
        )
        .get_matches();

    // Step 1. Get the source
    let filenames: Vec<_> = matches.values_of("input").unwrap().collect();
    let encoding = matches.value_of("encoding").unwrap();

    let input_stream: InputStream = filenames
        .iter()
        .filter_map(|f| match csv::Reader::from_path(f) {
            Ok(reader) => Some(ReaderSource {
                reader,
                path: f.to_string(),
            }),
            Err(e) => {
                match e.kind() {
                    csv::ErrorKind::Io(error) => match error.kind() {
                        io::ErrorKind::PermissionDenied => eprintln!("Permission denied: {}", f),
                        io::ErrorKind::NotFound => eprintln!("Not found: {}", f),
                        _ => eprintln!("IO Error: {}", f),
                    },
                    _ => eprintln!("This shouldn't happen, please report to mantainer"),
                }

                None
            }
        })
        .collect();

    dbg!(input_stream.headers());

    for line in input_stream {
        dbg!(line);
    }

    // Step 2. Map the info, add/remove, transform each row
    // let columns: Vec<_> = matches.values_of("add_columns").unwrap().collect();
    // let add_columns = AddColumns::new(input_stream, columns);
    // mapper = Mapper(input_stream, add_columns=self.add_columns)

    // Step 3. Stablish destination
    // dist = Distributor(mapper, self.output)

    // Step 4. Reduce, aggregate
    // reducer = Reducer(dist, grouping=self.grouping, columns=self.reducer_columns)

    // Step 5. Flush to destination
    // Flusher(reducer).flush()
}
