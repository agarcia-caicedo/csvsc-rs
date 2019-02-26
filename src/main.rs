use clap::{App, Arg};
use std::io;

use csvsc::ColSpec;
use csvsc::ReaderSource;
use csvsc::AddColumns;
use csvsc::InputStream;
use csvsc::Reducer;
use csvsc::Flusher;
use encoding::label::encoding_from_whatwg_label;

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
    let encoding = encoding_from_whatwg_label(matches.value_of("encoding").unwrap())
        .expect("Invalid encoding, check whatwg list");

    let input_stream = InputStream::from_readers(filenames
        .iter()
        .filter_map(|f| match csv::Reader::from_path(f) {
            Ok(reader) => Some(ReaderSource::from_reader(reader, f)),

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
        }), encoding);

    // Step 2. Map the info, add/remove, transform each row
    let add_columns = AddColumns::new(
        input_stream,
        match matches.values_of("add_columns") {
            Some(columns) => columns.map(|s| s.parse().unwrap()).collect(),
            None => Vec::new(),
        },
    );

    // Step 3. Stablish destination
    let add_dest = AddColumns::new(
        add_columns,
        vec![ColSpec::Mix{
            colname: "_target".to_string(),
            coldef: matches.value_of("output").unwrap().to_string(),
        }],
    );

    // Step 4. Reduce, aggregate
    let reducer = Reducer::new(
        add_dest,
        Vec::new(),
        Vec::new(),
    ).unwrap();

    // Step 5. Flush to destination
    let mut flusher = Flusher::new(reducer).into_iter();

    while let Some(item) = flusher.next() {
        if let Err(error) = item {
            eprintln!("{:?}", error);
        }
    }
}
