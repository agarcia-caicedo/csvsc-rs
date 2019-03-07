use clap::{App, Arg};

use csvsc::ColSpec;
use csvsc::InputStream;
use csvsc::ReaderSource;
use csvsc::RowStream;
use encoding::all::ISO_8859_1;

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
        .get_matches();

    // Step 1. Get the source
    let filenames: Vec<_> = matches.values_of("input").unwrap().collect();

    // Step 2. Map the info, add/remove, transform each row
    let mut chain = InputStream::from_readers(
        filenames
            .iter()
            .map(|f| ReaderSource::from_path(f).unwrap()),
        ISO_8859_1,
    )
    .add_columns(vec![r"regex:_source:variable:$1:(\w+)-(\w+)-(\d).csv$"
        .parse()
        .unwrap()])
    .add_columns(vec![ColSpec::Mix {
        colname: "_target".to_string(),
        coldef: matches.value_of("output").unwrap().to_string(),
    }])
    .reduce(
        match matches.values_of("group") {
            Some(groupings) => groupings.collect(),
            None => Vec::new(),
        },
        match matches.values_of("reduce") {
            Some(reduce) => reduce.map(|r| r.parse().unwrap()).collect(),
            None => Vec::new(),
        },
    )
    .expect("Error builing reducer")
    .flush()
    .into_iter();

    while let Some(item) = chain.next() {
        if let Err(e) = item {
            eprintln!("failed {:?}", e);
        }
    }
}
